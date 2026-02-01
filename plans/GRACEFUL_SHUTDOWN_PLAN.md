# Graceful Shutdown 改善計画

## 目標

`http.Server` と同様のパターンで API を設計し、graceful shutdown と強制終了を明確に分離する。

## API 設計

```go
// http.Server パターンに準拠
func (s *Subscriber) Subscribe(consumer Consumer) error      // context なし
func (s *Subscriber) Shutdown(ctx context.Context) error     // graceful shutdown
func (s *Subscriber) Close() error                           // 強制終了
```

### 使用例

```go
subscriber := NewSubscriber(...)

// 別 goroutine で Subscribe を開始
go func() {
    if err := subscriber.Subscribe(consumer); err != nil {
        log.Printf("Subscribe ended: %v", err)
    }
}()

// シグナルを待つ
sig := make(chan os.Signal, 1)
signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
<-sig

// graceful shutdown（タイムアウト付き）
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := subscriber.Shutdown(shutdownCtx); err != nil {
    // タイムアウト → 強制終了
    subscriber.Close()
}
```

## 終了パターン

| パターン | トリガー | drain | State |
|---------|---------|-------|-------|
| 正常完了 | endTimestamp 到達 | ✓ | FINISHED |
| graceful shutdown | `Shutdown()` | ✓ | RUNNING |
| 強制終了 | `Close()` | ✗ | RUNNING |

## http.Server との対応

| http.Server | Subscriber |
|-------------|------------|
| `ListenAndServe()` | `Subscribe(consumer)` |
| `Shutdown(ctx)` | `Shutdown(ctx)` |
| `Close()` | `Close()` |
| `ErrServerClosed` | `ErrSubscriberClosed` |

## 実装計画

### Phase 1: エラー定義

**ファイル**: `errors.go`

```go
var (
    // ErrSubscriberClosed is returned by Subscribe when Shutdown or Close is called.
    ErrSubscriberClosed = errors.New("spream: subscriber closed")
)
```

### Phase 2: coordinator の変更

**ファイル**: `coordinator.go`

```go
type coordinator struct {
    // ... 既存フィールド ...
    ctx    context.Context
    cancel context.CancelCauseFunc

    done chan struct{}  // run 終了通知用
}

func newCoordinator(...) *coordinator {
    return &coordinator{
        // ...
        done: make(chan struct{}),
    }
}

// run は内部で context を作成する（外部から渡さない）
func (c *coordinator) run() error {
    defer close(c.done)

    c.ctx, c.cancel = context.WithCancelCause(context.Background())
    defer c.cancel(nil)

    // ... 既存のロジック ...

    c.wg.Wait()

    if c.err != nil {
        return c.err
    }

    // Shutdown/Close による終了
    if c.ctx.Err() != nil {
        return ErrSubscriberClosed
    }

    return nil
}

// shutdown は graceful shutdown を開始する
// cause = nil で drain を有効化
func (c *coordinator) shutdown(ctx context.Context) error {
    c.cancel(nil)

    select {
    case <-c.done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// close は強制終了する
// cause = ErrSubscriberClosed で drain をスキップ
func (c *coordinator) close() error {
    c.cancel(ErrSubscriberClosed)

    <-c.done
    return nil
}

// recordError はエラーを記録し、全体をキャンセルする
func (c *coordinator) recordError(err error) {
    c.errOnce.Do(func() {
        c.err = err
        c.cancel(err)
    })
}

// initiateShutdown は全パーティション完了時に呼ばれる
func (c *coordinator) initiateShutdown() {
    c.cancel(nil)
}
```

### Phase 3: Subscriber の変更

**ファイル**: `subscriber.go`

```go
type Subscriber struct {
    // ... 既存フィールド ...

    coordinator atomic.Pointer[coordinator]
}

// Subscribe starts subscribing to the change stream.
// It blocks until Shutdown or Close is called, or endTimestamp is reached.
func (s *Subscriber) Subscribe(consumer Consumer) error {
    c := newCoordinator(
        s.spannerClient,
        s.streamName,
        s.partitionStorage,
        consumer,
        s.config,
    )

    s.coordinator.Store(c)
    defer s.coordinator.Store(nil)

    return c.run()
}

// Shutdown gracefully shuts down the subscriber.
// It stops accepting new partitions and waits for in-flight records to complete.
func (s *Subscriber) Shutdown(ctx context.Context) error {
    c := s.coordinator.Load()
    if c == nil {
        return nil
    }
    return c.shutdown(ctx)
}

// Close immediately closes the subscriber.
// It does not wait for in-flight records to complete.
func (s *Subscriber) Close() error {
    c := s.coordinator.Load()
    if c == nil {
        return nil
    }
    return c.close()
}
```

### Phase 4: partition_reader の変更

**ファイル**: `partition_reader.go`

```go
func (r *partitionReader) run(ctx context.Context) error {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    defer r.tracker.close()

    if err := r.partitionStorage.UpdateToRunning(ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to running: %w", err)
    }

    g, gctx := errgroup.WithContext(ctx)
    // ... goroutines 起動 ...

    err := g.Wait()

    if err != nil {
        if errors.Is(err, context.Canceled) {
            cause := context.Cause(ctx)
            if cause == nil {
                // graceful shutdown (Shutdown) → drain する
                r.drainInflight()
            }
            // 強制終了 (Close, recordError) → drain しない
            return err
        }
        return err
    }

    // 正常終了（endTimestamp まで読み切った）
    r.drainInflight()

    // UpdateToFinished は Spanner クライアントのタイムアウトに任せる
    if err := r.partitionStorage.UpdateToFinished(context.Background(), r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to finished: %w", err)
    }

    return nil
}

// drainInflight は in-flight の処理完了を待つ
// タイムアウトなし。Shutdown がタイムアウトしたら呼び出し側が Close を呼ぶ想定。
func (r *partitionReader) drainInflight() {
    r.tracker.initiateShutdown()
    r.tracker.waitAllCompleted(context.Background())
}
```

### Phase 5: coordinator.startPartitionReader の変更

```go
func (c *coordinator) startPartitionReader(partition *PartitionMetadata) {
    // ... 既存のコード ...

    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        err := reader.run(c.ctx)
        c.removeReader(partition.PartitionToken)
        if err != nil {
            if errors.Is(err, context.Canceled) {
                return  // Shutdown または Close
            }
            c.recordError(err)
        }
    }()
}
```

## 変更ファイル

| ファイル | 変更内容 |
|---------|---------|
| `errors.go` | `ErrSubscriberClosed` 追加 |
| `subscriber.go` | `Subscribe` から context 削除、`Shutdown`/`Close` 追加、`atomic.Pointer` 使用 |
| `coordinator.go` | `run()` が context を受け取らない、`shutdown`/`close` 追加 |
| `partition_reader.go` | `context.Cause` で drain 判定、`drainInflight()` 追加 |

## 動作フロー

### Graceful Shutdown

```
1. Shutdown(ctx) 呼び出し
2. c.cancel(nil) → cause = nil
3. partition_reader.run():
   - g.Wait() が context.Canceled を返す
   - context.Cause(ctx) == nil → drain 実行
   - return context.Canceled
4. c.wg.Wait() 完了
5. c.run() 終了 → close(c.done)
6. Shutdown() return nil
7. Subscribe() return ErrSubscriberClosed
```

### 強制終了 (Close)

```
1. Close() 呼び出し
2. c.cancel(ErrSubscriberClosed) → cause = ErrSubscriberClosed
3. partition_reader.run():
   - g.Wait() が context.Canceled を返す
   - context.Cause(ctx) != nil → drain スキップ
   - return context.Canceled
4. c.wg.Wait() 完了
5. c.run() 終了 → close(c.done)
6. Close() return nil
7. Subscribe() return ErrSubscriberClosed
```

## 検証方法

### 単体テスト

```go
func TestSubscriber_Shutdown(t *testing.T) {
    subscriber := NewSubscriber(...)

    done := make(chan error)
    go func() {
        done <- subscriber.Subscribe(consumer)
    }()

    time.Sleep(1 * time.Second)

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    err := subscriber.Shutdown(shutdownCtx)
    assert.NoError(t, err)

    assert.ErrorIs(t, <-done, ErrSubscriberClosed)
}

func TestSubscriber_Close(t *testing.T) {
    subscriber := NewSubscriber(...)

    done := make(chan error)
    go func() {
        done <- subscriber.Subscribe(consumer)
    }()

    time.Sleep(1 * time.Second)

    err := subscriber.Close()
    assert.NoError(t, err)

    assert.ErrorIs(t, <-done, ErrSubscriberClosed)
}
```

### 既存テストの更新

- `Subscribe(ctx, consumer)` → `Subscribe(consumer)` に変更
- または `SubscribeWithContext` を使用

## 今回スコープ外

- `initiateShutdown` の名前の整理
- drain timeout の configurable 化
- タイムアウト時のログ出力

## 参考

- [Go net/http source (server.go)](https://github.com/golang/go/blob/master/src/net/http/server.go)
- [Go issue #52805: context based graceful server shutdown](https://github.com/golang/go/issues/52805)
