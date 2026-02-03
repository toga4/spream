# ADR-003: 終了時の戻り値セマンティクスの整理

## ステータス

承認済み (2026-02-03 更新)

## 要約

`Subscribe()` の終了時戻り値を整理する:
- 全パーティション完了（EndTimestamp 到達）: `nil`
- Shutdown による途中終了: `ErrShutdown`
- Close による強制終了: `ErrClosed`

また、`Shutdown()` 呼び出し時に `Subscribe()` が即座に戻る設計を採用する（http.Server パターン）。

## コンテキスト

### 現状の実装

coordinator には複数の終了パスが存在する:

```go
func (c *coordinator) run() error {
    // ...
    if c.err != nil {
        return c.err
    }
    // Shutdown/Close による終了の場合
    if c.ctx.Err() != nil {
        return ErrSubscriberClosed
    }
    return nil
}

func (c *coordinator) initiateShutdown() {
    c.cancel(nil)  // 全パーティション完了時
}

func (c *coordinator) shutdown(ctx context.Context) error {
    c.cancel(nil)  // 外部からの graceful shutdown
    // ...
}

func (c *coordinator) close() error {
    c.cancel(ErrSubscriberClosed)  // 強制終了
    // ...
}
```

### 発見された問題

`run()` の終了判定で `c.ctx.Err() != nil` をチェックしているが、これでは `cancel(nil)` と `cancel(ErrSubscriberClosed)` を区別できない。

| トリガー | cancel 引数 | 現状の Subscribe() 戻り値 |
|---------|------------|-------------------------|
| 全パーティション完了 | `nil` | `ErrSubscriberClosed` |
| `Shutdown()` | `nil` | `ErrSubscriberClosed` |
| `Close()` | `ErrSubscriberClosed` | `ErrSubscriberClosed` |
| エラー発生 | そのエラー | そのエラー |

すべての終了パスが同じエラーを返しており、呼び出し側で区別できない。

## 検討した選択肢

### 選択肢 A: http.Server パターンに倣う

`http.Server` では:
- `Serve()` / `ListenAndServe()` は Shutdown/Close 後に必ず `ErrServerClosed` を返す
- `Shutdown()` 自体は成功時 `nil` を返す

**メリット:**
- 標準ライブラリと一貫したセマンティクス

**デメリット:**
- 「全パーティション完了」という正常終了も `ErrSubscriberClosed` になる
- Go の慣習（正常終了 = `nil`）に反する

### 選択肢 B: 正常終了は nil、強制終了は ErrSubscriberClosed

- 全パーティション完了 → `nil`
- `Shutdown()` → `nil`
- `Close()` → `ErrSubscriberClosed`

**メリット:**
- Go の慣習に沿っている
- 呼び出し側のコードが自然になる

**デメリット:**
- `http.Server` とは異なるセマンティクス

## 詳細な議論

### http.Server が常にエラーを返す理由

Go の Issue #11219 を調査した結果、`http.Server.Serve()` が常にエラーを返す設計は **意図的な設計ではなく、歴史的経緯と互換性制約の結果** であることが判明した。

#### 経緯

1. **Go 1 時代（2012年）**: `Serve` は Accept ループで、Listener が閉じられるとエラーで終了する設計だった。当時は graceful shutdown の仕組み自体がなく、「サーバーが止まる = 何か問題が起きた」という前提だった。

2. **Issue #11219（2015年）**: 「Serve が Listener close 時にエラーを返すのはおかしい、nil を返すべき」という提案があった。Brad Fitzpatrick の回答は **"The behavior of those functions is frozen."** であり、Go 1 互換性ポリシーにより変更できなかった。

3. **Go 1.8（2017年）**: `Shutdown()` と `ErrServerClosed` が導入されたが、Serve の戻り値セマンティクスは変更できなかった。代わりに sentinel error で「意図的な終了」を識別可能にした。

#### 結論

`http.Server` は互換性制約で変えられなかっただけであり、新規設計の spream がこれに倣う必要はない。実際、一部の開発者は「nil を返すべきだった」と考えている。

> "Some consider this was a mistake on the Go team's side, seeing no reason why returning nil wouldn't have been better and more properly aligned with Go conventions."

### context.Cause による終了理由の識別

Go 1.20 で導入された `context.WithCancelCause` と `context.Cause` を使えば、cancel 時に渡した値で終了理由を識別できる:

```go
c.ctx, c.cancel = context.WithCancelCause(context.Background())

// 終了時
c.cancel(errGracefulShutdown)  // initiateShutdown / shutdown（プライベートセンチネル）
c.cancel(ErrSubscriberClosed)  // close

// run() での判定
if cause := context.Cause(c.ctx); cause != nil && cause != errGracefulShutdown {
    return cause  // close() やエラーの場合
}
return nil  // initiateShutdown() / shutdown() の場合
```

**注**: `cancel(nil)` の場合、`context.Cause()` は `context.Canceled` を返す（Go の仕様）。そのため、正常終了を識別するにはプライベートなセンチネルエラー `errGracefulShutdown` を使用する。

### 呼び出し側のコード比較

**選択肢 A（http.Server パターン）:**

```go
err := subscriber.Subscribe()
if err != nil && !errors.Is(err, spream.ErrSubscriberClosed) {
    log.Fatal(err)
}
// 全パーティション完了も ErrSubscriberClosed なので区別できない
```

**選択肢 B（正常終了は nil）:**

```go
err := subscriber.Subscribe()
if err != nil {
    if errors.Is(err, spream.ErrSubscriberClosed) {
        log.Println("Subscriber was forcibly closed")
    } else {
        log.Fatal(err)
    }
}
// nil なら正常終了（全パーティション完了 or Shutdown）
```

選択肢 B の方が Go の慣習に沿っており、コードの意図が明確になる。

## 決定

**選択肢 B を基に拡張**: 全パーティション完了と Shutdown による途中終了を区別する。

| トリガー | Subscribe() 戻り値 | Shutdown() 戻り値 |
|---------|-------------------|-------------------|
| 全パーティション完了 | `nil` | - |
| `Shutdown()` 呼び出し | `ErrShutdown` (即座) | - |
| `Shutdown()` drain 完了 | - | `nil` |
| `Shutdown()` タイムアウト | - | `ctx.Err()` |
| `Close()` | `ErrClosed` | - |
| エラー発生 | そのエラー | - |

### エラー定義

```go
var (
    ErrShutdown = errors.New("shutdown")  // Shutdown による途中終了
    ErrClosed   = errors.New("closed")    // Close による強制終了
)
```

**注**: `ErrSubscriberClosed` は `ErrClosed` にリネーム。spream パッケージでは Subscriber が唯一の公開型であり、`spream.ErrClosed` で十分明確。

**注**: `ErrShutdownAborted` は削除。Shutdown のタイムアウト/キャンセル時は Go の慣習に従い `ctx.Err()` をそのまま返す。

## 理由

1. **完了と途中終了の区別**: spream には EndTimestamp という「完了」の概念がある。http.Server にはこの概念がないため、spream 独自のセマンティクスが必要
2. **運用上の重要性**: 完了（nil）なら再開不要、途中終了（ErrShutdown）なら次回続きから再開、という判断が可能
3. **Go の慣習に従う**: 完全な正常終了のみ `nil` を返す。Shutdown による途中終了は厳密には「正常終了」ではない
4. **http.Server パターンの採用**: Shutdown 呼び出しで Subscribe が即座に戻ることで、呼び出し側の制御フローがシンプルになる
5. **ctx.Err() の直接返却**: Go の慣習に従い、Shutdown のタイムアウト時は独自エラーではなく `ctx.Err()` を返す

## 結果

### 変更するコード

**errors.go:**

```go
// Before
var ErrSubscriberClosed = errors.New("subscriber closed")

// After
var (
    // 公開エラー
    ErrShutdown = errors.New("shutdown")
    ErrClosed   = errors.New("closed")
)

// 内部エラー（partition_reader が drain するかの判断に使用）
var errGracefulShutdown = errors.New("graceful shutdown")
```

**coordinator の新しいフィールド:**

```go
type coordinator struct {
    // 既存フィールド
    ctx    context.Context
    cancel context.CancelCauseFunc
    wg     sync.WaitGroup
    done   chan struct{}

    // 新規フィールド
    shutdownCh     chan struct{}   // Shutdown 開始シグナル（run() が監視）
    allReadersDone chan struct{}   // 全 reader 終了シグナル（shutdown/close が監視）
    shutdownFlag   atomic.Bool     // Shutdown が呼ばれたことを示すフラグ
    closedFlag     atomic.Bool     // Close が呼ばれたことを示すフラグ
}
```

**coordinator.run() の新しい実装:**

```go
func (c *coordinator) run() error {
    defer close(c.done)

    // wg.Wait() を goroutine で監視
    go func() {
        c.wg.Wait()
        close(c.allReadersDone)
    }()

    // 初期化
    if err := c.initialize(); err != nil {
        return fmt.Errorf("initialize: %w", err)
    }
    if err := c.resumeInterruptedPartitions(); err != nil {
        return fmt.Errorf("resume interrupted partitions: %w", err)
    }

    ticker := time.NewTicker(c.config.partitionDiscoveryInterval)
    defer ticker.Stop()

loop:
    for {
        select {
        case <-c.shutdownCh:
            break loop

        case <-c.allReadersDone:
            break loop

        case <-ticker.C:
            if err := c.detectAndSchedulePartitions(); err != nil {
                if errors.Is(err, errAllPartitionsFinished) {
                    continue  // allReadersDone を待つ
                }
                c.recordError(err)
                break loop
            }
        }
    }
    return c.exitError()
}

func (c *coordinator) exitError() error {
    // 優先順位: Close > Shutdown > エラー > 正常完了
    if c.closedFlag.Load() {
        return ErrClosed
    }
    if c.shutdownFlag.Load() {
        return ErrShutdown
    }
    if c.err != nil {
        return c.err
    }
    return nil  // EndTimestamp 到達（正常完了）
}
```

**coordinator.shutdown() の新しい実装:**

```go
func (c *coordinator) shutdown(ctx context.Context) error {
    c.shutdownOnce.Do(func() {
        c.shutdownFlag.Store(true)    // Shutdown フラグを立てる
        close(c.shutdownCh)           // run() を即座に戻す
        c.cancel(errGracefulShutdown) // reader に drain を開始させる
    })

    select {
    case <-c.allReadersDone:  // drain 完了
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

**coordinator.close() の新しい実装:**

```go
func (c *coordinator) close() error {
    c.closeOnce.Do(func() {
        c.closedFlag.Store(true)  // Close フラグを立てる
        c.cancel(ErrClosed)

        // reader を強制終了（drain を中断）
        c.mu.RLock()
        for _, reader := range c.readers {
            reader.Close()
        }
        c.mu.RUnlock()
    })

    <-c.allReadersDone  // reader の終了を待つ
    return nil
}
```

### フロー図

全ての終了パスで `exitError()` が呼ばれ、フラグの優先順位に従って戻り値が決定される。

```
exitError() の優先順位:
┌─────────────────────────────────────────────────────────────┐
│ 1. closedFlag == true   → ErrClosed                         │
│ 2. shutdownFlag == true → ErrShutdown                       │
│ 3. c.err != nil         → c.err                             │
│ 4. cause == errGracefulShutdown → nil (EndTimestamp 到達)   │
│ 5. その他               → cause                             │
└─────────────────────────────────────────────────────────────┘

正常完了 (EndTimestamp 到達):
┌─────────────────────────────────────────────────────────────┐
│ run()                                                       │
│   ├─ detectAndSchedulePartitions() → errAllPartitionsFinished
│   ├─ cancel(errGracefulShutdown)                            │
│   ├─ continue → <-allReadersDone                            │
│   ├─ break loop                                             │
│   └─ exitError() → nil                                      │
└─────────────────────────────────────────────────────────────┘

Shutdown:
┌─────────────────────────────────────────────────────────────┐
│ Shutdown(ctx)              │ run()                          │
│   │                        │   │                            │
│   ├─ shutdownFlag.Store(true)                               │
│   ├─ close(shutdownCh) ────│───│→ <-shutdownCh              │
│   │                        │   ├─ break loop                │
│   │                        │   └─ exitError() → ErrShutdown │
│   │                        │                                │
│   ├─ cancel(errGracefulShutdown)                            │
│   │   └─ reader が drain 開始                               │
│   │                        │                                │
│   ├─ <-allReadersDone ─────│── drain 完了                   │
│   └─ return nil            │                                │
└─────────────────────────────────────────────────────────────┘

Close:
┌─────────────────────────────────────────────────────────────┐
│ Close()                    │ run()                          │
│   │                        │   │                            │
│   ├─ closedFlag.Store(true)│   │                            │
│   ├─ cancel(ErrClosed)     │   │                            │
│   ├─ reader.Close() ───────│───│→ drain 中断                │
│   │                        │   ├─ <-allReadersDone          │
│   │                        │   ├─ break loop                │
│   │                        │   └─ exitError() → ErrClosed   │
│   │                        │                                │
│   ├─ <-allReadersDone      │                                │
│   └─ return nil            │                                │
└─────────────────────────────────────────────────────────────┘

Shutdown → Close (タイムアウト後):
┌─────────────────────────────────────────────────────────────┐
│ Shutdown(ctx)              │ Close()        │ run()         │
│   │                        │                │   │           │
│   ├─ shutdownFlag.Store(true)               │   │           │
│   ├─ close(shutdownCh) ────│────────────────│───│→ break loop
│   │                        │                │   └─ exitError() → ErrShutdown
│   ├─ cancel(errGracefulShutdown)            │               │
│   │                        │                │               │
│   ├─ <-ctx.Done() (timeout)│                │               │
│   └─ return ctx.Err()      │                │               │
│                            │                │               │
│                            ├─ closedFlag.Store(true)        │
│                            ├─ cancel(ErrClosed) (no-op)     │
│                            ├─ reader.Close() → drain 中断   │
│                            ├─ <-allReadersDone              │
│                            └─ return nil                    │
└─────────────────────────────────────────────────────────────┘

Shutdown 中に Close (race condition):
┌─────────────────────────────────────────────────────────────┐
│ どの select case が選ばれても exitError() が呼ばれるため、   │
│ closedFlag が true なら ErrClosed が返る。                  │
│ フラグの優先順位により一貫した結果が保証される。             │
└─────────────────────────────────────────────────────────────┘
```

### 影響

- `ErrSubscriberClosed` → `ErrClosed` にリネーム（破壊的変更）
- `ErrShutdown` 新規追加
- `ErrShutdownAborted` 削除
- `errGracefulShutdown`（内部）と `ErrShutdown`（公開）を分離
- Shutdown 呼び出しで Subscribe が即座に戻る（設計変更）
- coordinator に `shutdownCh`, `allReadersDone`, `shutdownFlag`, `closedFlag` を追加
- `exitError()` で戻り値の判定を一元化（優先順位: Close > Shutdown > エラー > 正常完了）

### ドキュメントへの記載事項

`Subscribe()` の godoc に以下を明記する:

```go
// Subscribe starts subscribing to the change stream.
// It blocks until one of the following occurs:
//   - All partitions are processed to endTimestamp (returns nil)
//   - Shutdown is called (returns ErrShutdown immediately)
//   - Close is called (returns ErrClosed)
//   - An error occurs (returns the error)
```

`Shutdown()` の godoc に以下を明記する:

```go
// Shutdown gracefully shuts down the subscriber.
// It causes Subscribe to return ErrShutdown immediately, then waits for
// in-flight records to complete (drain).
//
// If the context is canceled or times out before drain completes,
// Shutdown returns ctx.Err(). The drain continues in the background;
// call Close to abort it.
```

## 参考資料

- [Go Issue #11219: Server.Serve returns an error when the Listener is closed](https://github.com/golang/go/issues/11219)
- [Go Issue #26267: imprecise description of the return values of HTTP serve functions](https://github.com/golang/go/issues/26267)
- [SoByte: Go http server shutdown done right](https://www.sobyte.net/post/2021-10/go-http-server-shudown-done-right/)
- [Go 標準ライブラリ net/http/server.go](https://cs.opensource.google/go/go/+/refs/tags/go1.22.0:src/net/http/server.go)
