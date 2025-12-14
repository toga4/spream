# Cloud Spanner Change Stream Reader: Go 実装の最適設計

## エグゼクティブサマリー

In-flight Tracker を導入し、`net/http` パッケージの `http.Server` / `http.Handler` パターンを参考に、シンプルかつ強力な API 設計を行う。Consumer は `error` を返すだけでよく、spream 側が goroutine 管理と ack/nack 処理を完全に担当する。

---

## 1. 設計原則

### 1.1 http.Handler パターンとの対比

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        net/http vs spream 対比                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  net/http:                                                                      │
│  ─────────                                                                      │
│  type Handler interface {                                                       │
│      ServeHTTP(ResponseWriter, *Request)                                        │
│  }                                                                              │
│                                                                                 │
│  type Server struct {                                                           │
│      Handler Handler                                                            │
│      // ...                                                                     │
│  }                                                                              │
│                                                                                 │
│  server := &http.Server{Handler: myHandler}                                     │
│  server.ListenAndServe()                                                        │
│                                                                                 │
│  ─────────────────────────────────────────────────────────────────────────────  │
│                                                                                 │
│  spream:                                                                        │
│  ───────                                                                        │
│  type Consumer interface {                                                      │
│      Consume(ctx context.Context, change *DataChangeRecord) error               │
│  }                                                                              │
│                                                                                 │
│  subscriber := spream.NewSubscriber(client, streamName, storage, opts...)       │
│  subscriber.Subscribe(ctx, myConsumer)                                          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心的な設計思想

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              設計思想                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. Consumer はシンプルに                                                        │
│     ─────────────────────                                                       │
│     - error を返すだけで ack/nack を表現                                         │
│     - error == nil → 処理成功（ack）                                            │
│     - error != nil → 処理失敗（nack / エラーハンドリング）                        │
│     - ack() を呼ぶ必要なし、Message でラップする必要なし                          │
│                                                                                 │
│  2. 並行処理は spream が管理                                                     │
│     ────────────────────────                                                    │
│     - spream が goroutine を起動し Consumer を呼び出す                           │
│     - MaxInflight で同時実行数を制御                                             │
│     - Consumer は並行呼び出しされることを前提に実装                               │
│                                                                                 │
│  3. At-Least-Once は spream が保証                                              │
│     ─────────────────────────────                                               │
│     - 連続 ack されたところまでウォーターマークを更新                             │
│     - クラッシュ時は未 ack のレコードから再開                                    │
│                                                                                 │
│  4. バックプレッシャーは自然に                                                   │
│     ────────────────────────                                                    │
│     - MaxInflight がセマフォとして機能                                           │
│     - Consumer が遅い → 新しいレコードの読み取りがブロック                       │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 公開 API 設計

### 2.1 Consumer インターフェース

```go
// Consumer は DataChangeRecord を消費するインターフェース
//
// Consume は複数の goroutine から並行して呼び出される可能性がある。
// 実装はスレッドセーフでなければならない。
//
// 戻り値:
//   - nil: 処理成功。spream はこのレコードを ack し、ウォーターマークを進める
//   - error: 処理失敗。ErrorHandler の設定に従ってリトライまたは停止
//
// ctx がキャンセルされた場合、Consume は速やかに ctx.Err() を返すべきである。
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

// ConsumerFunc は関数を Consumer として使用するためのアダプタ
type ConsumerFunc func(context.Context, *DataChangeRecord) error

func (f ConsumerFunc) Consume(ctx context.Context, change *DataChangeRecord) error {
    return f(ctx, change)
}
```

### 2.2 Subscriber

```go
// Subscriber は Change Stream を購読する
type Subscriber struct {
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    config           *config
}

// NewSubscriber は新しい Subscriber を作成する
func NewSubscriber(
    client *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    options ...Option,
) *Subscriber

// Subscribe は Change Stream の購読を開始する
// ctx がキャンセルされるか、終了タイムスタンプに達するまでブロックする
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error

// SubscribeFunc は関数を Consumer として購読を開始する
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error
```

### 2.3 オプション

```go
// 既存オプション（変更なし）
func WithStartTimestamp(startTimestamp time.Time) Option
func WithEndTimestamp(endTimestamp time.Time) Option
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option
func WithSpannerRequestPriority(priority spannerpb.RequestOptions_Priority) Option

// 新規オプション
func WithMaxInflight(n int) Option                        // 最大同時処理数（デフォルト: 1）
func WithPartitionDiscoveryInterval(d time.Duration) Option // パーティション検出間隔（デフォルト: 1秒）
func WithErrorHandler(handler ErrorHandler) Option        // エラーハンドラー
```

### 2.4 ErrorHandler インターフェース

```go
// ErrorHandler はパーティション処理中のエラーを処理する
type ErrorHandler interface {
    // HandleError はエラー発生時に呼び出される
    //
    // 戻り値:
    //   - shouldContinue: true なら他のレコード処理を継続、false なら全体停止
    //   - retryDelay: 0 より大きければ指定時間後にこのレコードをリトライ
    //                 0 かつ shouldContinue=true ならこのレコードをスキップ（危険）
    HandleError(ctx context.Context, partition *PartitionMetadata, record *DataChangeRecord, err error) (shouldContinue bool, retryDelay time.Duration)
}

// ErrorHandlerFunc は関数を ErrorHandler として使用するためのアダプタ
type ErrorHandlerFunc func(context.Context, *PartitionMetadata, *DataChangeRecord, error) (bool, time.Duration)

func (f ErrorHandlerFunc) HandleError(ctx context.Context, partition *PartitionMetadata, record *DataChangeRecord, err error) (bool, time.Duration) {
    return f(ctx, partition, record, err)
}
```

---

## 3. 内部アーキテクチャ

### 3.1 全体構成

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Subscriber (公開)                                      │
│                              Subscribe(ctx, Consumer) error                              │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   coordinator (内部)                                     │
│                                                                                         │
│  責務:                                                                                  │
│    - パーティション検出ループの実行                                                      │
│    - partitionReader の起動・停止管理                                                    │
│    - グレースフルシャットダウン                                                          │
│                                                                                         │
│  ┌────────────────────────────────────┐                                                 │
│  │      partitionScheduler            │                                                 │
│  │  (マージ待機管理)                   │                                                 │
│  └────────────────────────────────────┘                                                 │
│                                                                                         │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   partitionReader   │     │   partitionReader   │     │   partitionReader   │
│    (goroutine)      │     │    (goroutine)      │     │    (goroutine)      │
│                     │     │                     │     │                     │
│  Partition Token A  │     │  Partition Token B  │     │  Partition Token C  │
│                     │     │                     │     │                     │
│  ┌───────────────┐  │     │  ┌───────────────┐  │     │  ┌───────────────┐  │
│  │inflightTracker│  │     │  │inflightTracker│  │     │  │inflightTracker│  │
│  └───────┬───────┘  │     │  └───────┬───────┘  │     │  └───────┬───────┘  │
│          │          │     │          │          │     │          │          │
│    ┌─────┼─────┐    │     │    ┌─────┼─────┐    │     │    ┌─────┼─────┐    │
│    ▼     ▼     ▼    │     │    ▼     ▼     ▼    │     │    ▼     ▼     ▼    │
│  [g1]  [g2]  [g3]   │     │  [g1]  [g2]  [g3]   │     │  [g1]  [g2]  [g3]   │
│   │     │     │     │     │   │     │     │     │     │   │     │     │     │
│   └─────┴─────┘     │     │   └─────┴─────┘     │     │   └─────┴─────┘     │
│         │           │     │         │           │     │         │           │
└─────────┼───────────┘     └─────────┼───────────┘     └─────────┼───────────┘
          │                           │                           │
          └───────────────────────────┼───────────────────────────┘
                                      │
                                      ▼
                          ┌───────────────────────┐
                          │ Consumer.Consume(ctx, │
                          │     DataChangeRecord) │
                          │        error          │
                          └───────────────────────┘

[g1], [g2], [g3] = goroutine per record (MaxInflight で数を制御)
```

### 3.2 Apache Beam との対応関係

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Apache Beam → Go 実装 マッピング                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Apache Beam                          │  spream (Go)                           │
│  ─────────────────────────────────────┼───────────────────────────────────────  │
│                                       │                                         │
│  Pipeline                             │  Subscriber.Subscribe()                 │
│                                       │                                         │
│  InitializeDoFn                       │  coordinator.initialize()               │
│                                       │                                         │
│  DetectNewPartitionsDoFn (SDF)        │  coordinator.detectNewPartitionsLoop()  │
│    - TimestampRangeTracker            │    - ticker ループ                      │
│    - ProcessContinuation.resume()     │    - goroutine 継続                     │
│                                       │                                         │
│  ReadChangeStreamPartitionDoFn (SDF)  │  partitionReader.run()                  │
│    - TimestampRangeTracker            │    - inflightTracker                    │
│    - ProcessContinuation.resume()     │    - goroutine per record               │
│                                       │                                         │
│  QueryChangeStreamAction              │  partitionReader.readStream()           │
│  DataChangeRecordAction               │  processDataChangeRecord()              │
│                                       │    → Consumer.Consume() in goroutine    │
│  HeartbeatRecordAction                │  processHeartbeatRecord()               │
│                                       │    → tracker.add() + complete() 即 ack  │
│  ChildPartitionsRecordAction          │  processChildPartitionsRecord()         │
│                                       │    → tracker.add() + complete() 即 ack  │
│                                       │    + partitionStorage.AddChildPartitions│
│                                       │                                         │
│  BundleFinalizer                      │  inflightTracker.markAcked()            │
│    - afterBundleCommit()              │    + UpdateWatermark()                  │
│                                       │                                         │
│  ManualWatermarkEstimator             │  inflightTracker.getSafeWatermark()     │
│                                       │                                         │
│  AsyncWatermarkCache                  │  (PartitionStorage 直接クエリ)          │
│                                       │                                         │
│  PartitionMetadataDao                 │  PartitionStorage                       │
│                                       │                                         │
│  ParDo の並列処理                      │  goroutine per record                   │
│                                       │                                         │
│  PCollection の伝播                    │  goroutine 内で Consumer 呼び出し       │
│                                       │                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. In-flight Tracker 詳細設計

### 4.1 連続 ack とウォーターマークの概念

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     連続 ack とウォーターマーク更新                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Change Stream から読み取り:                                                     │
│                                                                                 │
│    seq=1 (ts=10:00:01) ──▶ in-flight                                           │
│    seq=2 (ts=10:00:02) ──▶ in-flight                                           │
│    seq=3 (ts=10:00:03) ──▶ in-flight                                           │
│    seq=4 (ts=10:00:04) ──▶ in-flight                                           │
│    seq=5 (ts=10:00:05) ──▶ in-flight                                           │
│                                                                                 │
│  時刻 T1: seq=3 完了 (Consumer 成功)                                            │
│    - ackedSet = {3}                                                             │
│    - lastContinuousAcked = 0 (seq=1,2 が未完了)                                 │
│    - safeWatermark = なし                                                       │
│                                                                                 │
│  時刻 T2: seq=1 完了                                                            │
│    - ackedSet = {1, 3}                                                          │
│    - lastContinuousAcked = 1 (seq=1 のみ連続)                                   │
│    - safeWatermark = 10:00:01                                                   │
│                                                                                 │
│  時刻 T3: seq=2 完了                                                            │
│    - ackedSet = {1, 2, 3}                                                       │
│    - lastContinuousAcked = 3 (seq=1,2,3 が連続)                                 │
│    - safeWatermark = 10:00:03                                                   │
│                                                                                 │
│  時刻 T4: seq=5 完了                                                            │
│    - ackedSet = {1, 2, 3, 5}                                                    │
│    - lastContinuousAcked = 3 (seq=4 が未完了)                                   │
│    - safeWatermark = 10:00:03 (変わらず)                                        │
│                                                                                 │
│  時刻 T5: seq=4 完了                                                            │
│    - ackedSet = {1, 2, 3, 4, 5}                                                 │
│    - lastContinuousAcked = 5 (全て連続)                                         │
│    - safeWatermark = 10:00:05                                                   │
│                                                                                 │
│  ─────────────────────────────────────────────────────────────────────────────  │
│                                                                                 │
│  クラッシュ時の復旧:                                                             │
│    - safeWatermark (10:00:03) までが PartitionStorage に記録済み                │
│    - 再起動時は ts=10:00:03 から再開                                            │
│    - seq=4,5 のレコードは再処理される (at-least-once)                           │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 inflightTracker 構造体

`golang.org/x/sync/semaphore` を使用することで、セマフォ制御がシンプルになる。

```go
import (
    "sync/atomic"
    "golang.org/x/sync/semaphore"
)

// inflightTracker はパーティション内の in-flight レコードを追跡する
type inflightTracker struct {
    mu sync.Mutex

    // セマフォ（バックプレッシャー用）
    // golang.org/x/sync/semaphore を使用
    sem *semaphore.Weighted

    // シーケンス管理
    nextSeq int64 // 次に発行するシーケンス番号

    // 状態追跡（pending に acked フラグを統合）
    pending map[int64]*pendingRecord // seq -> レコード情報

    // 連続 ack 追跡
    lastContinuousAcked int64     // 連続 ack された最後のシーケンス
    safeWatermark       time.Time // 連続 ack されたウォーターマーク

    // イベント通知チャネル（正常系と異常系を分離）
    watermarks chan time.Time  // watermark 更新通知
    errors     chan error      // エラー通知

    // 新規レコード受付停止（acquire をブロック）
    // atomic.Bool で RWMutex を排除
    closed atomic.Bool

    // グレースフルシャットダウン
    // shutdown=true になると、pending=0 時に done を close する
    // atomic.Bool で RWMutex を排除
    shutdown atomic.Bool

    // waitAllCompleted 用
    // sync.OnceFunc (Go 1.21+) で done の close を一度だけ実行
    done      chan struct{}
    closeDone func()
}

type pendingRecord struct {
    seq       int64
    timestamp time.Time // CommitTimestamp
    acked     bool      // ack 済みかどうか
}
```

### 4.3 inflightTracker メソッド

```go
// newInflightTracker は新しい inflightTracker を作成する
func newInflightTracker(maxInflight int) *inflightTracker {
    t := &inflightTracker{
        sem:                 semaphore.NewWeighted(int64(maxInflight)),
        pending:             make(map[int64]*pendingRecord),
        lastContinuousAcked: 0,
        watermarks:          make(chan time.Time, maxInflight),
        errors:              make(chan error, maxInflight),
        done:                make(chan struct{}),
    }
    // sync.OnceFunc で done の close を一度だけ実行する関数を作成
    t.closeDone = sync.OnceFunc(func() { close(t.done) })
    return t
}

// acquire はセマフォを取得する（MaxInflight 制御）
// in-flight 数が maxInflight に達している場合はブロックする
// semaphore.Weighted を使用することで、context のキャンセルが自動的に処理される
func (t *inflightTracker) acquire(ctx context.Context) error {
    if t.closed.Load() {
        return errTrackerClosed
    }
    return t.sem.Acquire(ctx, 1)
}

// add は新しいレコードを in-flight に追加し、シーケンス番号を返す
func (t *inflightTracker) add(timestamp time.Time) int64 {
    t.mu.Lock()
    defer t.mu.Unlock()

    seq := t.nextSeq
    t.nextSeq++

    t.pending[seq] = &pendingRecord{
        seq:       seq,
        timestamp: timestamp,
    }

    return seq
}

// complete はレコードの処理完了を通知し、必要に応じてイベントを発行する
// この関数は goroutine から呼び出される（DataChangeRecord 用）
func (t *inflightTracker) complete(seq int64, err error) {
    t.mu.Lock()

    var newWatermark time.Time

    if err != nil {
        // エラーの場合: acked をマークしない（連続 ack は進まない）
        // セマフォ解放のみ行う
    } else {
        // 成功の場合: ack 処理と連続 ack 計算
        if rec, ok := t.pending[seq]; ok {
            rec.acked = true
        }
        newWatermark = t.advanceWatermark()
    }

    // セマフォ解放
    t.sem.Release(1)

    t.mu.Unlock()

    // mutex 解放後に channel 送信（デッドロック防止）
    if err != nil {
        t.errors <- err
    } else if !newWatermark.IsZero() {
        t.watermarks <- newWatermark
    }
}

// ackImmediate は即座に ack するレコード用（HeartbeatRecord, ChildPartitionsRecord）
// セマフォを使用しない（goroutine を生成しないため）
func (t *inflightTracker) ackImmediate(timestamp time.Time) {
    t.mu.Lock()

    seq := t.nextSeq
    t.nextSeq++

    t.pending[seq] = &pendingRecord{
        seq:       seq,
        timestamp: timestamp,
        acked:     true, // 即座に ack 済み
    }

    newWatermark := t.advanceWatermark()

    t.mu.Unlock()

    if !newWatermark.IsZero() {
        t.watermarks <- newWatermark
    }
}

// advanceWatermark は連続 ack を計算し、ウォーターマークを進める
// mu.Lock() を保持した状態で呼び出すこと
// 戻り値: 更新された watermark（更新がなければ zero value）
func (t *inflightTracker) advanceWatermark() time.Time {
    var watermark time.Time

    for {
        nextExpected := t.lastContinuousAcked + 1
        rec, ok := t.pending[nextExpected]
        if !ok || !rec.acked {
            break
        }

        if rec.timestamp.After(t.safeWatermark) {
            t.safeWatermark = rec.timestamp
            watermark = t.safeWatermark
        }

        t.lastContinuousAcked = nextExpected
        delete(t.pending, nextExpected)
    }

    // シャットダウンフェーズ中に pending が 0 になったら done を close
    if t.shutdown.Load() && len(t.pending) == 0 {
        t.closeDone()
    }

    return watermark
}

// initiateShutdown はグレースフルシャットダウンを開始する
// readStream() が終了した後に呼び出す
// これ以降、pending が 0 になると done が close される
func (t *inflightTracker) initiateShutdown() {
    t.shutdown.Store(true)

    // 既に pending が 0 なら即座に done を close
    t.mu.Lock()
    defer t.mu.Unlock()

    if len(t.pending) == 0 {
        t.closeDone()
    }
}

// getSafeWatermark は連続 ack されたウォーターマークを返す
func (t *inflightTracker) getSafeWatermark() time.Time {
    t.mu.Lock()
    defer t.mu.Unlock()

    return t.safeWatermark
}

// waitAllCompleted は全ての in-flight レコードが完了するまで待機する
func (t *inflightTracker) waitAllCompleted(ctx context.Context) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-t.done:
        return nil
    }
}

// close はトラッカーを閉じる
func (t *inflightTracker) close() {
    t.closed.Store(true)
    close(t.watermarks)
    close(t.errors)

    // done がまだ close されていなければ close
    t.closeDone()
}
```

### 4.4 同期プリミティブの選択

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         同期プリミティブの選択理由                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  用途                    │ 選択                    │ 理由                       │
│  ────────────────────────┼─────────────────────────┼────────────────────────────│
│  セマフォ                │ semaphore.Weighted      │ context 対応が組み込み     │
│                          │                         │ acquire() が ~4行で済む    │
│                          │                         │                            │
│  closed/shutdown フラグ  │ atomic.Bool             │ RWMutex より軽量           │
│                          │                         │ Load()/Store() で十分      │
│                          │                         │                            │
│  done の一度だけ close   │ sync.OnceFunc (Go 1.21) │ sync.Once + func より簡潔  │
│                          │                         │ closeDone() で呼び出し     │
│                          │                         │                            │
│  pending map             │ sync.Mutex              │ map は atomic 不可         │
│                          │                         │ 連続ack計算で一貫性が必要  │
│                          │                         │ acked を pending に統合    │
│                                                                                 │
│  ─────────────────────────────────────────────────────────────────────────────  │
│                                                                                 │
│  最適化の効果:                                                                  │
│    - RWMutex 2つ削除 → atomic.Bool 2つ                                         │
│    - sync.Once + func → sync.OnceFunc                                          │
│    - pending + acked map → pending map のみ（acked フラグを統合）              │
│    - results channel → watermarks + errors channel（正常系/異常系を分離）      │
│    - processResult 削除 → complete に統合                                       │
│    - 連続 ack 計算を advanceWatermark() に共通化                                │
│    - ackImmediate() 追加（HeartbeatRecord/ChildPartitionsRecord 用）           │
│    - acquire() が ~4行、close() が ~4行に簡略化                                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. partitionReader 詳細設計

### 5.1 処理フロー

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         partitionReader 処理フロー                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  partitionReader.run(ctx)                                                       │
│       │                                                                         │
│       ├── ctx, cancel := context.WithCancel(ctx)                                │
│       ├── defer cancel()                                                        │
│       ├── defer tracker.close()                                                 │
│       │                                                                         │
│       ├── UpdateToRunning                                                       │
│       │                                                                         │
│       └── errgroup.WithContext(ctx)                                             │
│               │                                                                 │
│               ├── g.Go: processWatermarks(gctx)  ◄──── tracker.watermarks      │
│               │       └── select { case watermark: UpdateWatermark() }          │
│               │                                                                 │
│               ├── g.Go: processErrors(gctx)  ◄──────── tracker.errors          │
│               │       └── select { case err: HandleError() }                    │
│               │                                                                 │
│               └── g.Go: readStream(gctx)                                        │
│                       └── iter.Do(func(row) error {                             │
│                               │                                                 │
│                               ├── DataChangeRecord:                             │
│                               │     ├── tracker.acquire(ctx)                    │
│                               │     ├── seq := tracker.add(timestamp)           │
│                               │     └── go func() {                             │
│                               │           err := consumer.Consume(ctx, record)  │
│                               │           tracker.complete(seq, err) ──────────┐│
│                               │         }()                          │         ││
│                               │                                      │         ││
│                               ├── HeartbeatRecord:                   │         ││
│                               │     └── tracker.ackImmediate(ts) ────┤         ││
│                               │         (セマフォ不要、即 ack)        │         ││
│                               │                                      │         ││
│                               └── ChildPartitionsRecord:             │         ││
│                                     ├── AddChildPartitions()         │         ││
│                                     ├── coordinator に通知           │         ││
│                                     └── tracker.ackImmediate(ts) ────┘         ││
│                                         (永続化→通知→ack の順)                 ││
│                           })                                                   ││
│                                                                                ││
│               g.Wait() ◄───────────────────────────────────────────────────────┘│
│                   │                                                             │
│                   ├── err != nil && !errors.Is(err, context.Canceled)           │
│                   │       └── return err  (異常終了: initiateShutdown しない)   │
│                   │                                                             │
│                   └── err == nil (正常終了)                                     │
│                           │                                                     │
│                           ├── tracker.initiateShutdown()                        │
│                           ├── tracker.waitAllCompleted()                        │
│                           └── UpdateToFinished                                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 partitionReader 実装

```go
type partitionReader struct {
    partition        *PartitionMetadata
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    config           *config

    tracker *inflightTracker

    // イベント通知
    events chan<- partitionEvent
}

func newPartitionReader(
    partition *PartitionMetadata,
    spannerClient *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    consumer Consumer,
    config *config,
    events chan<- partitionEvent,
) *partitionReader {
    return &partitionReader{
        partition:        partition,
        spannerClient:    spannerClient,
        streamName:       streamName,
        partitionStorage: partitionStorage,
        consumer:         consumer,
        config:           config,
        tracker:          newInflightTracker(config.maxInflight),
        events:           events,
    }
}

func (r *partitionReader) run(ctx context.Context) error {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()
    defer r.tracker.close()

    // 状態を RUNNING に更新
    if err := r.partitionStorage.UpdateToRunning(ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to running: %w", err)
    }

    // errgroup で並行処理を管理
    g, gctx := errgroup.WithContext(ctx)

    // watermark 更新処理
    g.Go(func() error {
        return r.processWatermarks(gctx)
    })

    // エラーハンドリング処理
    g.Go(func() error {
        return r.processErrors(gctx)
    })

    // Change Stream 読み取り
    g.Go(func() error {
        return r.readStream(gctx)
    })

    // 全 goroutine の完了を待機
    if err := g.Wait(); err != nil {
        // context.Canceled のみ正常終了として扱う
        // DeadlineExceeded は発生源が区別できないためエラーとして扱う
        if errors.Is(err, context.Canceled) {
            return nil
        }
        return err
    }

    // 正常終了の場合のみ initiateShutdown
    r.tracker.initiateShutdown()

    // 全 in-flight の完了を待機
    waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer waitCancel()
    if err := r.tracker.waitAllCompleted(waitCtx); err != nil {
        // タイムアウトしてもログを出して続行
    }

    // 状態を FINISHED に更新
    if err := r.partitionStorage.UpdateToFinished(ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to finished: %w", err)
    }

    return nil
}

// processWatermarks は watermark 更新を処理する
func (r *partitionReader) processWatermarks(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case watermark, ok := <-r.tracker.watermarks:
            if !ok {
                return nil
            }
            if err := r.partitionStorage.UpdateWatermark(
                ctx, r.partition.PartitionToken, watermark,
            ); err != nil {
                return fmt.Errorf("update watermark: %w", err)
            }
        }
    }
}

// processErrors はエラーを処理する
func (r *partitionReader) processErrors(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case err, ok := <-r.tracker.errors:
            if !ok {
                return nil
            }
            if r.config.errorHandler == nil {
                return err
            }
            shouldContinue, retryDelay := r.config.errorHandler.HandleError(
                ctx, r.partition, nil, err,
            )
            if !shouldContinue && retryDelay == 0 {
                return err
            }
            if retryDelay > 0 {
                // リトライロジック (実装詳細は省略)
            }
        }
    }
}

func (r *partitionReader) readStream(ctx context.Context) error {
    stmt := r.buildStatement()
    iter := r.spannerClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{
        Priority: r.config.spannerRequestPriority,
    })

    return iter.Do(func(row *spanner.Row) error {
        records := []*changeRecord{}
        if err := row.Columns(&records); err != nil {
            return err
        }
        return r.processRecords(ctx, records)
    })
}

func (r *partitionReader) processRecords(ctx context.Context, records []*changeRecord) error {
    for _, cr := range records {
        // DataChangeRecord の処理
        for _, record := range cr.DataChangeRecords {
            if err := r.processDataChangeRecord(ctx, record); err != nil {
                return err
            }
        }

        // HeartbeatRecord の処理
        // シーケンスを割り当て即座に完了することで、連続 ack のチェーンに含める
        for _, record := range cr.HeartbeatRecords {
            if err := r.processHeartbeatRecord(ctx, record); err != nil {
                return err
            }
        }

        // ChildPartitionsRecord の処理
        for _, record := range cr.ChildPartitionsRecords {
            if err := r.processChildPartitionsRecord(ctx, record); err != nil {
                return err
            }
        }
    }
    return nil
}

func (r *partitionReader) processDataChangeRecord(ctx context.Context, record *dataChangeRecord) error {
    // 1. セマフォを取得（MaxInflight に達していたらブロック）
    if err := r.tracker.acquire(ctx); err != nil {
        return err
    }

    // 2. in-flight に登録
    decoded := record.decodeToNonSpannerType()
    seq := r.tracker.add(record.CommitTimestamp)

    // 3. goroutine で Consumer を呼び出す
    go func() {
        err := r.consumer.Consume(ctx, decoded)
        r.tracker.complete(seq, err)
    }()

    return nil
}

func (r *partitionReader) processHeartbeatRecord(ctx context.Context, record *HeartbeatRecord) error {
    // HeartbeatRecord は Consumer を呼び出さないので、goroutine を生成しない。
    // そのためセマフォ（acquire）は不要。ackImmediate で即座に ack する。
    // これにより、DataChangeRecord が来ない間もウォーターマークが進む。
    r.tracker.ackImmediate(record.Timestamp)
    return nil
}

func (r *partitionReader) processChildPartitionsRecord(ctx context.Context, record *ChildPartitionsRecord) error {
    // ChildPartitionsRecord も goroutine を生成しないのでセマフォは不要。
    // 重要: 永続化 → 通知 → ack の順序で処理する。
    // ack を先にすると、永続化失敗時にリカバリできなくなる。

    // 1. 子パーティションを永続化
    if err := r.partitionStorage.AddChildPartitions(
        ctx,
        r.partition.EndTimestamp,
        r.partition.HeartbeatMillis,
        record,
    ); err != nil {
        return fmt.Errorf("add child partitions: %w", err)
    }

    // 2. coordinator に通知
    r.events <- partitionEvent{
        eventType:       eventChildPartitions,
        partition:       r.partition,
        childPartitions: record,
    }

    // 3. 最後に ack
    r.tracker.ackImmediate(record.StartTimestamp)

    return nil
}
```

---

## 6. coordinator 詳細設計

### 6.1 構造体

```go
type coordinator struct {
    // 依存
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    config           *config

    // パーティション管理
    mu        sync.RWMutex
    readers   map[string]*partitionReader // token -> reader
    scheduler *partitionScheduler

    // イベント
    events chan partitionEvent

    // 制御
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup

    // エラー
    err     error
    errOnce sync.Once
}

type partitionEvent struct {
    eventType       partitionEventType
    partition       *PartitionMetadata
    childPartitions *ChildPartitionsRecord
    err             error
}

type partitionEventType int

const (
    eventChildPartitions partitionEventType = iota
    eventPartitionFinished
    eventPartitionError
)
```

### 6.2 メインループ

```go
func (c *coordinator) run(ctx context.Context) error {
    c.ctx, c.cancel = context.WithCancel(ctx)
    defer c.cancel()

    // 1. 初期化
    if err := c.initialize(); err != nil {
        return fmt.Errorf("initialize: %w", err)
    }

    // 2. 中断されたパーティションを再開
    if err := c.resumeInterruptedPartitions(); err != nil {
        return fmt.Errorf("resume interrupted partitions: %w", err)
    }

    // 3. パーティション検出ループ
    c.wg.Add(1)
    go c.detectNewPartitionsLoop()

    // 4. イベント処理ループ
    c.wg.Add(1)
    go c.handleEvents()

    // 5. 完了を待機
    c.wg.Wait()

    if c.err != nil {
        return c.err
    }

    return c.ctx.Err()
}

func (c *coordinator) detectNewPartitionsLoop() {
    defer c.wg.Done()

    ticker := time.NewTicker(c.config.partitionDiscoveryInterval)
    defer ticker.Stop()

    for {
        select {
        case <-c.ctx.Done():
            return
        case <-ticker.C:
            if err := c.detectAndSchedulePartitions(); err != nil {
                if errors.Is(err, errAllPartitionsFinished) {
                    c.initiateShutdown()
                    return
                }
                c.recordError(err)
                return
            }
        }
    }
}

func (c *coordinator) handleEvents() {
    defer c.wg.Done()

    for {
        select {
        case <-c.ctx.Done():
            return
        case event := <-c.events:
            c.processEvent(event)
        }
    }
}
```

---

## 7. 使用例

### 7.1 シンプルな同期処理

```go
func main() {
    ctx := context.Background()

    client, _ := spanner.NewClient(ctx, "projects/my-project/instances/my-instance/databases/my-db")
    defer client.Close()

    storage := partitionstorage.NewSpanner(client, "change_stream_metadata")

    subscriber := spream.NewSubscriber(client, "MyChangeStream", storage)

    // シンプルに関数を渡すだけ
    err := subscriber.SubscribeFunc(ctx, func(ctx context.Context, change *spream.DataChangeRecord) error {
        log.Printf("Received: %s.%s %v", change.TableName, change.ModType, change.Mods)
        return nil // nil を返せば自動的に ack
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 7.2 並行処理（バッファリング相当）

```go
func main() {
    ctx := context.Background()

    client, _ := spanner.NewClient(ctx, "...")
    storage := partitionstorage.NewSpanner(client, "change_stream_metadata")

    // MaxInflight=100 で最大100件の並行処理を許可
    subscriber := spream.NewSubscriber(client, "MyChangeStream", storage,
        spream.WithMaxInflight(100),
    )

    // Consumer は並行呼び出しされる
    // 各 goroutine が独立して処理し、error で成否を返す
    err := subscriber.SubscribeFunc(ctx, func(ctx context.Context, change *spream.DataChangeRecord) error {
        // 重い処理（DB書き込み、外部API呼び出しなど）
        if err := processChange(ctx, change); err != nil {
            return err // エラーを返せば nack / リトライ
        }
        return nil // nil を返せば ack
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 7.3 エラーハンドリングのカスタマイズ

```go
func main() {
    ctx := context.Background()

    client, _ := spanner.NewClient(ctx, "...")
    storage := partitionstorage.NewSpanner(client, "change_stream_metadata")

    errorHandler := spream.ErrorHandlerFunc(func(
        ctx context.Context,
        partition *spream.PartitionMetadata,
        record *spream.DataChangeRecord,
        err error,
    ) (shouldContinue bool, retryDelay time.Duration) {
        log.Printf("Error processing record: %v", err)

        // 一時的なエラーなら1秒後にリトライ
        if isRetryable(err) {
            return true, time.Second
        }

        // 致命的なエラーなら停止
        return false, 0
    })

    subscriber := spream.NewSubscriber(client, "MyChangeStream", storage,
        spream.WithMaxInflight(100),
        spream.WithErrorHandler(errorHandler),
    )

    err := subscriber.Subscribe(ctx, &MyConsumer{})
    // ...
}
```

### 7.4 http.Handler パターンとの類似性

```go
// http.Handler パターン
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    // リクエスト処理
})
http.ListenAndServe(":8080", nil)

// spream パターン
subscriber.SubscribeFunc(ctx, func(ctx context.Context, change *spream.DataChangeRecord) error {
    // レコード処理
    return nil
})
```

---

## 8. 設計上の考慮事項

### 8.1 MaxInflight=1 での後方互換性

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       MaxInflight=1 での動作                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  MaxInflight=1 (デフォルト) の場合:                                              │
│                                                                                 │
│    record1 ──▶ acquire() ──▶ goroutine ──▶ Consumer.Consume()                  │
│                                              │                                  │
│    record2 ──▶ acquire() ◄── ブロック ◄──────┘ complete() で解放                │
│                   │                                                             │
│                   ▼                                                             │
│               goroutine ──▶ Consumer.Consume()                                  │
│                              │                                                  │
│    record3 ──▶ acquire() ◄──┘                                                   │
│                   │                                                             │
│                   ▼                                                             │
│               ...                                                               │
│                                                                                 │
│  効果:                                                                          │
│    - 1件ずつ順番に処理される                                                     │
│    - 現在の実装と同等の動作                                                      │
│    - 後方互換性を維持                                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 トレードオフ

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              トレードオフ                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  利点:                                                                          │
│  ─────                                                                          │
│  ✓ シンプルな API: Consumer は error を返すだけ                                  │
│  ✓ http.Handler パターンと同様の使いやすさ                                       │
│  ✓ At-Least-Once 保証を spream が担保                                           │
│  ✓ バックプレッシャーが自然に機能                                                │
│  ✓ MaxInflight=1 で後方互換                                                     │
│                                                                                 │
│  欠点/制限:                                                                      │
│  ─────────                                                                      │
│  △ API 変更: Consumer.Consume(ctx, change) error (context 追加)                │
│  △ Consumer は並行呼び出しされることを前提に実装が必要                            │
│  △ spream 側の実装複雑度が増加                                                   │
│  △ 順序保証: 同一パーティション内でも順序が保証されない (MaxInflight>1 の場合)    │
│                                                                                 │
│  順序保証について:                                                               │
│  ────────────────                                                               │
│  - Cloud Spanner Change Stream は「同一キーへの変更は同一パーティションで         │
│    タイムスタンプ順に配信」を保証                                                │
│  - しかし MaxInflight>1 の場合、Consumer の処理順序は保証されない                │
│  - 順序が重要な場合は MaxInflight=1 を使用するか、Consumer 側で順序制御           │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 8.3 メモリ管理

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                             メモリ管理                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  in-flight レコードのメモリ使用量:                                               │
│                                                                                 │
│    各 in-flight レコードが保持するもの:                                          │
│    - seq (int64): 8 bytes                                                       │
│    - timestamp (time.Time): 24 bytes                                            │
│    - DataChangeRecord: 可変 (数百 bytes 〜 数 KB)                               │
│    - goroutine: ~2-8 KB (スタック)                                              │
│                                                                                 │
│    MaxInflight=100 の場合:                                                       │
│    - 最大 100 goroutine × ~8 KB = ~800 KB スタック                              │
│    - 最大 100 レコード × ~数 KB = 数百 KB データ                                 │
│    - 合計: 1-2 MB 程度                                                          │
│                                                                                 │
│  対策:                                                                          │
│    - MaxInflight に適切な上限を設定（デフォルト=1、最大=1000 など）               │
│    - Consumer が完了しない場合のタイムアウト検出                                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. 移行計画

### 9.1 フェーズ分け

```
フェーズ 1: Consumer インターフェースの変更 (v2.0.0)
────────────────────────────────────────────────────
- Consumer.Consume(ctx, change) error への変更
- 内部実装は現行維持（同期処理）

フェーズ 2: In-flight Tracker の導入 (v2.1.0)
────────────────────────────────────────────────────
- Go 1.21 以上を要求（sync.OnceFunc 使用のため）
- golang.org/x/sync/semaphore への依存追加
- golang.org/x/sync/errgroup への依存追加
- inflightTracker の実装
  - semaphore.Weighted（バックプレッシャー）
  - atomic.Bool（closed/shutdown フラグ）
  - sync.OnceFunc（done channel の close）
  - watermarks/errors channel（正常系/異常系の分離）
- partitionReader の改修
  - errgroup による並行処理管理
  - processWatermarks/processErrors の分離
- coordinator の改修
- MaxInflight オプション追加（デフォルト=1 で後方互換）

フェーズ 3: エラーハンドリング強化 (v2.2.0)
────────────────────────────────────────────────────
- ErrorHandler インターフェース追加
- リトライロジックの実装

フェーズ 4: 観測可能性 (v2.3.0) [オプション]
────────────────────────────────────────────────────
- メトリクス（in-flight 数、処理レイテンシなど）
- トレーシング統合
```

### 9.2 ファイル構成

```
spream/
├── subscriber.go              # Subscriber (公開 API)
├── consumer.go                # Consumer インターフェース (公開 API)
├── option.go                  # Option パターン (公開 API)
├── error_handler.go           # ErrorHandler (公開 API)
├── change_record.go           # データ型 (変更なし)
├── partition_metadata.go      # パーティションメタデータ (変更なし)
├── coordinator.go             # 内部: coordinator
├── partition_reader.go        # 内部: partitionReader
├── partition_scheduler.go     # 内部: partitionScheduler
├── inflight_tracker.go        # 内部: inflightTracker ← 新規
├── errors.go                  # エラー定義
├── partitionstorage/
│   ├── interface.go           # PartitionStorage インターフェース
│   ├── inmemory.go            # InMemory 実装
│   └── spanner.go             # Spanner 実装
└── cmd/spream/
    └── main.go                # CLI ツール
```

---

## 10. 結論

この設計は以下の要件を満たす:

1. **シンプルな API**: `Consumer.Consume(ctx, change) error` のみ
2. **http.Handler パターン**: Subscriber に Consumer を登録するだけで利用可能
3. **At-Least-Once 保証**: 連続 ack されたところまでウォーターマークを更新
4. **並行処理**: MaxInflight で制御、goroutine per record
5. **後方互換性**: MaxInflight=1 で現行同等の動作
6. **バックプレッシャー**: セマフォによる自然な流量制御

Apache Beam のアーキテクチャを Go の特性（goroutine, channel, context）に適切にマッピングし、単一プロセスで動作するライブラリとして最適化された設計となっている。
