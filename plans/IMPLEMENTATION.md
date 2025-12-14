# spream リファクタリング実装計画

## 目次

1. [現在の実装分析](#1-現在の実装分析)
2. [公開 API インターフェース](#2-公開-api-インターフェース)
3. [API 変更計画](#3-api-変更計画)
4. [内部アーキテクチャ移行計画](#4-内部アーキテクチャ移行計画)
5. [詳細実装](#5-詳細実装)
6. [移行手順](#6-移行手順)

---

## 1. 現在の実装分析

### 1.1 アーキテクチャ概要

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              Subscriber                                                 │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  Subscribe(ctx, consumer)                                                              │
│       │                                                                                 │
│       ├── InitializeRootPartition (最初の実行時)                                        │
│       │                                                                                 │
│       ├── GetInterruptedPartitions → 各パーティションで queryChangeStream              │
│       │                                                                                 │
│       └── detectNewPartitions ループ (1秒ごと)                                          │
│              │                                                                          │
│              ├── GetUnfinishedMinWatermarkPartition                                    │
│              ├── GetSchedulablePartitions                                              │
│              ├── UpdateToScheduled                                                     │
│              └── 各パーティションで queryChangeStream (goroutine)                       │
│                                                                                         │
│  queryChangeStream(ctx, partition)                                                     │
│       │                                                                                 │
│       ├── UpdateToRunning                                                              │
│       ├── READ_ChangeStreamName クエリ実行                                             │
│       ├── handle() で各レコード処理                                                     │
│       │     ├── DataChangeRecord → consumer.Consume()                                  │
│       │     ├── HeartbeatRecord → watermark 更新                                       │
│       │     └── ChildPartitionsRecord → AddChildPartitions                             │
│       ├── UpdateWatermark                                                              │
│       └── UpdateToFinished                                                             │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 現在の問題点

| 問題 | 説明 | 影響 |
|------|------|------|
| **Consumer に context がない** | `Consume(change *DataChangeRecord) error` | Consumer がキャンセルを検知できない、タイムアウト制御不可 |
| **グレースフルシャットダウン不完全** | context.Cancel() のみ | 処理中のレコードが失われる可能性 |
| **errgroup の使い方** | エラー発生時に全体が停止 | 単一パーティションの一時的エラーで全体停止 |
| **ウォーターマーク管理が分散** | 各 goroutine が個別に更新 | グローバルウォーターマークの計算が PartitionStorage 依存 |
| **パーティション検出の間隔が固定** | 1秒固定 | 設定による調整不可 |

### 1.3 Apache Beam との比較

| 機能 | Apache Beam | 現在の spream |
|------|-------------|---------------|
| パーティション検出 | DetectNewPartitionsDoFn (SDF) | detectNewPartitions ループ |
| パーティション読み取り | ReadChangeStreamPartitionDoFn (SDF) | queryChangeStream goroutine |
| ウォーターマーク管理 | AsyncWatermarkCache + ManualWatermarkEstimator | watermarker + PartitionStorage |
| 状態管理 | PartitionMetadataDao | PartitionStorage |
| ソフトタイムアウト | RestrictionInterrupter | なし |
| バッチ処理 | 200件単位 | なし |

---

## 2. 公開 API インターフェース

### 2.1 現在の公開 API

```go
// === エントリーポイント ===
type Subscriber struct { /* 非公開フィールド */ }

func NewSubscriber(
    client *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    options ...Option,
) *Subscriber

func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error

// === Consumer インターフェース ===
type Consumer interface {
    Consume(change *DataChangeRecord) error  // ← context がない
}

type ConsumerFunc func(*DataChangeRecord) error

// === オプション ===
type Option interface {
    Apply(*config)
}

func WithStartTimestamp(startTimestamp time.Time) Option
func WithEndTimestamp(endTimestamp time.Time) Option
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option
func WithSpannerRequestPriority(priority spannerpb.RequestOptions_Priority) Option

// === PartitionStorage インターフェース ===
type PartitionStorage interface {
    GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error)
    GetInterruptedPartitions(ctx context.Context) ([]*PartitionMetadata, error)
    InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error
    GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error)
    AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, childPartitionsRecord *ChildPartitionsRecord) error
    UpdateToScheduled(ctx context.Context, partitionTokens []string) error
    UpdateToRunning(ctx context.Context, partitionToken string) error
    UpdateToFinished(ctx context.Context, partitionToken string) error
    UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error
}

// === データ型 ===
type PartitionMetadata struct { ... }
type State string
const RootPartitionToken = "Parent0"

type DataChangeRecord struct { ... }
type ColumnType struct { ... }
type Type struct { ... }
type TypeCode string
type Mod struct { ... }
type ModType string

type HeartbeatRecord struct { ... }
type ChildPartitionsRecord struct { ... }
type ChildPartition struct { ... }
```

---

## 3. API 変更計画

### 3.1 破壊的変更 (Breaking Changes)

#### 3.1.1 Consumer インターフェース

```go
// 変更前
type Consumer interface {
    Consume(change *DataChangeRecord) error
}

type ConsumerFunc func(*DataChangeRecord) error

// 変更後
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

type ConsumerFunc func(context.Context, *DataChangeRecord) error

func (f ConsumerFunc) Consume(ctx context.Context, change *DataChangeRecord) error {
    return f(ctx, change)
}
```

**理由**:
- Consumer がキャンセルを検知できるようにする
- タイムアウト制御を可能にする
- Go の慣習に従う（context は第一引数）

**移行ガイド**:
```go
// 変更前
func (l *Logger) Consume(change *spream.DataChangeRecord) error {
    return json.NewEncoder(l.out).Encode(change)
}

// 変更後
func (l *Logger) Consume(ctx context.Context, change *spream.DataChangeRecord) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        return json.NewEncoder(l.out).Encode(change)
    }
}
```

### 3.2 追加 API

#### 3.2.1 新しいオプション

```go
// パーティション検出間隔の設定
func WithPartitionDiscoveryInterval(interval time.Duration) Option

// 最大並列パーティション数の設定
func WithMaxConcurrentPartitions(max int) Option

// エラーハンドラーの設定
func WithErrorHandler(handler ErrorHandler) Option
```

#### 3.2.2 ErrorHandler インターフェース

```go
// ErrorHandler はエラー発生時の処理を定義する
type ErrorHandler interface {
    // HandleError はエラーを処理し、続行するかどうかを返す
    // true を返すと処理を続行、false を返すと処理を停止
    HandleError(ctx context.Context, partition *PartitionMetadata, err error) bool
}

type ErrorHandlerFunc func(ctx context.Context, partition *PartitionMetadata, err error) bool

func (f ErrorHandlerFunc) HandleError(ctx context.Context, partition *PartitionMetadata, err error) bool {
    return f(ctx, partition, err)
}
```

#### 3.2.3 Stop メソッド (オプション検討)

```go
// Stop はグレースフルシャットダウンを開始する
// すべての処理中のレコードが完了するまで待機する
func (s *Subscriber) Stop(ctx context.Context) error
```

**注意**: 現在は `Subscribe` に渡した context をキャンセルすることでシャットダウンが可能。
`Stop` メソッドを追加するかどうかは、グレースフルシャットダウンの要件次第。

### 3.3 PartitionStorage インターフェースの変更 (内部的)

内部実装の変更に伴い、いくつかのメソッドの使い方が変わるが、インターフェース自体は維持可能。

```go
// 追加を検討
type PartitionStorage interface {
    // 既存メソッド...

    // GetPartitionByToken は指定されたトークンのパーティションを取得
    // (マージ時の重複チェック用 - 内部で使用)
    GetPartitionByToken(ctx context.Context, token string) (*PartitionMetadata, error)
}
```

---

## 4. 内部アーキテクチャ移行計画

### 4.1 新アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Subscriber                                            │
│                              (エントリーポイント)                                         │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   coordinator                                           │
│                        (内部: 全体制御・パーティション管理)                                │
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │ 責務:                                                                           │   │
│  │   - パーティション検出ループの実行                                                │   │
│  │   - partitionReader の起動・停止管理                                             │   │
│  │   - グローバルウォーターマーク追跡                                                │   │
│  │   - グレースフルシャットダウン                                                    │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│  ┌────────────────────────────────────┐     ┌────────────────────────────────────┐    │
│  │      partitionScheduler            │     │       watermarkTracker            │    │
│  │  (新規パーティションのスケジュール)   │     │    (ウォーターマーク追跡)          │    │
│  └────────────────────────────────────┘     └────────────────────────────────────┘    │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   partitionReader   │     │   partitionReader   │     │   partitionReader   │
│    (goroutine)      │     │    (goroutine)      │     │    (goroutine)      │
│                     │     │                     │     │                     │
│  Partition Token A  │     │  Partition Token B  │     │  Partition Token C  │
└──────────┬──────────┘     └──────────┬──────────┘     └──────────┬──────────┘
           │                           │                           │
           └───────────────────────────┼───────────────────────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │   Consumer.Consume()  │
                           └───────────────────────┘
```

### 4.2 内部コンポーネント

#### 4.2.1 coordinator (非公開)

```go
// coordinator はパーティションの検出とリーダーの管理を行う
type coordinator struct {
    mu sync.RWMutex

    // 依存コンポーネント
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    config           *config

    // パーティション管理
    readers          map[string]*partitionReader
    scheduler        *partitionScheduler
    watermarkTracker *watermarkTracker

    // チャネル
    partitionEvents  chan partitionEvent

    // 制御
    ctx              context.Context
    cancel           context.CancelFunc
    wg               sync.WaitGroup

    // エラーハンドリング
    errorHandler     ErrorHandler
    firstError       error
    firstErrorOnce   sync.Once
}
```

#### 4.2.2 partitionReader (非公開)

```go
// partitionReader は単一パーティションの Change Stream を読み取る
type partitionReader struct {
    partition        *PartitionMetadata
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    config           *config

    // イベント通知
    events           chan<- partitionEvent

    // 制御
    ctx              context.Context
    cancel           context.CancelFunc
}

type partitionEvent struct {
    eventType        partitionEventType
    partition        *PartitionMetadata
    childPartitions  *ChildPartitionsRecord
    watermark        time.Time
    err              error
}

type partitionEventType int

const (
    eventChildPartitions partitionEventType = iota
    eventWatermarkUpdate
    eventPartitionFinished
    eventPartitionError
)
```

#### 4.2.3 partitionScheduler (非公開)

```go
// partitionScheduler は新しいパーティションのスケジューリングを管理する
type partitionScheduler struct {
    mu sync.Mutex

    // マージ待機中のパーティション
    pendingMerge map[string]*pendingMergeState
}

type pendingMergeState struct {
    childPartition ChildPartition
    startTimestamp time.Time
    receivedFrom   map[string]bool
    allParents     []string
}
```

#### 4.2.4 watermarkTracker (非公開)

```go
// watermarkTracker は各パーティションのウォーターマークを追跡する
type watermarkTracker struct {
    mu sync.RWMutex

    watermarks      map[string]time.Time
    globalWatermark time.Time
}
```

---

## 5. 詳細実装

### 5.1 Subscriber の変更

```go
// Subscriber subscribes change stream.
type Subscriber struct {
    spannerClient          *spanner.Client
    streamName             string
    partitionStorage       PartitionStorage
    config                 *config
}

func NewSubscriber(
    client *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    options ...Option,
) *Subscriber {
    c := &config{
        startTimestamp:             time.Now(),
        endTimestamp:               defaultEndTimestamp,
        heartbeatInterval:          defaultHeartbeatInterval,
        partitionDiscoveryInterval: time.Second,
        maxConcurrentPartitions:    0, // 0 = unlimited
        errorHandler:               nil,
    }
    for _, o := range options {
        o.Apply(c)
    }

    return &Subscriber{
        spannerClient:    client,
        streamName:       streamName,
        partitionStorage: partitionStorage,
        config:           c,
    }
}

// Subscribe starts subscribing to the change stream.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
    coord := newCoordinator(
        s.spannerClient,
        s.streamName,
        s.partitionStorage,
        consumer,
        s.config,
    )

    return coord.run(ctx)
}

// SubscribeFunc is an adapter to allow the use of ordinary functions as Consumer.
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error {
    return s.Subscribe(ctx, f)
}
```

### 5.2 coordinator の実装

```go
func newCoordinator(
    spannerClient *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    consumer Consumer,
    config *config,
) *coordinator {
    return &coordinator{
        spannerClient:    spannerClient,
        streamName:       streamName,
        partitionStorage: partitionStorage,
        consumer:         consumer,
        config:           config,
        readers:          make(map[string]*partitionReader),
        scheduler:        newPartitionScheduler(),
        watermarkTracker: newWatermarkTracker(),
        partitionEvents:  make(chan partitionEvent, 100),
        errorHandler:     config.errorHandler,
    }
}

func (c *coordinator) run(ctx context.Context) error {
    c.ctx, c.cancel = context.WithCancel(ctx)
    defer c.cancel()

    // 1. 初期化
    if err := c.initialize(c.ctx); err != nil {
        return fmt.Errorf("failed to initialize: %w", err)
    }

    // 2. 中断されたパーティションを再開
    if err := c.resumeInterruptedPartitions(c.ctx); err != nil {
        return fmt.Errorf("failed to resume interrupted partitions: %w", err)
    }

    // 3. パーティション検出ループ
    c.wg.Add(1)
    go c.detectNewPartitionsLoop()

    // 4. パーティションイベント処理ループ
    c.wg.Add(1)
    go c.handlePartitionEvents()

    // 5. 完了を待機
    c.wg.Wait()

    // エラーがあれば返す
    if c.firstError != nil {
        return c.firstError
    }

    return c.ctx.Err()
}

func (c *coordinator) initialize(ctx context.Context) error {
    // 未完了パーティションがなければルートパーティションを初期化
    minWatermarkPartition, err := c.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
    if err != nil {
        return err
    }

    if minWatermarkPartition == nil {
        if err := c.partitionStorage.InitializeRootPartition(
            ctx,
            c.config.startTimestamp,
            c.config.endTimestamp,
            c.config.heartbeatInterval,
        ); err != nil {
            return err
        }
    }

    return nil
}

func (c *coordinator) resumeInterruptedPartitions(ctx context.Context) error {
    partitions, err := c.partitionStorage.GetInterruptedPartitions(ctx)
    if err != nil {
        return err
    }

    for _, p := range partitions {
        if err := c.startPartitionReader(p); err != nil {
            return err
        }
    }

    return nil
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
                if errors.Is(err, errDone) {
                    // すべてのパーティションが完了
                    c.initiateShutdown()
                    return
                }
                c.recordError(err)
                return
            }
        }
    }
}

func (c *coordinator) detectAndSchedulePartitions() error {
    minWatermarkPartition, err := c.partitionStorage.GetUnfinishedMinWatermarkPartition(c.ctx)
    if err != nil {
        return fmt.Errorf("failed to get unfinished min watermark partition: %w", err)
    }

    if minWatermarkPartition == nil {
        return errDone
    }

    partitions, err := c.partitionStorage.GetSchedulablePartitions(c.ctx, minWatermarkPartition.Watermark)
    if err != nil {
        return fmt.Errorf("failed to get schedulable partitions: %w", err)
    }

    if len(partitions) == 0 {
        return nil
    }

    // 最大並列数チェック
    if c.config.maxConcurrentPartitions > 0 {
        c.mu.RLock()
        currentCount := len(c.readers)
        c.mu.RUnlock()

        available := c.config.maxConcurrentPartitions - currentCount
        if available <= 0 {
            return nil
        }
        if len(partitions) > available {
            partitions = partitions[:available]
        }
    }

    partitionTokens := make([]string, 0, len(partitions))
    for _, p := range partitions {
        partitionTokens = append(partitionTokens, p.PartitionToken)
    }

    if err := c.partitionStorage.UpdateToScheduled(c.ctx, partitionTokens); err != nil {
        return fmt.Errorf("failed to update to scheduled: %w", err)
    }

    for _, p := range partitions {
        if err := c.startPartitionReader(p); err != nil {
            return err
        }
    }

    return nil
}

func (c *coordinator) startPartitionReader(partition *PartitionMetadata) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // 既に実行中なら何もしない
    if _, exists := c.readers[partition.PartitionToken]; exists {
        return nil
    }

    reader := newPartitionReader(
        partition,
        c.spannerClient,
        c.streamName,
        c.partitionStorage,
        c.consumer,
        c.config,
        c.partitionEvents,
    )

    c.readers[partition.PartitionToken] = reader
    c.watermarkTracker.Set(partition.PartitionToken, partition.Watermark)

    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        reader.run(c.ctx)
    }()

    return nil
}

func (c *coordinator) handlePartitionEvents() {
    defer c.wg.Done()

    for {
        select {
        case <-c.ctx.Done():
            return
        case event := <-c.partitionEvents:
            c.processPartitionEvent(event)
        }
    }
}

func (c *coordinator) processPartitionEvent(event partitionEvent) {
    switch event.eventType {
    case eventChildPartitions:
        c.handleChildPartitions(event)

    case eventWatermarkUpdate:
        c.watermarkTracker.Set(event.partition.PartitionToken, event.watermark)

    case eventPartitionFinished:
        c.handlePartitionFinished(event)

    case eventPartitionError:
        c.handlePartitionError(event)
    }
}

func (c *coordinator) handleChildPartitions(event partitionEvent) {
    for _, child := range event.childPartitions.ChildPartitions {
        scheduled := c.scheduler.Schedule(child, event.childPartitions.StartTimestamp, event.partition.PartitionToken)
        if scheduled != nil {
            // 子パーティションの追加は partitionReader 側で行われる
            // ここでは特に何もしない（PartitionStorage 経由で検出される）
        }
    }
}

func (c *coordinator) handlePartitionFinished(event partitionEvent) {
    c.mu.Lock()
    delete(c.readers, event.partition.PartitionToken)
    c.mu.Unlock()

    c.watermarkTracker.Remove(event.partition.PartitionToken)
}

func (c *coordinator) handlePartitionError(event partitionEvent) {
    // エラーハンドラーがあれば呼び出す
    if c.errorHandler != nil {
        shouldContinue := c.errorHandler.HandleError(c.ctx, event.partition, event.err)
        if shouldContinue {
            return
        }
    }

    c.recordError(event.err)
}

func (c *coordinator) recordError(err error) {
    c.firstErrorOnce.Do(func() {
        c.firstError = err
        c.cancel()
    })
}

func (c *coordinator) initiateShutdown() {
    c.cancel()
}
```

### 5.3 partitionReader の実装

```go
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
        events:           events,
    }
}

func (r *partitionReader) run(ctx context.Context) {
    r.ctx, r.cancel = context.WithCancel(ctx)
    defer r.cancel()

    err := r.readPartition()
    if err != nil {
        r.events <- partitionEvent{
            eventType: eventPartitionError,
            partition: r.partition,
            err:       err,
        }
        return
    }

    r.events <- partitionEvent{
        eventType: eventPartitionFinished,
        partition: r.partition,
    }
}

func (r *partitionReader) readPartition() error {
    // 状態を RUNNING に更新
    if err := r.partitionStorage.UpdateToRunning(r.ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("failed to update to running: %w", err)
    }

    // Change Stream クエリ実行
    stmt := spanner.Statement{
        SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)", r.streamName),
        Params: map[string]any{
            "startTimestamp":        r.partition.Watermark,
            "endTimestamp":          r.partition.EndTimestamp,
            "partitionToken":        r.partition.PartitionToken,
            "heartbeatMilliseconds": r.partition.HeartbeatMillis,
        },
    }

    if r.partition.IsRootPartition() {
        stmt.Params["partitionToken"] = nil
    }

    iter := r.spannerClient.Single().QueryWithOptions(r.ctx, stmt, spanner.QueryOptions{
        Priority: r.config.spannerRequestPriority,
    })

    if err := iter.Do(func(row *spanner.Row) error {
        records := []*changeRecord{}
        if err := row.Columns(&records); err != nil {
            return err
        }
        return r.handleRecords(records)
    }); err != nil {
        return err
    }

    // 状態を FINISHED に更新
    if err := r.partitionStorage.UpdateToFinished(r.ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("failed to update to finished: %w", err)
    }

    return nil
}

func (r *partitionReader) handleRecords(records []*changeRecord) error {
    var latestWatermark time.Time

    for _, cr := range records {
        // DataChangeRecord の処理
        for _, record := range cr.DataChangeRecords {
            // context を渡して Consumer を呼び出す
            if err := r.consumer.Consume(r.ctx, record.decodeToNonSpannerType()); err != nil {
                return err
            }
            if record.CommitTimestamp.After(latestWatermark) {
                latestWatermark = record.CommitTimestamp
            }
        }

        // HeartbeatRecord の処理
        for _, record := range cr.HeartbeatRecords {
            if record.Timestamp.After(latestWatermark) {
                latestWatermark = record.Timestamp
            }
        }

        // ChildPartitionsRecord の処理
        for _, record := range cr.ChildPartitionsRecords {
            if err := r.partitionStorage.AddChildPartitions(
                r.ctx,
                r.partition.EndTimestamp,
                r.partition.HeartbeatMillis,
                record,
            ); err != nil {
                return fmt.Errorf("failed to add child partitions: %w", err)
            }

            // coordinator に通知
            r.events <- partitionEvent{
                eventType:       eventChildPartitions,
                partition:       r.partition,
                childPartitions: record,
            }

            if record.StartTimestamp.After(latestWatermark) {
                latestWatermark = record.StartTimestamp
            }
        }
    }

    // ウォーターマーク更新
    if !latestWatermark.IsZero() {
        if err := r.partitionStorage.UpdateWatermark(r.ctx, r.partition.PartitionToken, latestWatermark); err != nil {
            return fmt.Errorf("failed to update watermark: %w", err)
        }

        r.events <- partitionEvent{
            eventType: eventWatermarkUpdate,
            partition: r.partition,
            watermark: latestWatermark,
        }
    }

    return nil
}
```

### 5.4 partitionScheduler の実装

```go
func newPartitionScheduler() *partitionScheduler {
    return &partitionScheduler{
        pendingMerge: make(map[string]*pendingMergeState),
    }
}

// Schedule は子パーティションをスケジュールする
// マージの場合、すべての親から報告を受けるまで待機する
// スケジュールされた場合は true を返す
func (s *partitionScheduler) Schedule(child ChildPartition, startTimestamp time.Time, fromParent string) *ChildPartition {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 分割の場合 (親が1つ)
    if len(child.ParentPartitionTokens) == 1 {
        return &child
    }

    // マージの場合 (親が複数)
    key := child.Token
    pending, exists := s.pendingMerge[key]
    if !exists {
        pending = &pendingMergeState{
            childPartition: child,
            startTimestamp: startTimestamp,
            receivedFrom:   make(map[string]bool),
            allParents:     child.ParentPartitionTokens,
        }
        s.pendingMerge[key] = pending
    }

    pending.receivedFrom[fromParent] = true

    // すべての親から受信したかチェック
    if len(pending.receivedFrom) == len(pending.allParents) {
        delete(s.pendingMerge, key)
        return &pending.childPartition
    }

    return nil
}
```

### 5.5 watermarkTracker の実装

```go
func newWatermarkTracker() *watermarkTracker {
    return &watermarkTracker{
        watermarks: make(map[string]time.Time),
    }
}

func (t *watermarkTracker) Set(token string, watermark time.Time) {
    t.mu.Lock()
    defer t.mu.Unlock()

    t.watermarks[token] = watermark
    t.recalculateGlobal()
}

func (t *watermarkTracker) Remove(token string) {
    t.mu.Lock()
    defer t.mu.Unlock()

    delete(t.watermarks, token)
    t.recalculateGlobal()
}

func (t *watermarkTracker) GetGlobal() time.Time {
    t.mu.RLock()
    defer t.mu.RUnlock()

    return t.globalWatermark
}

func (t *watermarkTracker) recalculateGlobal() {
    var minWatermark time.Time
    first := true

    for _, wm := range t.watermarks {
        if first || wm.Before(minWatermark) {
            minWatermark = wm
            first = false
        }
    }

    t.globalWatermark = minWatermark
}
```

---

## 6. 移行手順

### 6.1 フェーズ 1: Consumer インターフェースの変更

**目的**: 破壊的変更を先に行い、ユーザーに移行時間を与える

1. Consumer インターフェースに context を追加
2. ConsumerFunc の署名を変更
3. 内部の consumer 呼び出しを更新
4. ドキュメントと例を更新

```go
// 変更後
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}
```

**影響範囲**:
- `subscriber.go`: `Consumer` インターフェース、`ConsumerFunc` 型、`Consume` メソッド
- `subscriber_example_test.go`: 例の更新
- `subscriber_example_with_options_test.go`: 例の更新
- `cmd/spream/main.go`: `jsonOutputConsumer` の更新

### 6.2 フェーズ 2: 内部アーキテクチャの刷新

**目的**: 公開 API を維持しながら内部実装を Apache Beam スタイルに移行

1. 新しい内部コンポーネントの追加
   - `coordinator.go` (非公開)
   - `partition_reader.go` (非公開)
   - `partition_scheduler.go` (非公開)
   - `watermark_tracker.go` (非公開)

2. `Subscriber.Subscribe` の内部実装を `coordinator` に委譲

3. テストの追加・更新

### 6.3 フェーズ 3: 新機能の追加

**目的**: 追加 API と機能強化

1. 新しいオプションの追加
   - `WithPartitionDiscoveryInterval`
   - `WithMaxConcurrentPartitions`
   - `WithErrorHandler`

2. `ErrorHandler` インターフェースの追加

### 6.4 ファイル構成 (提案)

```
spream/
├── subscriber.go              # Subscriber (公開 API)
├── consumer.go                # Consumer インターフェース (公開 API) ← 新規
├── option.go                  # Option パターン (公開 API) ← 新規 or subscriber.go から分離
├── change_record.go           # データ型 (変更なし)
├── partition_metadata.go      # パーティションメタデータ (変更なし)
├── coordinator.go             # 内部: coordinator ← 新規
├── partition_reader.go        # 内部: partitionReader ← 新規
├── partition_scheduler.go     # 内部: partitionScheduler ← 新規
├── watermark_tracker.go       # 内部: watermarkTracker ← 新規
├── errors.go                  # エラー定義 ← 新規
├── partitionstorage/
│   ├── inmemory.go           # (変更なし)
│   └── spanner.go            # (変更なし)
└── cmd/spream/
    └── main.go               # CLI ツール (Consumer 変更に対応)
```

---

## 補足: 後方互換性オプション (検討用)

Consumer の破壊的変更を緩和するため、移行期間中は非推奨の古いインターフェースを維持することも可能：

```go
// Deprecated: Use Consumer instead.
type LegacyConsumer interface {
    Consume(change *DataChangeRecord) error
}

// ConsumerAdapter は LegacyConsumer を Consumer に変換する
func ConsumerAdapter(legacy LegacyConsumer) Consumer {
    return ConsumerFunc(func(ctx context.Context, change *DataChangeRecord) error {
        return legacy.Consume(change)
    })
}

// SubscribeLegacy は後方互換性のために提供される
// Deprecated: Use Subscribe with Consumer interface instead.
func (s *Subscriber) SubscribeLegacy(ctx context.Context, consumer LegacyConsumer) error {
    return s.Subscribe(ctx, ConsumerAdapter(consumer))
}
```

ただし、これは複雑さを増すため、セマンティックバージョニングに従って v1.0.0 のメジャーバージョンアップとして破壊的変更を導入するのが望ましい。
