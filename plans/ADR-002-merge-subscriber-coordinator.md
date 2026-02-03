# Subscriber と coordinator の統合計画

## 概要

Subscriber 構造体と coordinator 構造体を統合し、API を Config + NewSubscriber パターンに変更する。

## 背景

現状の問題:
- Subscriber は薄いラッパーで、coordinator に処理を委譲しているだけ
- spannerClient, streamName, partitionStorage, config が両方に重複
- `atomic.Pointer[coordinator]` による間接参照が複雑

### 現在の実装状況（2025-01時点）

- `subscriber.go`: Subscriber は coordinator を生成し、処理を委譲
- `coordinator.go`: 実際のパーティション管理ロジックを保持
- `option.go`: Functional Options パターン（`WithStartTimestamp` 等）
- `partition_reader.go`: **config 依存は解消済み**（`maxInflight` を直接受け取る形に変更済み）
- `errors.go`: `ErrShutdown`, `ErrClosed` を定義

## 設計方針

1. **Config 構造体を公開** - 設定を渡すための一時的な構造体
2. **Subscriber のフィールドは全て非公開** - 書き換え不可
3. **NewSubscriber(cfg *Config)** - バリデーションとデフォルト値適用
4. **Subscribe() は引数なし** - consumer は Config で渡す
5. **Subscribe() は一度しか呼べない** - http.Server パターン（再利用したければ新しい Subscriber を作成）

## API 変更

### Before（現在の API）

```go
sub := spream.NewSubscriber(client, stream, storage,
    spream.WithMaxInflight(10),
)
err := sub.Subscribe(consumer)
// または
err := sub.SubscribeFunc(func(ctx context.Context, change *spream.DataChangeRecord) error {
    // ...
    return nil
})
```

### After（変更後の API）

```go
sub, err := spream.NewSubscriber(&spream.Config{
    SpannerClient:    client,
    StreamName:       stream,
    PartitionStorage: storage,
    Consumer:         consumer,
    MaxInflight:      10,
})
if err != nil {
    // handle error
}
err = sub.Subscribe()
```

## 変更対象ファイル

### 削除

- `coordinator.go` - Subscriber に統合
- `coordinator_test.go` - subscriber_test.go に統合

### 大幅変更

- `subscriber.go` - coordinator のロジックを統合
- `option.go` → `config.go` - Config 構造体に置き換え、Functional Options 削除

### 変更不要

- `partition_reader.go` - **✅ 既に config 依存を解消済み**（maxInflight を直接受け取る形に変更済み）

## 新しい構造体定義

### Config（公開）

```go
type Config struct {
    // 必須
    SpannerClient    *spanner.Client
    StreamName       string
    PartitionStorage PartitionStorage
    Consumer         Consumer

    // オプション（ゼロ値ならデフォルト適用）
    BaseContext                context.Context // default: context.Background()
    StartTimestamp             time.Time       // default: time.Now()
    EndTimestamp               time.Time       // default: 9999-12-31
    HeartbeatInterval          time.Duration   // default: 10s
    MaxInflight                int             // default: 1
    PartitionDiscoveryInterval time.Duration   // default: 1s
}
```

`BaseContext` は Subscriber 内部で使用するコンテキストの親となる。これにより：
- 外部からのキャンセル伝播
- トレーシングコンテキストの引き継ぎ
- デッドラインの設定

が可能になる。

注: `SpannerRequestPriority` は `spanner.ClientConfig.QueryOptions.Priority` で設定するため、ライブラリの Config には含めない。CLI (`cmd/spream`) では `spanner.NewClientWithConfig` で設定している。

### Subscriber（非公開フィールド）

```go
type Subscriber struct {
    // 設定（NewSubscriber で確定、不変）
    spannerClient              *spanner.Client
    streamName                 string
    partitionStorage           PartitionStorage
    consumer                   Consumer
    startTimestamp             time.Time
    endTimestamp               time.Time
    heartbeatInterval          time.Duration
    maxInflight                int
    partitionDiscoveryInterval time.Duration

    // 実行時状態（現在の coordinator のフィールドを統合）
    mu           sync.RWMutex
    readers      map[string]*partitionReader
    ctx          context.Context
    cancel       context.CancelCauseFunc
    readerWg     *asyncWaitGroup

    // 状態フラグ
    started      atomic.Bool  // Subscribe() が呼ばれたか（再利用不可）
    inShutdown atomic.Bool
    closed   atomic.Bool
    err          error
    errOnce      sync.Once
}
```

## NewSubscriber の実装

```go
func NewSubscriber(cfg *Config) (*Subscriber, error) {
    // バリデーション
    if cfg == nil {
        return nil, errors.New("config is required")
    }
    if cfg.SpannerClient == nil {
        return nil, errors.New("SpannerClient is required")
    }
    if cfg.StreamName == "" {
        return nil, errors.New("StreamName is required")
    }
    if cfg.PartitionStorage == nil {
        return nil, errors.New("PartitionStorage is required")
    }
    if cfg.Consumer == nil {
        return nil, errors.New("Consumer is required")
    }

    // デフォルト値適用
    startTimestamp := cfg.StartTimestamp
    if startTimestamp.IsZero() {
        startTimestamp = time.Now()
    }
    endTimestamp := cfg.EndTimestamp
    if endTimestamp.IsZero() {
        endTimestamp = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
    }
    heartbeatInterval := cfg.HeartbeatInterval
    if heartbeatInterval == 0 {
        heartbeatInterval = 10 * time.Second
    }
    maxInflight := cfg.MaxInflight
    if maxInflight == 0 {
        maxInflight = 1
    }
    partitionDiscoveryInterval := cfg.PartitionDiscoveryInterval
    if partitionDiscoveryInterval == 0 {
        partitionDiscoveryInterval = time.Second
    }
    baseContext := cfg.BaseContext
    if baseContext == nil {
        baseContext = context.Background()
    }

    ctx, cancel := context.WithCancelCause(baseContext)
    return &Subscriber{
        // 設定（不変）
        spannerClient:              cfg.SpannerClient,
        streamName:                 cfg.StreamName,
        partitionStorage:           cfg.PartitionStorage,
        consumer:                   cfg.Consumer,
        startTimestamp:             startTimestamp,
        endTimestamp:               endTimestamp,
        heartbeatInterval:          heartbeatInterval,
        maxInflight:                maxInflight,
        partitionDiscoveryInterval: partitionDiscoveryInterval,
        // 実行時状態（data race 回避のため NewSubscriber で初期化）
        readers:  make(map[string]*partitionReader),
        ctx:      ctx,
        cancel:   cancel,
        readerWg: newAsyncWaitGroup(),
    }, nil
}
```

## Subscribe の実装

現在の `coordinator.run()` のロジックを `Subscriber.Subscribe()` に統合する。

```go
func (s *Subscriber) Subscribe() error {
    // 二重起動チェック・再利用チェックは別セクション参照

    defer s.cancel(nil)

    // 以下、現在の coordinator.run() と同じロジック
    // 1. initialize()
    // 2. resumeInterruptedPartitions()
    // 3. runMainLoop()
    // 4. exitError() で戻り値を決定
}
```

**注意**: 実行時状態（`readers`, `ctx`, `cancel`, `readerWg`）は `NewSubscriber` で初期化済み。Subscribe() 内で初期化すると data race が発生する可能性がある。

### exitError の戻り値優先順位

現在の coordinator と同じロジックを維持:
1. `closed` が true → `ErrClosed`
2. `err` が非 nil → その error
3. `inShutdown` が true → `ErrShutdown`
4. それ以外 → nil（正常終了）

## 削除されるもの

- `coordinator.go` ファイル全体
  - `coordinator` 構造体
  - `newCoordinator()` 関数
  - `run()`, `runMainLoop()`, `exitError()` など（Subscriber に統合）
- `option.go` の大部分
  - `Option` interface
  - `withStartTimestamp`, `withEndTimestamp`, `withHeartbeatInterval`, `withMaxInflight`, `withPartitionDiscoveryInterval` 型
  - `WithStartTimestamp()`, `WithEndTimestamp()`, `WithHeartbeatInterval()`, `WithMaxInflight()`, `WithPartitionDiscoveryInterval()` 関数
  - `newConfig()` 関数
  - デフォルト値定数（Config 側に移動）
- `SubscribeFunc()` メソッド（ConsumerFunc を直接渡せば良い）
- `coordinator_test.go`（subscriber_test.go に統合またはリファクタ）

## 移行の影響

### 破壊的変更

1. `NewSubscriber` のシグネチャ変更
2. `Subscribe(consumer)` → `Subscribe()`
3. `SubscribeFunc()` 削除
4. Functional Options 削除

### 利用者の移行例

```go
// Before
sub := spream.NewSubscriber(client, "Stream", storage,
    spream.WithMaxInflight(10),
)
if err := sub.Subscribe(consumer); err != nil {
    // ...
}

// After
sub, err := spream.NewSubscriber(&spream.Config{
    SpannerClient:    client,
    StreamName:       "Stream",
    PartitionStorage: storage,
    Consumer:         consumer,
    MaxInflight:      10,
})
if err != nil {
    // ...
}
if err := sub.Subscribe(); err != nil {
    // ...
}
```

## partition_reader の変更

### ✅ 実装済み

`partitionReader` は既に `*config` への依存を解消済み。`maxInflight` を直接受け取る形になっている。

```go
// 現在の実装（変更不要）
type partitionReader struct {
    partitionToken  string
    startWatermark  time.Time
    endTimestamp    time.Time
    heartbeatMillis int64
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    tracker *inflightTracker
}

func newPartitionReader(
    partition *PartitionMetadata,
    spannerClient *spanner.Client,
    streamName string,
    partitionStorage PartitionStorage,
    consumer Consumer,
    maxInflight int,  // config ではなく個別の値を受け取る
) *partitionReader
```

統合後も `Subscriber.startPartitionReader()` から同じシグネチャで呼び出せばよい。

## 再利用不可の設計

http.Server パターンに倣い、**Subscribe() は一度しか呼べない**設計とする。正常終了・Shutdown・Close いずれの場合も再利用不可。

```go
type Subscriber struct {
    // ...
    started      atomic.Bool  // Subscribe() が呼ばれたかどうか
    inShutdown atomic.Bool
    closed   atomic.Bool
}

func (s *Subscriber) Subscribe() error {
    // 一度しか呼べない
    if s.started.Swap(true) {
        return errors.New("subscriber already started")
    }

    // ...
}
```

**理由**:
- 実行時状態（`ctx`, `cancel`, `readerWg` など）は NewSubscriber で初期化済み
- Subscribe() 終了後にこれらをリセットすると data race の原因になる
- 再度 Subscribe したい場合は新しい Subscriber を作成する

## 実装順序

1. **option.go → config.go**: Config 構造体を定義、Functional Options は削除
2. **subscriber.go**: coordinator のロジックを統合、NewSubscriber の新シグネチャ
3. **coordinator.go**: 削除
4. ~~**partition_reader.go**: config 参照を個別フィールドに変更~~ → **✅ 実装済み**
5. **テストファイル更新**:
   - `coordinator_test.go` → `subscriber_test.go` に統合（または削除してリファクタ）
   - `subscriber_example_test.go`
   - `subscriber_example_with_options_test.go`（削除またはリネーム）
   - `integration_test.go`
6. **cmd/spream/main.go**: 新 API に移行

## 更新が必要なファイル一覧

| ファイル | 変更内容 |
|---------|---------|
| `option.go` → `config.go` | Config 構造体定義、Functional Options 削除 |
| `subscriber.go` | coordinator 統合、API 変更 |
| `coordinator.go` | **削除** |
| `coordinator_test.go` | **削除**（subscriber_test.go に統合） |
| `partition_reader.go` | ✅ 変更不要（既に config 依存解消済み） |
| `subscriber_example_test.go` | 新 API に移行 |
| `subscriber_example_with_options_test.go` | 新 API に移行（またはファイル削除して統合） |
| `integration_test.go` | 新 API に移行 |
| `cmd/spream/main.go` | 新 API に移行 |

## 移行後の Example

### subscriber_example_test.go

```go
func ExampleNewSubscriber() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
    defer stop()

    database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
    spannerClient, err := spanner.NewClient(ctx, database)
    if err != nil {
        panic(err)
    }
    defer spannerClient.Close()

    var mu sync.Mutex
    subscriber, err := spream.NewSubscriber(&spream.Config{
        SpannerClient:    spannerClient,
        StreamName:       "FooStream",
        PartitionStorage: partitionstorage.NewInmemory(),
        Consumer: spream.ConsumerFunc(func(_ context.Context, change *spream.DataChangeRecord) error {
            mu.Lock()
            defer mu.Unlock()
            return json.NewEncoder(os.Stdout).Encode(change)
        }),
    })
    if err != nil {
        panic(err)
    }

    fmt.Fprintf(os.Stderr, "Reading the stream...\n")

    done := make(chan error)
    go func() {
        done <- subscriber.Subscribe()
    }()

    <-ctx.Done()
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := subscriber.Shutdown(shutdownCtx); err != nil {
        subscriber.Close()
    }

    if err := <-done; err != nil && !errors.Is(err, spream.ErrShutdown) {
        panic(err)
    }
}
```

### subscriber_example_with_options_test.go（統合後）

`WithOptions` サフィックスのファイルは不要になり、`ExampleNewSubscriber_withOptions` のように Example 名で区別する。

```go
func ExampleNewSubscriber_withOptions() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
    defer stop()

    database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
    spannerClient, err := spanner.NewClient(ctx, database)
    if err != nil {
        panic(err)
    }
    defer spannerClient.Close()

    partitionMetadataTableName := "PartitionMetadata_FooStream"
    partitionStorage := partitionstorage.NewSpanner(spannerClient, partitionMetadataTableName)
    if err := partitionStorage.CreateTableIfNotExists(ctx); err != nil {
        panic(err)
    }

    subscriber, err := spream.NewSubscriber(&spream.Config{
        SpannerClient:     spannerClient,
        StreamName:        "FooStream",
        PartitionStorage:  partitionStorage,
        Consumer:          &Logger{out: os.Stdout},
        StartTimestamp:    time.Now().Add(-time.Hour),
        EndTimestamp:      time.Now().Add(5 * time.Minute),
        HeartbeatInterval: 3 * time.Second,
    })
    if err != nil {
        panic(err)
    }

    done := make(chan error)
    go func() {
        done <- subscriber.Subscribe()
    }()

    <-ctx.Done()
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := subscriber.Shutdown(shutdownCtx); err != nil {
        subscriber.Close()
    }

    if err := <-done; err != nil && !errors.Is(err, spream.ErrShutdown) {
        panic(err)
    }
}
```

## 検証方法

1. `go build ./...` - コンパイルエラーがないこと
2. `go test ./...` - 既存テストが通ること
3. `go run ./cmd/spream -d "projects/..." -s "StreamName"` - CLI 動作確認
4. Example テストが godoc で正しく表示されること
