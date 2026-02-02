# Subscriber と coordinator の統合計画

## 概要

Subscriber 構造体と coordinator 構造体を統合し、API を Config + NewSubscriber パターンに変更する。

## 背景

現状の問題:
- Subscriber は薄いラッパーで、coordinator に処理を委譲しているだけ
- spannerClient, streamName, partitionStorage, config が両方に重複
- atomic.Pointer[coordinator] による間接参照が複雑

## 設計方針

1. **Config 構造体を公開** - 設定を渡すための一時的な構造体
2. **Subscriber のフィールドは全て非公開** - 書き換え不可
3. **NewSubscriber(cfg *Config)** - バリデーションとデフォルト値適用
4. **Subscribe() は引数なし** - consumer は Config で渡す
5. **Shutdown 後は再利用不可** - http.Server パターン

## API 変更

### Before

```go
sub := spream.NewSubscriber(client, stream, storage,
    spream.WithMaxInflight(10),
)
sub.Subscribe(consumer)
sub.SubscribeFunc(func(...) error { ... })
```

### After

```go
sub, err := spream.NewSubscriber(&spream.Config{
    SpannerClient:    client,
    StreamName:       stream,
    PartitionStorage: storage,
    Consumer:         consumer,
    MaxInflight:      10,
})
sub.Subscribe()
```

## 変更対象ファイル

### 削除

- `coordinator.go` - Subscriber に統合

### 大幅変更

- `subscriber.go` - coordinator のロジックを統合
- `option.go` - Config 構造体に置き換え、Functional Options 削除

### 軽微な変更

- `partition_reader.go` - config 参照方法の調整（必要に応じて）

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
    StartTimestamp             time.Time                        // default: time.Now()
    EndTimestamp               time.Time                        // default: 9999-12-31
    HeartbeatInterval          time.Duration                    // default: 10s
    SpannerRequestPriority     spannerpb.RequestOptions_Priority // default: unspecified
    MaxInflight                int                              // default: 1
    PartitionDiscoveryInterval time.Duration                    // default: 1s
    ErrorHandler               ErrorHandler                     // default: nil (エラーで停止)
}
```

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
    spannerRequestPriority     spannerpb.RequestOptions_Priority
    maxInflight                int
    partitionDiscoveryInterval time.Duration
    errorHandler               ErrorHandler

    // 実行時状態
    inShutdown atomic.Bool
    mu         sync.RWMutex
    readers    map[string]*partitionReader
    ctx        context.Context
    cancel     context.CancelCauseFunc
    wg         sync.WaitGroup
    done       chan struct{}
    err        error
    errOnce    sync.Once
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

    return &Subscriber{
        spannerClient:              cfg.SpannerClient,
        streamName:                 cfg.StreamName,
        partitionStorage:           cfg.PartitionStorage,
        consumer:                   cfg.Consumer,
        startTimestamp:             startTimestamp,
        endTimestamp:               endTimestamp,
        heartbeatInterval:          heartbeatInterval,
        spannerRequestPriority:     cfg.SpannerRequestPriority,
        maxInflight:                maxInflight,
        partitionDiscoveryInterval: partitionDiscoveryInterval,
        errorHandler:               cfg.ErrorHandler,
    }, nil
}
```

## Subscribe の実装

```go
func (s *Subscriber) Subscribe() error {
    // 二重起動・再利用チェック
    if s.inShutdown.Load() {
        return ErrSubscriberClosed
    }

    // 実行時状態の初期化
    s.readers = make(map[string]*partitionReader)
    s.done = make(chan struct{})
    s.err = nil
    s.errOnce = sync.Once{}
    s.ctx, s.cancel = context.WithCancelCause(context.Background())

    defer close(s.done)
    defer s.cancel(nil)

    // 以下、現在の coordinator.run() と同じロジック
    // ...
}
```

## 削除されるもの

- `coordinator` 構造体
- `newCoordinator()` 関数
- `Option` interface と各 with* 型
- `SubscribeFunc()` メソッド（ConsumerFunc を直接渡せば良い）
- `newConfig()` 関数

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

現在 `partitionReader` は `*config` を受け取っているが、統合後は個別のフィールドを渡す形に変更する。

### 現在

```go
type partitionReader struct {
    // ...
    config *config
}

func newPartitionReader(..., cfg *config) *partitionReader
```

### 変更後

`Subscriber` から必要なフィールドを直接渡す。`partitionReader` 側では `config` 構造体への依存をなくす。

```go
type partitionReader struct {
    // ...
    maxInflight            int
    spannerRequestPriority spannerpb.RequestOptions_Priority
    errorHandler           ErrorHandler
}

func newPartitionReader(..., maxInflight int, priority spannerpb.RequestOptions_Priority, errorHandler ErrorHandler) *partitionReader
```

または、`Subscriber` への参照を持たせる方法もあるが、循環参照を避けるため個別フィールドを渡す方が良い。

## 二重起動防止

```go
func (s *Subscriber) Subscribe() error {
    // inShutdown が true なら既に Shutdown/Close 済み
    if s.inShutdown.Load() {
        return ErrSubscriberClosed
    }

    // 二重起動チェック（running フラグ）
    s.mu.Lock()
    if s.running {
        s.mu.Unlock()
        return errors.New("subscriber is already running")
    }
    s.running = true
    s.mu.Unlock()

    // ...
}
```

`running` フラグを追加して、Subscribe 中の二重呼び出しを防ぐ。

## 実装順序

1. **option.go → config.go**: Config 構造体を定義、Functional Options は削除
2. **subscriber.go**: coordinator のロジックを統合、NewSubscriber の新シグネチャ
3. **coordinator.go**: 削除
4. **partition_reader.go**: config 参照を個別フィールドに変更
5. **テストファイル更新**:
   - `subscriber_example_test.go`
   - `subscriber_example_with_options_test.go`
   - `integration_test.go`（存在すれば）
6. **cmd/spream/main.go**: 新 API に移行

## 更新が必要なファイル一覧

| ファイル | 変更内容 |
|---------|---------|
| `option.go` → `config.go` | Config 構造体定義、Functional Options 削除 |
| `subscriber.go` | coordinator 統合、API 変更 |
| `coordinator.go` | 削除 |
| `partition_reader.go` | config → 個別フィールド |
| `subscriber_example_test.go` | 新 API に移行 |
| `subscriber_example_with_options_test.go` | 新 API に移行 |
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

    if err := <-done; err != nil && !errors.Is(err, spream.ErrSubscriberClosed) {
        panic(err)
    }
}
```

## 検証方法

1. `go build ./...` - コンパイルエラーがないこと
2. `go test ./...` - 既存テストが通ること
3. `go run ./cmd/spream -d "projects/..." -s "StreamName"` - CLI 動作確認
4. Example テストが godoc で正しく表示されること
