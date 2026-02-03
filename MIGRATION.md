# Migration Guide

## Migrating from v0.2.x to v0.3.0

v0.3.0 introduces significant API changes for improved usability and reliability. This guide covers all breaking changes and how to update your code.

### NewSubscriber API

**Before (v0.2.x):**
```go
subscriber := spream.NewSubscriber(
    spannerClient,
    "StreamName",
    partitionStorage,
    spream.WithStartTimestamp(startTime),
    spream.WithEndTimestamp(endTime),
    spream.WithHeartbeatInterval(10*time.Second),
)
```

**After (v0.3.0):**
```go
subscriber, err := spream.NewSubscriber(&spream.Config{
    SpannerClient:     spannerClient,
    StreamName:        "StreamName",
    PartitionStorage:  partitionStorage,
    Consumer:          consumer, // Now required in Config
    StartTimestamp:    startTime,
    EndTimestamp:      endTime,
    HeartbeatInterval: 10 * time.Second,
})
if err != nil {
    // Handle validation error
}
```

Key changes:
- Uses `Config` struct instead of functional options
- Returns `(*Subscriber, error)` instead of `*Subscriber`
- `Consumer` is now passed in `Config`, not in `Subscribe()`

### Consumer Interface

**Before (v0.2.x):**
```go
type Consumer interface {
    Consume(change *DataChangeRecord) error
}

// Using ConsumerFunc
consumer := spream.ConsumerFunc(func(change *spream.DataChangeRecord) error {
    return processChange(change)
})
```

**After (v0.3.0):**
```go
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

// Using ConsumerFunc
consumer := spream.ConsumerFunc(func(ctx context.Context, change *spream.DataChangeRecord) error {
    return processChange(ctx, change)
})
```

Key changes:
- `context.Context` is now the first parameter
- Use the context for cancellation handling and passing request-scoped values

### Subscribe and Shutdown

**Before (v0.2.x):**
```go
ctx, cancel := context.WithCancel(context.Background())

// Start subscribing
go func() {
    if err := subscriber.Subscribe(ctx, consumer); err != nil {
        log.Printf("Subscribe error: %v", err)
    }
}()

// To stop: cancel the context
cancel()
```

**After (v0.3.0):**
```go
// Start subscribing (no context parameter)
done := make(chan error)
go func() {
    done <- subscriber.Subscribe()
}()

// Graceful shutdown with timeout
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := subscriber.Shutdown(shutdownCtx); err != nil {
    // Shutdown timed out, force close
    subscriber.Close()
}

// Check Subscribe result
if err := <-done; err != nil && !errors.Is(err, spream.ErrShutdown) {
    log.Printf("Subscribe error: %v", err)
}
```

Key changes:
- `Subscribe()` no longer takes `context` or `consumer` parameters
- Use `Shutdown(ctx)` for graceful shutdown with timeout
- Use `Close()` for immediate termination
- Check for `spream.ErrShutdown` (normal shutdown) vs other errors

### New Configuration Options

v0.3.0 adds new configuration options in `Config`:

```go
subscriber, err := spream.NewSubscriber(&spream.Config{
    // ... required fields ...

    // New options:
    MaxInflight:                5,              // Concurrent record processing per partition (default: 1)
    PartitionDiscoveryInterval: 2*time.Second,  // Interval for discovering new partitions (default: 1s)
    BaseContext:                parentCtx,      // Parent context for tracing/cancellation propagation
})
```

### Spanner Request Priority

The `WithSpannerRequestPriotiry` option for change stream reads has been removed. Instead, configure the priority at the `spanner.Client` level using `NewClientWithConfig`.

**Before (v0.2.x):**
```go
client, _ := spanner.NewClient(ctx, database)
subscriber := spream.NewSubscriber(
    client, "StreamName", storage,
    spream.WithSpannerRequestPriotiry(spannerpb.RequestOptions_PRIORITY_LOW),
)
```

**After (v0.3.0):**
```go
client, _ := spanner.NewClientWithConfig(ctx, database, spanner.ClientConfig{
    QueryOptions: spanner.QueryOptions{
        Priority: spannerpb.RequestOptions_PRIORITY_LOW,
    },
})
subscriber, _ := spream.NewSubscriber(&spream.Config{
    SpannerClient:    client,
    StreamName:       "StreamName",
    PartitionStorage: storage,
    Consumer:         consumer,
})
```

This approach applies the priority to all queries made by the client, which is generally the desired behavior.

### SpannerPartitionStorage Options

**Before (v0.2.x):**
```go
storage := partitionstorage.NewSpanner(
    client,
    "TableName",
    partitionstorage.WithRequestPriotiry(spannerpb.RequestOptions_PRIORITY_LOW), // Note: typo
)
```

**After (v0.3.0):**
```go
storage := partitionstorage.NewSpanner(
    client,
    "TableName",
    partitionstorage.WithRequestPriority(spannerpb.RequestOptions_PRIORITY_LOW), // Typo fixed
)
```

### PartitionStorage Interface (Custom Implementations Only)

If you have a custom `PartitionStorage` implementation, the interface has changed:

**Before (v0.2.x):**
```go
type PartitionStorage interface {
    AddChildPartitions(ctx context.Context, parent *PartitionMetadata, record *ChildPartitionsRecord) error
    UpdateToScheduled(ctx context.Context, partitions []*PartitionMetadata) error
    UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error
    UpdateToFinished(ctx context.Context, partition *PartitionMetadata) error
    UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error
    // ... other methods unchanged
}
```

**After (v0.3.0):**
```go
type PartitionStorage interface {
    AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, record *ChildPartitionsRecord) error
    UpdateToScheduled(ctx context.Context, partitionTokens []string) error
    UpdateToRunning(ctx context.Context, partitionToken string) error
    UpdateToFinished(ctx context.Context, partitionToken string) error
    UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error
    // ... other methods unchanged
}
```

Key changes:
- Methods now take primitive values (`string`, `[]string`) instead of `*PartitionMetadata`
- `AddChildPartitions` takes `endTimestamp` and `heartbeatMillis` directly instead of parent metadata

### Complete Migration Example

**Before (v0.2.x):**
```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
    defer cancel()

    client, _ := spanner.NewClient(ctx, database)
    storage := partitionstorage.NewSpanner(client, "PartitionMetadata")

    subscriber := spream.NewSubscriber(client, "MyStream", storage)

    consumer := spream.ConsumerFunc(func(change *spream.DataChangeRecord) error {
        return json.NewEncoder(os.Stdout).Encode(change)
    })

    if err := subscriber.Subscribe(ctx, consumer); err != nil && ctx.Err() == nil {
        log.Fatal(err)
    }
}
```

**After (v0.3.0):**
```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    client, _ := spanner.NewClient(ctx, database)
    storage := partitionstorage.NewSpanner(client, "PartitionMetadata")

    subscriber, err := spream.NewSubscriber(&spream.Config{
        SpannerClient:    client,
        StreamName:       "MyStream",
        PartitionStorage: storage,
        Consumer: spream.ConsumerFunc(func(ctx context.Context, change *spream.DataChangeRecord) error {
            return json.NewEncoder(os.Stdout).Encode(change)
        }),
    })
    if err != nil {
        log.Fatal(err)
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
        log.Fatal(err)
    }
}
```
