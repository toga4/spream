package spream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
)

// Config holds the configuration for creating a Subscriber.
// Required fields must be set; optional fields use sensible defaults when zero.
type Config struct {
	// Required fields.
	SpannerClient    *spanner.Client
	StreamName       string
	PartitionStorage PartitionStorage
	Consumer         Consumer

	// Optional fields (zero values use defaults).

	// BaseContext is the parent context for the Subscriber.
	// It enables external cancellation propagation, tracing context inheritance,
	// and deadline setting.
	// Default: context.Background()
	BaseContext context.Context

	// StartTimestamp sets the start timestamp for reading change streams.
	// The value must be within the retention period of the change stream
	// and before the current time.
	// Default: time.Now()
	StartTimestamp time.Time

	// EndTimestamp sets the end timestamp for reading change streams.
	// The value must be within the retention period of the change stream
	// and must be after the start timestamp.
	// Default: 9999-12-31T23:59:59.999999999Z (maximum Spanner TIMESTAMP)
	EndTimestamp time.Time

	// HeartbeatInterval sets the heartbeat interval for reading change streams.
	// Default: 10 seconds
	HeartbeatInterval time.Duration

	// MaxInflight sets the maximum number of concurrent record processing
	// per partition.
	// Default: 1 (sequential processing)
	MaxInflight int

	// PartitionDiscoveryInterval sets the interval for discovering new partitions.
	// Default: 1 second
	PartitionDiscoveryInterval time.Duration
}

// Default values for configuration.
var (
	defaultEndTimestamp               = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC) // Maximum value of Spanner TIMESTAMP type.
	defaultHeartbeatInterval          = 10 * time.Second
	defaultMaxInflight                = 1
	defaultPartitionDiscoveryInterval = 1 * time.Second
)

// nowFunc is a variable for testing purposes.
var nowFunc = time.Now

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume is called from multiple goroutines concurrently.
// Implementations must be thread-safe.
//
// Return values:
//   - nil: Processing succeeded. spream will ack this record and advance the watermark.
//   - error: Processing failed. spream will stop subscription.
//
// When ctx is canceled, Consume should return ctx.Err() promptly.
type Consumer interface {
	Consume(ctx context.Context, change *DataChangeRecord) error
}

// ConsumerFunc is an adapter to allow the use of ordinary functions as Consumer.
type ConsumerFunc func(context.Context, *DataChangeRecord) error

// Consume calls f(ctx, change).
func (f ConsumerFunc) Consume(ctx context.Context, change *DataChangeRecord) error {
	return f(ctx, change)
}

// Subscriber subscribes to a change stream.
// It manages partition readers and coordinates change stream subscription.
//
// Subscribe can only be called once. If you need to subscribe again,
// create a new Subscriber instance.
type Subscriber struct {
	// Configuration (immutable after NewSubscriber).
	spannerClient              *spanner.Client
	streamName                 string
	partitionStorage           PartitionStorage
	consumer                   Consumer
	startTimestamp             time.Time
	endTimestamp               time.Time
	heartbeatInterval          time.Duration
	maxInflight                int
	partitionDiscoveryInterval time.Duration

	// Partition management.
	readersMu sync.RWMutex
	readers   map[string]*partitionReader

	// Control.
	ctx      context.Context
	cancel   context.CancelCauseFunc
	wg       sync.WaitGroup
	done     chan struct{}
	waitOnce sync.Once

	// State flags.
	started      atomic.Bool // Subscribe() has been called (not reusable).
	shutdownFlag atomic.Bool
	closedFlag   atomic.Bool

	// Error handling.
	// err records the first error from fail() and is returned by Subscribe()
	// unless Close() was called (which returns ErrClosed).
	err     error
	errOnce sync.Once
}

// PartitionStorage is an interface for storing and reading PartitionMetadata.
type PartitionStorage interface {
	GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error)
	GetInterruptedPartitions(ctx context.Context) ([]*PartitionMetadata, error)
	InitializeRootPartition(ctx context.Context, startTimestamp, endTimestamp time.Time, heartbeatInterval time.Duration) error
	GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error)
	AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, childPartitionsRecord *ChildPartitionsRecord) error
	UpdateToScheduled(ctx context.Context, partitionTokens []string) error
	UpdateToRunning(ctx context.Context, partitionToken string) error
	UpdateToFinished(ctx context.Context, partitionToken string) error
	UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error
}

// NewSubscriber creates a new subscriber of change streams.
// It validates the configuration and applies default values for optional fields.
func NewSubscriber(cfg *Config) (*Subscriber, error) {
	// Validation.
	if cfg == nil {
		return nil, errors.New("spream: config is required")
	}
	if cfg.SpannerClient == nil {
		return nil, errors.New("spream: SpannerClient is required")
	}
	if cfg.StreamName == "" {
		return nil, errors.New("spream: StreamName is required")
	}
	if cfg.PartitionStorage == nil {
		return nil, errors.New("spream: PartitionStorage is required")
	}
	if cfg.Consumer == nil {
		return nil, errors.New("spream: Consumer is required")
	}

	// Apply default values.
	startTimestamp := cfg.StartTimestamp
	if startTimestamp.IsZero() {
		startTimestamp = nowFunc()
	}
	endTimestamp := cfg.EndTimestamp
	if endTimestamp.IsZero() {
		endTimestamp = defaultEndTimestamp
	}
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	maxInflight := cfg.MaxInflight
	if maxInflight == 0 {
		maxInflight = defaultMaxInflight
	}
	partitionDiscoveryInterval := cfg.PartitionDiscoveryInterval
	if partitionDiscoveryInterval == 0 {
		partitionDiscoveryInterval = defaultPartitionDiscoveryInterval
	}
	baseContext := cfg.BaseContext
	if baseContext == nil {
		baseContext = context.Background()
	}

	ctx, cancel := context.WithCancelCause(baseContext)
	return &Subscriber{
		// Configuration (immutable).
		spannerClient:              cfg.SpannerClient,
		streamName:                 cfg.StreamName,
		partitionStorage:           cfg.PartitionStorage,
		consumer:                   cfg.Consumer,
		startTimestamp:             startTimestamp,
		endTimestamp:               endTimestamp,
		heartbeatInterval:          heartbeatInterval,
		maxInflight:                maxInflight,
		partitionDiscoveryInterval: partitionDiscoveryInterval,
		// Runtime state (initialized here to avoid data races).
		readers: make(map[string]*partitionReader),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}, nil
}

// Subscribe starts subscribing to the change stream.
// It blocks until one of the following occurs:
//   - All partitions are processed to endTimestamp (returns nil)
//   - Shutdown is called (returns ErrShutdown immediately)
//   - Close is called (returns ErrClosed)
//   - An error occurs (returns the error)
//
// Subscribe can only be called once. Subsequent calls return an error.
func (s *Subscriber) Subscribe() error {
	// Prevent multiple calls.
	if s.started.Swap(true) {
		return errors.New("spream: subscriber already started")
	}

	defer s.cancel(nil)

	// 1. Initialize.
	if err := s.initialize(); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	// 2. Resume interrupted partitions.
	if err := s.resumeInterruptedPartitions(); err != nil {
		return fmt.Errorf("resume interrupted partitions: %w", err)
	}

	// 3. Main loop: partition detection and shutdown handling.
	s.runMainLoop()

	return s.exitError()
}

// Shutdown gracefully shuts down the subscriber.
// It causes Subscribe to return ErrShutdown immediately, then waits for
// in-flight records to complete (drain).
//
// If the context is canceled or times out before drain completes,
// Shutdown returns ctx.Err(). The drain continues in the background;
// call Close to abort it.
func (s *Subscriber) Shutdown(ctx context.Context) error {
	if !s.shutdownFlag.Swap(true) {
		s.cancel(errGracefulShutdown)
	}

	select {
	case <-s.drain():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close immediately closes the subscriber.
// It does not wait for in-flight records to complete.
// Subscribe returns ErrClosed.
func (s *Subscriber) Close() error {
	if !s.closedFlag.Swap(true) {
		s.cancel(ErrClosed)

		// Force close all readers to break out of drainInflight.
		s.readersMu.RLock()
		for _, reader := range s.readers {
			reader.Close()
		}
		s.readersMu.RUnlock()
	}

	<-s.drain()
	return nil
}

// runMainLoop runs the main partition detection loop.
// It returns when shutdown is requested, context is canceled,
// all readers finish, or an error occurs.
func (s *Subscriber) runMainLoop() {
	ticker := time.NewTicker(s.partitionDiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return

		case <-s.done:
			return

		case <-ticker.C:
			if err := s.detectAndSchedulePartitions(); err != nil {
				if errors.Is(err, errAllPartitionsFinished) {
					ticker.Stop()
					// Start drain but don't wait; completion is detected via case <-s.done.
					s.drain()
					continue
				}
				s.fail(err)
				return
			}
		}
	}
}

// exitError determines the return value based on shutdown/close state and errors.
// Priority: Close > Error > Shutdown > nil (normal completion).
// Error takes precedence over Shutdown because if an error occurs during
// graceful shutdown, that error should be reported instead of ErrShutdown.
func (s *Subscriber) exitError() error {
	if s.closedFlag.Load() {
		return ErrClosed
	}
	if s.err != nil {
		return s.err
	}
	if s.shutdownFlag.Load() {
		return ErrShutdown
	}
	return nil
}

func (s *Subscriber) initialize() error {
	// Initialize root partition if this is the first run or if the previous run has already been completed.
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(s.ctx)
	if err != nil {
		return fmt.Errorf("get unfinished min watermark partition: %w", err)
	}
	if minWatermarkPartition == nil {
		if err := s.partitionStorage.InitializeRootPartition(
			s.ctx,
			s.startTimestamp,
			s.endTimestamp,
			s.heartbeatInterval,
		); err != nil {
			return fmt.Errorf("failed to initialize root partition: %w", err)
		}
	}
	return nil
}

func (s *Subscriber) resumeInterruptedPartitions() error {
	interruptedPartitions, err := s.partitionStorage.GetInterruptedPartitions(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get interrupted partitions: %w", err)
	}
	for _, p := range interruptedPartitions {
		s.startPartitionReader(p)
	}
	return nil
}

func (s *Subscriber) detectAndSchedulePartitions() error {
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition: %w", err)
	}

	if minWatermarkPartition == nil {
		return errAllPartitionsFinished
	}

	// To make sure changes for a key are processed in timestamp order, wait until the records returned from all parents have been processed.
	partitions, err := s.partitionStorage.GetSchedulablePartitions(s.ctx, minWatermarkPartition.Watermark)
	if err != nil {
		return fmt.Errorf("failed to get schedulable partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil
	}

	partitionTokens := make([]string, 0, len(partitions))
	for _, p := range partitions {
		partitionTokens = append(partitionTokens, p.PartitionToken)
	}
	if err := s.partitionStorage.UpdateToScheduled(s.ctx, partitionTokens); err != nil {
		return fmt.Errorf("failed to update to scheduled: %w", err)
	}

	for _, p := range partitions {
		s.startPartitionReader(p)
	}

	return nil
}

func (s *Subscriber) startPartitionReader(partition *PartitionMetadata) {
	s.readersMu.Lock()
	defer s.readersMu.Unlock()

	// Skip if reader already exists.
	if _, exists := s.readers[partition.PartitionToken]; exists {
		return
	}

	reader := newPartitionReader(
		partition,
		s.spannerClient,
		s.streamName,
		s.partitionStorage,
		s.consumer,
		s.maxInflight,
	)
	s.readers[partition.PartitionToken] = reader

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := reader.run(s.ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.fail(err)
			}
		}
		s.readersMu.Lock()
		defer s.readersMu.Unlock()
		delete(s.readers, partition.PartitionToken)
	}()
}

// drain waits for all goroutines to finish and returns a channel that is closed when done.
// Safe to call multiple times; only the first call starts the wait goroutine.
func (s *Subscriber) drain() <-chan struct{} {
	s.waitOnce.Do(func() {
		go func() {
			s.wg.Wait()
			close(s.done)
		}()
	})
	return s.done
}

func (s *Subscriber) fail(err error) {
	s.errOnce.Do(func() {
		s.err = err
		s.cancel(err)
	})
}
