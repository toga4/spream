package spream

import (
	"context"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
)

// Subscriber subscribes to a change stream.
type Subscriber struct {
	spannerClient    *spanner.Client
	streamName       string
	partitionStorage PartitionStorage
	config           *config

	coordinator atomic.Pointer[coordinator]
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
func NewSubscriber(
	client *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	options ...Option,
) *Subscriber {
	return &Subscriber{
		spannerClient:    client,
		streamName:       streamName,
		partitionStorage: partitionStorage,
		config:           newConfig(options...),
	}
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

// SubscribeFunc starts subscribing with a function as Consumer.
func (s *Subscriber) SubscribeFunc(f ConsumerFunc) error {
	return s.Subscribe(f)
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
