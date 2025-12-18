package spream

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
)

// Subscriber subscribes to a change stream.
type Subscriber struct {
	spannerClient    *spanner.Client
	streamName       string
	partitionStorage PartitionStorage
	config           *config
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
// Blocks until ctx is canceled or end timestamp is reached.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	c := newCoordinator(
		s.spannerClient,
		s.streamName,
		s.partitionStorage,
		consumer,
		s.config,
	)
	return c.run(ctx)
}

// SubscribeFunc starts subscribing with a function as Consumer.
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error {
	return s.Subscribe(ctx, f)
}
