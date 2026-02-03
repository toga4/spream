package spream

import (
	"context"
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
