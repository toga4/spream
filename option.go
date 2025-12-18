package spream

import (
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

// config holds the configuration for the Subscriber.
type config struct {
	startTimestamp             time.Time
	endTimestamp               time.Time
	heartbeatInterval          time.Duration
	spannerRequestPriority     spannerpb.RequestOptions_Priority
	maxInflight                int
	partitionDiscoveryInterval time.Duration
	errorHandler               ErrorHandler
}

// Option is an interface for configuring the Subscriber.
type Option interface {
	Apply(*config)
}

type withStartTimestamp time.Time

func (o withStartTimestamp) Apply(c *config) {
	c.startTimestamp = time.Time(o)
}

// WithStartTimestamp sets the start timestamp option for reading change streams.
//
// The value must be within the retention period of the change stream and before the current time.
// Default value is current timestamp.
func WithStartTimestamp(startTimestamp time.Time) Option {
	return withStartTimestamp(startTimestamp)
}

type withEndTimestamp time.Time

func (o withEndTimestamp) Apply(c *config) {
	c.endTimestamp = time.Time(o)
}

// WithEndTimestamp sets the end timestamp option for reading change streams.
//
// The value must be within the retention period of the change stream and must be after the start timestamp.
// If not set, reads latest changes until canceled.
func WithEndTimestamp(endTimestamp time.Time) Option {
	return withEndTimestamp(endTimestamp)
}

type withHeartbeatInterval time.Duration

func (o withHeartbeatInterval) Apply(c *config) {
	c.heartbeatInterval = time.Duration(o)
}

// WithHeartbeatInterval sets the heartbeat interval for reading change streams.
//
// Default value is 10 seconds.
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option {
	return withHeartbeatInterval(heartbeatInterval)
}

type withSpannerRequestPriority spannerpb.RequestOptions_Priority

func (o withSpannerRequestPriority) Apply(c *config) {
	c.spannerRequestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithSpannerRequestPriority sets the request priority option for reading change streams.
//
// Default value is unspecified, equivalent to high.
func WithSpannerRequestPriority(priority spannerpb.RequestOptions_Priority) Option {
	return withSpannerRequestPriority(priority)
}

type withMaxInflight int

func (o withMaxInflight) Apply(c *config) {
	c.maxInflight = int(o)
}

// WithMaxInflight sets the maximum number of concurrent record processing.
//
// Default value is 1 (sequential processing for backward compatibility).
func WithMaxInflight(n int) Option {
	return withMaxInflight(n)
}

type withPartitionDiscoveryInterval time.Duration

func (o withPartitionDiscoveryInterval) Apply(c *config) {
	c.partitionDiscoveryInterval = time.Duration(o)
}

// WithPartitionDiscoveryInterval sets the partition discovery interval.
//
// Default value is 1 second.
func WithPartitionDiscoveryInterval(d time.Duration) Option {
	return withPartitionDiscoveryInterval(d)
}

type withErrorHandler struct {
	handler ErrorHandler
}

func (o withErrorHandler) Apply(c *config) {
	c.errorHandler = o.handler
}

// WithErrorHandler sets the error handler for processing errors.
//
// If not set, any error from Consumer will stop the entire subscription.
func WithErrorHandler(handler ErrorHandler) Option {
	return withErrorHandler{handler: handler}
}

var (
	defaultEndTimestamp              = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC) // Maximum value of Spanner TIMESTAMP type.
	defaultHeartbeatInterval         = 10 * time.Second
	defaultMaxInflight               = 1
	defaultPartitionDiscoveryInterval = 1 * time.Second
)

func newConfig(options ...Option) *config {
	c := &config{
		startTimestamp:             nowFunc(),
		endTimestamp:               defaultEndTimestamp,
		heartbeatInterval:          defaultHeartbeatInterval,
		maxInflight:                defaultMaxInflight,
		partitionDiscoveryInterval: defaultPartitionDiscoveryInterval,
	}
	for _, o := range options {
		o.Apply(c)
	}
	return c
}

var nowFunc = time.Now
