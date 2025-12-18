package spream

import "context"

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume is called from multiple goroutines concurrently.
// Implementations must be thread-safe.
//
// Return values:
//   - nil: Processing succeeded. spream will ack this record and advance the watermark.
//   - error: Processing failed. Retry or stop based on ErrorHandler settings.
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
