package spream

import (
	"context"
	"time"
)

// ErrorHandler handles errors during partition processing.
type ErrorHandler interface {
	// HandleError is called when an error occurs.
	//
	// Return values:
	//   - shouldContinue: true to continue processing other records, false to stop entirely
	//   - retryDelay: if > 0, retry this record after the specified duration
	//                 if 0 and shouldContinue=true, skip this record (dangerous)
	HandleError(ctx context.Context, partition *PartitionMetadata, record *DataChangeRecord, err error) (shouldContinue bool, retryDelay time.Duration)
}

// ErrorHandlerFunc is an adapter to allow the use of ordinary functions as ErrorHandler.
type ErrorHandlerFunc func(context.Context, *PartitionMetadata, *DataChangeRecord, error) (bool, time.Duration)

// HandleError calls f(ctx, partition, record, err).
func (f ErrorHandlerFunc) HandleError(ctx context.Context, partition *PartitionMetadata, record *DataChangeRecord, err error) (bool, time.Duration) {
	return f(ctx, partition, record, err)
}
