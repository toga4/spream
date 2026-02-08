package spream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

// partitionReader reads a single partition of the change stream.
type partitionReader struct {
	// Partition information (extracted from PartitionMetadata at creation time).
	partitionToken  string
	startTimestamp  time.Time // Query start position (initial value).
	endTimestamp    time.Time
	heartbeatMillis int64

	// Dependencies.
	spannerClient    *spanner.Client
	streamName       string
	partitionStorage PartitionStorage
	consumer Consumer
	tracker  *inflightTracker
}

func newPartitionReader(
	partition *PartitionMetadata,
	spannerClient *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	consumer Consumer,
	maxInflight int,
) *partitionReader {
	return &partitionReader{
		partitionToken:   partition.PartitionToken,
		startTimestamp:   partition.Watermark,
		endTimestamp:     partition.EndTimestamp,
		heartbeatMillis:  partition.HeartbeatMillis,
		spannerClient:    spannerClient,
		streamName:       streamName,
		partitionStorage: partitionStorage,
		consumer:         consumer,
		tracker:          newInflightTracker(maxInflight),
	}
}

func (r *partitionReader) run(ctx context.Context) error {
	defer r.tracker.close()

	// Mark the partition as actively processing so restarts can detect and resume it.
	if err := r.partitionStorage.UpdateToRunning(ctx, r.partitionToken); err != nil {
		return fmt.Errorf("update to running: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.processWatermarks(gctx)
	})

	g.Go(func() error {
		return r.processErrors(gctx)
	})

	g.Go(func() error {
		if err := r.readStream(gctx); err != nil {
			return err
		}
		// The stream terminated normally upon reaching EndTimestamp.
		// Drain in-flight records, then close the tracker to
		// unblock processWatermarks/processErrors waiting on channels.
		r.tracker.drain()
		r.tracker.close()
		return nil
	})

	if err := g.Wait(); err != nil {
		if errors.Is(context.Cause(ctx), errGracefulShutdown) {
			r.drainInflight()
		}
		return err
	}

	// Normal termination (endTimestamp reached).
	r.drainInflight()

	// UpdateToFinished uses background context since ctx is already done.
	if err := r.partitionStorage.UpdateToFinished(context.Background(), r.partitionToken); err != nil {
		return fmt.Errorf("update to finished: %w", err)
	}

	return nil
}

// drainInflight waits for all in-flight records to complete.
// No timeout is applied here. If Shutdown times out, the caller should call Close.
func (r *partitionReader) drainInflight() {
	r.tracker.drain()
}

// close forcefully closes the partition reader, interrupting any drainInflight.
func (r *partitionReader) close() {
	r.tracker.close()
}

// processWatermarks persists watermark advances to PartitionStorage so restarts can resume from the last committed position.
func (r *partitionReader) processWatermarks(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watermark, ok := <-r.tracker.watermarks:
			if !ok {
				return nil
			}
			if err := r.partitionStorage.UpdateWatermark(ctx, r.partitionToken, watermark); err != nil {
				return fmt.Errorf("update watermark: %w", err)
			}
		}
	}
}

// processErrors propagates Consumer errors to stop the partition reader, since the subscription cannot continue safely after a Consumer failure.
func (r *partitionReader) processErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-r.tracker.errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}

func (r *partitionReader) readStream(ctx context.Context) error {
	return r.spannerClient.Single().Query(ctx, r.buildStatement()).Do(func(row *spanner.Row) error {
		var records []*changeRecord
		if err := row.Columns(&records); err != nil {
			return err
		}
		return r.processRecords(ctx, records)
	})
}

func (r *partitionReader) buildStatement() spanner.Statement {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)", r.streamName),
		Params: map[string]any{
			"startTimestamp":        r.startTimestamp,
			"endTimestamp":          r.endTimestamp,
			"partitionToken":        r.partitionToken,
			"heartbeatMilliseconds": r.heartbeatMillis,
		},
	}

	if r.partitionToken == RootPartitionToken {
		stmt.Params["partitionToken"] = nil // Root partition requires NULL.
	}

	return stmt
}

func (r *partitionReader) processRecords(ctx context.Context, records []*changeRecord) error {
	for _, cr := range records {
		for _, record := range cr.DataChangeRecords {
			if err := r.processDataChangeRecord(ctx, record); err != nil {
				return err
			}
		}

		for _, record := range cr.HeartbeatRecords {
			r.processHeartbeatRecord(record)
		}

		for _, record := range cr.ChildPartitionsRecords {
			if err := r.processChildPartitionsRecord(ctx, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *partitionReader) processDataChangeRecord(ctx context.Context, record *dataChangeRecord) error {
	// Step 1: Block until an in-flight slot is available to bound concurrent processing.
	if err := r.tracker.acquire(ctx); err != nil {
		return err
	}

	// Step 2: Decode and register before starting the goroutine to preserve read-order sequencing.
	decoded := record.decodeToNonSpannerType()
	seq := r.tracker.add(record.CommitTimestamp)

	// Step 3: Run Consumer concurrently so the stream read loop can continue without blocking.
	go func() {
		err := r.consumer.Consume(ctx, decoded)
		r.tracker.complete(seq, err)
	}()

	return nil
}

func (r *partitionReader) processHeartbeatRecord(record *HeartbeatRecord) {
	// HeartbeatRecord carries no data, so it acks immediately without Consumer or semaphore involvement.
	// This advances the watermark even during idle periods with no data changes.
	r.tracker.ackImmediate(record.Timestamp)
}

func (r *partitionReader) processChildPartitionsRecord(ctx context.Context, record *ChildPartitionsRecord) error {
	// ChildPartitionsRecord also doesn't spawn goroutine, so no semaphore needed.
	// Important: Process in order of persist -> ack.
	// If ack comes first and persist fails, recovery is impossible.

	// 1. Persist child partitions.
	if err := r.partitionStorage.AddChildPartitions(
		ctx,
		r.endTimestamp,
		r.heartbeatMillis,
		record,
	); err != nil {
		return fmt.Errorf("add child partitions: %w", err)
	}

	// 2. Ack.
	r.tracker.ackImmediate(record.StartTimestamp)

	return nil
}
