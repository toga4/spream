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
	consumer         Consumer
	tracker          *inflightTracker

	// flushCtx controls the lifetime of post-drain watermark flush.
	// close() cancels it to abort flush when Close() is called.
	flushCtx    context.Context
	flushCancel context.CancelFunc
}

func newPartitionReader(
	ctx context.Context,
	partition *PartitionMetadata,
	spannerClient *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	consumer Consumer,
	maxInflight int,
) *partitionReader {
	// flushCtx inherits values (e.g. tracing) from ctx but is detached from its
	// cancellation. It is canceled only by close(), giving Consumer goroutines and
	// watermark flushes a lifetime that survives graceful shutdown but is cut short
	// by Close().
	flushCtx, flushCancel := context.WithCancel(context.WithoutCancel(ctx))
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
		flushCtx:         flushCtx,
		flushCancel:      flushCancel,
	}
}

func (r *partitionReader) run(ctx context.Context) error {
	defer r.flushCancel()
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
			// Flush watermarks concurrently with drain so that completed records'
			// watermarks are persisted immediately, not deferred until all records finish.
			// Without this, a timeout during drain would lose all watermarks.
			flushDone := make(chan struct{})
			go func() {
				defer close(flushDone)
				r.flushWatermarks()
			}()
			r.drainInflight()
			r.tracker.close() // Close watermarks channel to unblock flushWatermarks.
			<-flushDone
		}
		return err
	}

	// Normal termination (endTimestamp reached).
	// readStream already called tracker.drain(), but this ensures all in-flight
	// processing has completed before updating the partition state.
	r.drainInflight()

	// UpdateToFinished uses flushCtx so that Close() can cancel the RPC.
	if err := r.partitionStorage.UpdateToFinished(r.flushCtx, r.partitionToken); err != nil {
		return fmt.Errorf("update to finished: %w", err)
	}

	return nil
}

// drainInflight waits for all in-flight records to complete.
// No timeout is applied here. If Shutdown times out, the caller should call Close.
func (r *partitionReader) drainInflight() {
	r.tracker.drain()
}

// close forcefully closes the partition reader, interrupting any drainInflight and flushWatermarks.
func (r *partitionReader) close() {
	r.flushCancel()
	r.tracker.close()
}

// flushWatermarks reads watermarks from the tracker channel and persists them.
// Runs concurrently with drainInflight during graceful shutdown so that
// watermarks are persisted as each record completes, not deferred until all finish.
// flushCtx controls cancellation: Close() cancels it to abort in-flight Spanner RPCs.
func (r *partitionReader) flushWatermarks() {
	for {
		select {
		case <-r.flushCtx.Done():
			return
		case watermark, ok := <-r.tracker.watermarks:
			if !ok {
				return
			}
			// Best-effort: errors are ignored because at-least-once semantics remain
			// intact. On restart, the subscriber resumes from the last persisted watermark.
			_ = r.partitionStorage.UpdateWatermark(r.flushCtx, r.partitionToken, watermark)
		}
	}
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
			// Use flushCtx instead of ctx (errgroup context) to avoid watermark loss
			// during graceful shutdown. When ctx is canceled, both <-ctx.Done() and
			// the watermark case can be ready simultaneously. Go's select picks randomly,
			// so the watermark case may win — consuming the value from the channel —
			// then the RPC fails immediately on the canceled ctx, and the watermark is
			// irrecoverably lost (flushWatermarks cannot recover it either).
			// flushCtx is only canceled by Close(), so it remains valid during graceful shutdown.
			if err := r.partitionStorage.UpdateWatermark(r.flushCtx, r.partitionToken, watermark); err != nil {
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
	// Use flushCtx so the Consumer is NOT canceled by graceful shutdown.
	// Only Close() cancels flushCtx, giving the Consumer time to finish during drain.
	go func() {
		err := r.consumer.Consume(r.flushCtx, decoded)
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
