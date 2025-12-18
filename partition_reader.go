package spream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

// partitionEvent represents an event from a partition reader.
type partitionEvent struct {
	eventType       partitionEventType
	partition       *PartitionMetadata
	childPartitions *ChildPartitionsRecord
	err             error
}

type partitionEventType int

const (
	eventChildPartitions partitionEventType = iota
	eventPartitionFinished
	eventPartitionError
)

// partitionReader reads a single partition of the change stream.
type partitionReader struct {
	partition        *PartitionMetadata
	spannerClient    *spanner.Client
	streamName       string
	partitionStorage PartitionStorage
	consumer         Consumer
	config           *config

	tracker *inflightTracker

	// Event notification.
	events chan<- partitionEvent
}

func newPartitionReader(
	partition *PartitionMetadata,
	spannerClient *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	consumer Consumer,
	cfg *config,
	events chan<- partitionEvent,
) *partitionReader {
	return &partitionReader{
		partition:        partition,
		spannerClient:    spannerClient,
		streamName:       streamName,
		partitionStorage: partitionStorage,
		consumer:         consumer,
		config:           cfg,
		tracker:          newInflightTracker(cfg.maxInflight),
		events:           events,
	}
}

func (r *partitionReader) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer r.tracker.close()

	// Update state to RUNNING.
	if err := r.partitionStorage.UpdateToRunning(ctx, r.partition.PartitionToken); err != nil {
		return fmt.Errorf("update to running: %w", err)
	}

	// Manage concurrent processing with errgroup.
	g, gctx := errgroup.WithContext(ctx)

	// Watermark update processing.
	g.Go(func() error {
		return r.processWatermarks(gctx)
	})

	// Error handling processing.
	g.Go(func() error {
		return r.processErrors(gctx)
	})

	// Change Stream reading.
	g.Go(func() error {
		return r.readStream(gctx)
	})

	// Wait for all goroutines to complete.
	if err := g.Wait(); err != nil {
		// Only context.Canceled is treated as normal termination.
		// DeadlineExceeded is treated as error since source cannot be distinguished.
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	// initiateShutdown only on normal termination.
	r.tracker.initiateShutdown()

	// Wait for all in-flight to complete.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer waitCancel()
	if err := r.tracker.waitAllCompleted(waitCtx); err != nil {
		// Continue even on timeout, just log.
	}

	// Update state to FINISHED.
	if err := r.partitionStorage.UpdateToFinished(ctx, r.partition.PartitionToken); err != nil {
		return fmt.Errorf("update to finished: %w", err)
	}

	return nil
}

// processWatermarks processes watermark updates.
func (r *partitionReader) processWatermarks(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watermark, ok := <-r.tracker.watermarks:
			if !ok {
				return nil
			}
			if err := r.partitionStorage.UpdateWatermark(
				ctx, r.partition.PartitionToken, watermark,
			); err != nil {
				return fmt.Errorf("update watermark: %w", err)
			}
		}
	}
}

// processErrors processes errors.
func (r *partitionReader) processErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-r.tracker.errors:
			if !ok {
				return nil
			}
			if r.config.errorHandler == nil {
				return err
			}
			shouldContinue, retryDelay := r.config.errorHandler.HandleError(
				ctx, r.partition, nil, err,
			)
			if !shouldContinue && retryDelay == 0 {
				return err
			}
			if retryDelay > 0 {
				// TODO: Implement retry logic.
				_ = retryDelay
			}
		}
	}
}

func (r *partitionReader) readStream(ctx context.Context) error {
	stmt := r.buildStatement()
	iter := r.spannerClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{
		Priority: r.config.spannerRequestPriority,
	})

	return iter.Do(func(row *spanner.Row) error {
		records := []*changeRecord{}
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
			"startTimestamp":        r.partition.Watermark,
			"endTimestamp":          r.partition.EndTimestamp,
			"partitionToken":        r.partition.PartitionToken,
			"heartbeatMilliseconds": r.partition.HeartbeatMillis,
		},
	}

	if r.partition.IsRootPartition() {
		// Must be converted to NULL (root partition).
		stmt.Params["partitionToken"] = nil
	}

	return stmt
}

func (r *partitionReader) processRecords(ctx context.Context, records []*changeRecord) error {
	for _, cr := range records {
		// DataChangeRecord processing.
		for _, record := range cr.DataChangeRecords {
			if err := r.processDataChangeRecord(ctx, record); err != nil {
				return err
			}
		}

		// HeartbeatRecord processing.
		// Allocate sequence and complete immediately to include in continuous ack chain.
		for _, record := range cr.HeartbeatRecords {
			if err := r.processHeartbeatRecord(ctx, record); err != nil {
				return err
			}
		}

		// ChildPartitionsRecord processing.
		for _, record := range cr.ChildPartitionsRecords {
			if err := r.processChildPartitionsRecord(ctx, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *partitionReader) processDataChangeRecord(ctx context.Context, record *dataChangeRecord) error {
	// 1. Acquire semaphore (blocks if MaxInflight reached).
	if err := r.tracker.acquire(ctx); err != nil {
		return err
	}

	// 2. Register in in-flight.
	decoded := record.decodeToNonSpannerType()
	seq := r.tracker.add(record.CommitTimestamp)

	// 3. Call Consumer in goroutine.
	go func() {
		err := r.consumer.Consume(ctx, decoded)
		r.tracker.complete(seq, err)
	}()

	return nil
}

func (r *partitionReader) processHeartbeatRecord(_ context.Context, record *HeartbeatRecord) error {
	// HeartbeatRecord doesn't call Consumer, so no goroutine is spawned.
	// Semaphore (acquire) is not needed. ackImmediate for immediate ack.
	// This advances watermark even when no DataChangeRecord arrives.
	r.tracker.ackImmediate(record.Timestamp)
	return nil
}

func (r *partitionReader) processChildPartitionsRecord(ctx context.Context, record *ChildPartitionsRecord) error {
	// ChildPartitionsRecord also doesn't spawn goroutine, so no semaphore needed.
	// Important: Process in order of persist -> notify -> ack.
	// If ack comes first and persist fails, recovery is impossible.

	// 1. Persist child partitions.
	if err := r.partitionStorage.AddChildPartitions(
		ctx,
		r.partition.EndTimestamp,
		r.partition.HeartbeatMillis,
		record,
	); err != nil {
		return fmt.Errorf("add child partitions: %w", err)
	}

	// 2. Notify coordinator.
	r.events <- partitionEvent{
		eventType:       eventChildPartitions,
		partition:       r.partition,
		childPartitions: record,
	}

	// 3. Ack last.
	r.tracker.ackImmediate(record.StartTimestamp)

	return nil
}
