package spream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
)

// coordinator manages partition readers and coordinates change stream subscription.
type coordinator struct {
	// Dependencies.
	spannerClient    *spanner.Client
	streamName       string
	partitionStorage PartitionStorage
	consumer         Consumer
	config           *config

	// Partition management.
	mu      sync.RWMutex
	readers map[string]*partitionReader

	// Control.
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	// Completion notification.
	done chan struct{}

	// Error handling.
	// spream has two error propagation paths: the err field and context.Cause.
	// cancel(err) sets the cause only on the first call.
	// If shutdown() calls cancel() first, subsequent errors won't be reflected
	// in the cause. Therefore, we record errors separately via err + errOnce.
	err     error
	errOnce sync.Once
}

func newCoordinator(
	spannerClient *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	consumer Consumer,
	cfg *config,
) *coordinator {
	return &coordinator{
		spannerClient:    spannerClient,
		streamName:       streamName,
		partitionStorage: partitionStorage,
		consumer:         consumer,
		config:           cfg,
		readers:          make(map[string]*partitionReader),
		done:             make(chan struct{}),
	}
}

func (c *coordinator) run() error {
	defer close(c.done)

	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	defer c.cancel(nil)

	// 1. Initialize.
	if err := c.initialize(); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}

	// 2. Resume interrupted partitions.
	if err := c.resumeInterruptedPartitions(); err != nil {
		return fmt.Errorf("resume interrupted partitions: %w", err)
	}

	// 3. Partition detection loop.
	c.wg.Add(1)
	go c.detectNewPartitionsLoop()

	// 4. Wait for completion.
	c.wg.Wait()

	if c.err != nil {
		return c.err
	}

	// Determine exit reason from the cancel cause:
	// - initiateShutdown / shutdown: cancel(errGracefulShutdown) → return nil
	// - close: cancel(ErrSubscriberClosed) → return ErrSubscriberClosed
	if cause := context.Cause(c.ctx); cause != nil && cause != errGracefulShutdown {
		return cause
	}
	return nil
}

func (c *coordinator) initialize() error {
	// Initialize root partition if this is the first run or if the previous run has already been completed.
	minWatermarkPartition, err := c.partitionStorage.GetUnfinishedMinWatermarkPartition(c.ctx)
	if err != nil {
		return fmt.Errorf("get unfinished min watermark partition: %w", err)
	}
	if minWatermarkPartition == nil {
		if err := c.partitionStorage.InitializeRootPartition(
			c.ctx,
			c.config.startTimestamp,
			c.config.endTimestamp,
			c.config.heartbeatInterval,
		); err != nil {
			return fmt.Errorf("failed to initialize root partition: %w", err)
		}
	}
	return nil
}

func (c *coordinator) resumeInterruptedPartitions() error {
	interruptedPartitions, err := c.partitionStorage.GetInterruptedPartitions(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get interrupted partitions: %w", err)
	}
	for _, p := range interruptedPartitions {
		c.startPartitionReader(p)
	}
	return nil
}

func (c *coordinator) detectNewPartitionsLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.partitionDiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.detectAndSchedulePartitions(); err != nil {
				if errors.Is(err, errAllPartitionsFinished) {
					c.initiateShutdown()
					return
				}
				c.recordError(err)
				return
			}
		}
	}
}

func (c *coordinator) detectAndSchedulePartitions() error {
	minWatermarkPartition, err := c.partitionStorage.GetUnfinishedMinWatermarkPartition(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition: %w", err)
	}

	if minWatermarkPartition == nil {
		return errAllPartitionsFinished
	}

	// To make sure changes for a key is processed in timestamp order, wait until the records returned from all parents have been processed.
	partitions, err := c.partitionStorage.GetSchedulablePartitions(c.ctx, minWatermarkPartition.Watermark)
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
	if err := c.partitionStorage.UpdateToScheduled(c.ctx, partitionTokens); err != nil {
		return fmt.Errorf("failed to update to scheduled: %w", err)
	}

	for _, p := range partitions {
		c.startPartitionReader(p)
	}

	return nil
}

func (c *coordinator) startPartitionReader(partition *PartitionMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Skip if reader already exists.
	if _, exists := c.readers[partition.PartitionToken]; exists {
		return
	}

	reader := newPartitionReader(
		partition,
		c.spannerClient,
		c.streamName,
		c.partitionStorage,
		c.consumer,
		c.config.maxInflight,
	)
	c.readers[partition.PartitionToken] = reader

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := reader.run(c.ctx)
		c.removeReader(partition.PartitionToken)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return // Shutdown or Close.
			}
			c.recordError(err)
		}
	}()
}

func (c *coordinator) removeReader(partitionToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.readers, partitionToken)
}

func (c *coordinator) recordError(err error) {
	c.errOnce.Do(func() {
		c.err = err
		c.cancel(err)
	})
}

func (c *coordinator) initiateShutdown() {
	c.cancel(errGracefulShutdown)
}

// shutdown gracefully shuts down the coordinator.
// It stops accepting new partitions and waits for in-flight records to complete.
func (c *coordinator) shutdown(ctx context.Context) error {
	c.cancel(errGracefulShutdown)

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.recordError(ErrShutdownAborted)
		return ctx.Err()
	}
}

// close immediately closes the coordinator.
// It does not wait for in-flight records to complete.
func (c *coordinator) close() error {
	c.cancel(ErrSubscriberClosed)

	// Force close all readers to break out of drainInflight.
	c.mu.RLock()
	for _, reader := range c.readers {
		reader.Close()
	}
	c.mu.RUnlock()

	<-c.done
	return nil
}
