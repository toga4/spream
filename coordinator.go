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

	// Events.
	events chan partitionEvent

	// Control.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Error.
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
		events:           make(chan partitionEvent, 100),
	}
}

func (c *coordinator) run(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()

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

	// 4. Event processing loop.
	c.wg.Add(1)
	go c.handleEvents()

	// 5. Wait for completion.
	c.wg.Wait()

	if c.err != nil {
		return c.err
	}

	return c.ctx.Err()
}

func (c *coordinator) initialize() error {
	// Initialize root partition if this is the first run or if the previous run has already been completed.
	minWatermarkPartition, err := c.partitionStorage.GetUnfinishedMinWatermarkPartition(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition on start subscribe: %w", err)
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
		c.config,
		c.events,
	)
	c.readers[partition.PartitionToken] = reader

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := reader.run(c.ctx); err != nil {
			c.events <- partitionEvent{
				eventType: eventPartitionError,
				partition: partition,
				err:       err,
			}
		} else {
			c.events <- partitionEvent{
				eventType: eventPartitionFinished,
				partition: partition,
			}
		}
	}()
}

func (c *coordinator) handleEvents() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case event := <-c.events:
			c.processEvent(event)
		}
	}
}

func (c *coordinator) processEvent(event partitionEvent) {
	switch event.eventType {
	case eventChildPartitions:
		// Child partitions have been added to storage, nothing else to do here.
		// The next tick of detectNewPartitionsLoop will pick them up.
	case eventPartitionFinished:
		c.removeReader(event.partition.PartitionToken)
	case eventPartitionError:
		c.removeReader(event.partition.PartitionToken)
		c.recordError(event.err)
	}
}

func (c *coordinator) removeReader(partitionToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.readers, partitionToken)
}

func (c *coordinator) recordError(err error) {
	c.errOnce.Do(func() {
		c.err = err
		c.cancel()
	})
}

func (c *coordinator) initiateShutdown() {
	c.cancel()
}
