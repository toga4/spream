package spream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockPartitionStorage is a mock implementation of PartitionStorage for testing.
type mockPartitionStorage struct {
	getUnfinishedMinWatermarkPartitionFunc func(ctx context.Context) (*PartitionMetadata, error)
	getInterruptedPartitionsFunc           func(ctx context.Context) ([]*PartitionMetadata, error)
	initializeRootPartitionFunc            func(ctx context.Context, startTimestamp, endTimestamp time.Time, heartbeatInterval time.Duration) error
	getSchedulablePartitionsFunc           func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error)
	addChildPartitionsFunc                 func(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, childPartitionsRecord *ChildPartitionsRecord) error
	updateToScheduledFunc                  func(ctx context.Context, partitionTokens []string) error
	updateToRunningFunc                    func(ctx context.Context, partitionToken string) error
	updateToFinishedFunc                   func(ctx context.Context, partitionToken string) error
	updateWatermarkFunc                    func(ctx context.Context, partitionToken string, watermark time.Time) error
}

func (m *mockPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error) {
	if m.getUnfinishedMinWatermarkPartitionFunc != nil {
		return m.getUnfinishedMinWatermarkPartitionFunc(ctx)
	}
	return nil, nil
}

func (m *mockPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*PartitionMetadata, error) {
	if m.getInterruptedPartitionsFunc != nil {
		return m.getInterruptedPartitionsFunc(ctx)
	}
	return nil, nil
}

func (m *mockPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	if m.initializeRootPartitionFunc != nil {
		return m.initializeRootPartitionFunc(ctx, startTimestamp, endTimestamp, heartbeatInterval)
	}
	return nil
}

func (m *mockPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
	if m.getSchedulablePartitionsFunc != nil {
		return m.getSchedulablePartitionsFunc(ctx, minWatermark)
	}
	return nil, nil
}

func (m *mockPartitionStorage) AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, childPartitionsRecord *ChildPartitionsRecord) error {
	if m.addChildPartitionsFunc != nil {
		return m.addChildPartitionsFunc(ctx, endTimestamp, heartbeatMillis, childPartitionsRecord)
	}
	return nil
}

func (m *mockPartitionStorage) UpdateToScheduled(ctx context.Context, partitionTokens []string) error {
	if m.updateToScheduledFunc != nil {
		return m.updateToScheduledFunc(ctx, partitionTokens)
	}
	return nil
}

func (m *mockPartitionStorage) UpdateToRunning(ctx context.Context, partitionToken string) error {
	if m.updateToRunningFunc != nil {
		return m.updateToRunningFunc(ctx, partitionToken)
	}
	return nil
}

func (m *mockPartitionStorage) UpdateToFinished(ctx context.Context, partitionToken string) error {
	if m.updateToFinishedFunc != nil {
		return m.updateToFinishedFunc(ctx, partitionToken)
	}
	return nil
}

func (m *mockPartitionStorage) UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error {
	if m.updateWatermarkFunc != nil {
		return m.updateWatermarkFunc(ctx, partitionToken, watermark)
	}
	return nil
}

func TestCoordinator_ShutdownAborted(t *testing.T) {
	t.Run("shutdown timeout returns ErrShutdownAborted from Subscribe", func(t *testing.T) {
		storage := &mockPartitionStorage{
			// Return a partition to keep the coordinator running.
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test-partition",
					Watermark:      time.Now(),
				}, nil
			},
			// Return no schedulable partitions to avoid starting partition readers.
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return nil, nil
			},
		}

		cfg := newConfig()
		cfg.partitionDiscoveryInterval = 100 * time.Millisecond

		c := newCoordinator(
			nil,
			"test-stream",
			storage,
			ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error {
				return nil
			}),
			cfg,
		)

		// Run coordinator in a goroutine.
		runDone := make(chan error, 1)
		go func() {
			runDone <- c.run()
		}()

		// Wait for coordinator to start.
		time.Sleep(50 * time.Millisecond)

		// Call shutdown with an already-canceled context to simulate timeout.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		shutdownErr := c.shutdown(ctx)
		if !errors.Is(shutdownErr, context.Canceled) {
			t.Errorf("shutdown() should return context.Canceled, got: %v", shutdownErr)
		}

		// Close to stop the coordinator.
		_ = c.close()

		// run() should return ErrShutdownAborted.
		runErr := <-runDone
		if !errors.Is(runErr, ErrShutdownAborted) {
			t.Errorf("run() should return ErrShutdownAborted, got: %v", runErr)
		}
	})

	t.Run("existing error takes precedence over ErrShutdownAborted", func(t *testing.T) {
		existingErr := errors.New("existing error")

		storage := &mockPartitionStorage{
			// Return error to trigger recordError.
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return nil, existingErr
			},
		}

		cfg := newConfig()

		c := newCoordinator(
			nil,
			"test-stream",
			storage,
			ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error {
				return nil
			}),
			cfg,
		)

		// run() should return the existing error (initialization fails).
		runErr := c.run()
		if !errors.Is(runErr, existingErr) {
			t.Errorf("run() should return existing error, got: %v", runErr)
		}
	})

	t.Run("successful shutdown returns nil from Subscribe", func(t *testing.T) {
		callCount := 0
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				callCount++
				if callCount == 1 {
					// Return a partition on first call (initialize).
					return &PartitionMetadata{
						PartitionToken: "test-partition",
						Watermark:      time.Now(),
					}, nil
				}
				// Return nil on subsequent calls (detectAndSchedulePartitions) to trigger graceful shutdown.
				return nil, nil
			},
		}

		cfg := newConfig()
		cfg.partitionDiscoveryInterval = 50 * time.Millisecond

		c := newCoordinator(
			nil,
			"test-stream",
			storage,
			ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error {
				return nil
			}),
			cfg,
		)

		// run() should return nil when all partitions finish.
		runErr := c.run()
		if runErr != nil {
			t.Errorf("run() should return nil, got: %v", runErr)
		}
	})
}
