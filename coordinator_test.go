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

func TestCoordinator_Shutdown(t *testing.T) {
	t.Run("shutdown causes Subscribe to return ErrShutdown immediately", func(t *testing.T) {
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

		// Call shutdown. Subscribe should return ErrShutdown immediately.
		go func() {
			ctx := context.Background()
			c.shutdown(ctx)
		}()

		// run() should return ErrShutdown immediately.
		select {
		case runErr := <-runDone:
			if !errors.Is(runErr, ErrShutdown) {
				t.Errorf("run() should return ErrShutdown, got: %v", runErr)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("run() should return immediately after shutdown")
		}
	})

	t.Run("shutdown waits for drain completion", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test-partition",
					Watermark:      time.Now(),
				}, nil
			},
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
		go func() {
			c.run()
		}()

		// Wait for coordinator to start.
		time.Sleep(50 * time.Millisecond)

		// Shutdown should return nil when drain completes (no readers to drain).
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- c.shutdown(context.Background())
		}()

		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Errorf("shutdown() should return nil, got: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("shutdown() should complete when drain finishes")
		}
	})

	t.Run("shutdown timeout returns ctx.Err()", func(t *testing.T) {
		// Test shutdown timeout behavior directly without run().
		// This tests that shutdown returns ctx.Err() when drain doesn't complete in time.
		coordinatorCtx, coordinatorCancel := context.WithCancelCause(context.Background())
		defer coordinatorCancel(nil)

		c := &coordinator{
			ctx:        coordinatorCtx,
			cancel:     coordinatorCancel,
			shutdownCh: make(chan struct{}),
		}

		// Simulate an in-flight reader that takes longer than the shutdown timeout.
		readerDone := make(chan struct{})
		c.readerWg.Go(func() {
			<-readerDone // Block until explicitly released.
		})

		// Call shutdown with a very short timeout context.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		shutdownErr := c.shutdown(ctx)
		if !errors.Is(shutdownErr, context.DeadlineExceeded) {
			t.Errorf("shutdown() should return context.DeadlineExceeded, got: %v", shutdownErr)
		}

		// Verify shutdown state.
		if !c.shutdownFlag.Load() {
			t.Error("shutdownFlag should be true after shutdown()")
		}

		// Cleanup: release the blocked reader.
		close(readerDone)
	})
}

func TestCoordinator_Close(t *testing.T) {
	t.Run("close causes Subscribe to return ErrClosed", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test-partition",
					Watermark:      time.Now(),
				}, nil
			},
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

		// Close the coordinator.
		c.close()

		// run() should return ErrClosed.
		runErr := <-runDone
		if !errors.Is(runErr, ErrClosed) {
			t.Errorf("run() should return ErrClosed, got: %v", runErr)
		}
	})

	t.Run("close called before shutdown completes returns ErrClosed", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test-partition",
					Watermark:      time.Now(),
				}, nil
			},
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

		// Call close directly (which sets closedFlag and cancels context).
		// This should cause run() to return ErrClosed.
		c.close()

		runErr := <-runDone
		if !errors.Is(runErr, ErrClosed) {
			t.Errorf("run() should return ErrClosed, got: %v", runErr)
		}
	})

	t.Run("close takes precedence when both flags are set", func(t *testing.T) {
		// Test exitError priority directly
		c := &coordinator{}
		c.shutdownFlag.Store(true)
		c.closedFlag.Store(true)

		err := c.exitError()
		if !errors.Is(err, ErrClosed) {
			t.Errorf("exitError() should return ErrClosed when both flags are set, got: %v", err)
		}
	})
}

func TestCoordinator_AllPartitionsFinished(t *testing.T) {
	t.Run("all partitions finished returns nil from Subscribe", func(t *testing.T) {
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
				// Return nil on subsequent calls (detectAndSchedulePartitions) to trigger all partitions finished.
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

func TestCoordinator_ExistingError(t *testing.T) {
	t.Run("existing error takes precedence", func(t *testing.T) {
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
}
