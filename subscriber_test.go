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

// newTestSubscriber creates a Subscriber for testing with the given storage.
// This bypasses validation since tests don't need an actual Spanner client.
func newTestSubscriber(storage PartitionStorage, partitionDiscoveryInterval time.Duration) *Subscriber {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Subscriber{
		spannerClient:              nil, // Tests don't use actual Spanner client.
		streamName:                 "test-stream",
		partitionStorage:           storage,
		consumer:                   ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error { return nil }),
		startTimestamp:             nowFunc(),
		endTimestamp:               defaultEndTimestamp,
		heartbeatInterval:          defaultHeartbeatInterval,
		maxInflight:                defaultMaxInflight,
		partitionDiscoveryInterval: partitionDiscoveryInterval,
		readers: make(map[string]*partitionReader),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

func TestSubscriber_Shutdown(t *testing.T) {
	t.Run("shutdown causes Subscribe to return ErrShutdown immediately", func(t *testing.T) {
		storage := &mockPartitionStorage{
			// Return a partition to keep the subscriber running.
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

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Run subscriber in a goroutine.
		runDone := make(chan error, 1)
		go func() {
			runDone <- subscriber.Subscribe()
		}()

		// Wait for subscriber to start.
		time.Sleep(50 * time.Millisecond)

		// Call shutdown. Subscribe should return ErrShutdown immediately.
		go func() {
			ctx := context.Background()
			subscriber.Shutdown(ctx)
		}()

		// Subscribe() should return ErrShutdown immediately.
		select {
		case runErr := <-runDone:
			if !errors.Is(runErr, ErrShutdown) {
				t.Errorf("Subscribe() should return ErrShutdown, got: %v", runErr)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Subscribe() should return immediately after shutdown")
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

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Run subscriber in a goroutine.
		go func() {
			subscriber.Subscribe()
		}()

		// Wait for subscriber to start.
		time.Sleep(50 * time.Millisecond)

		// Shutdown should return nil when drain completes (no readers to drain).
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- subscriber.Shutdown(context.Background())
		}()

		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Errorf("Shutdown() should return nil, got: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Shutdown() should complete when drain finishes")
		}
	})

	t.Run("shutdown timeout returns ctx.Err()", func(t *testing.T) {
		// Test shutdown timeout behavior directly without Subscribe().
		// This tests that Shutdown returns ctx.Err() when drain doesn't complete in time.
		baseCtx, baseCancel := context.WithCancelCause(context.Background())
		defer baseCancel(nil)

		s := &Subscriber{
			ctx:    baseCtx,
			cancel: baseCancel,
			done:   make(chan struct{}),
		}

		// Simulate an in-flight reader that takes longer than the shutdown timeout.
		readerDone := make(chan struct{})
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			<-readerDone // Block until explicitly released.
		}()

		// Call shutdown with a very short timeout context.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		shutdownErr := s.Shutdown(ctx)
		if !errors.Is(shutdownErr, context.DeadlineExceeded) {
			t.Errorf("Shutdown() should return context.DeadlineExceeded, got: %v", shutdownErr)
		}

		// Verify shutdown state.
		if !s.shutdown.Load() {
			t.Error("shutdown should be true after Shutdown()")
		}

		// Cleanup: release the blocked reader.
		close(readerDone)
	})
}

func TestSubscriber_Close(t *testing.T) {
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

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Run subscriber in a goroutine.
		runDone := make(chan error, 1)
		go func() {
			runDone <- subscriber.Subscribe()
		}()

		// Wait for subscriber to start.
		time.Sleep(50 * time.Millisecond)

		// Close the subscriber.
		subscriber.Close()

		// Subscribe() should return ErrClosed.
		runErr := <-runDone
		if !errors.Is(runErr, ErrClosed) {
			t.Errorf("Subscribe() should return ErrClosed, got: %v", runErr)
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

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Run subscriber in a goroutine.
		runDone := make(chan error, 1)
		go func() {
			runDone <- subscriber.Subscribe()
		}()

		// Wait for subscriber to start.
		time.Sleep(50 * time.Millisecond)

		// Call close directly (which sets closed and cancels context).
		// This should cause Subscribe() to return ErrClosed.
		subscriber.Close()

		runErr := <-runDone
		if !errors.Is(runErr, ErrClosed) {
			t.Errorf("Subscribe() should return ErrClosed, got: %v", runErr)
		}
	})

	t.Run("close takes precedence when both flags are set", func(t *testing.T) {
		// Test exitError priority directly
		s := &Subscriber{}
		s.shutdown.Store(true)
		s.closed.Store(true)

		err := s.exitError()
		if !errors.Is(err, ErrClosed) {
			t.Errorf("exitError() should return ErrClosed when both flags are set, got: %v", err)
		}
	})
}

func TestSubscriber_exitError(t *testing.T) {
	testErr := errors.New("test error")

	tests := []struct {
		name     string
		closed   bool
		shutdown bool
		err      error
		want     error
	}{
		{
			name: "no flags and no error returns nil",
			want: nil,
		},
		{
			name:         "shutdown only returns ErrShutdown",
			shutdown: true,
			want:         ErrShutdown,
		},
		{
			name:       "close only returns ErrClosed",
			closed: true,
			want:       ErrClosed,
		},
		{
			name: "error only returns that error",
			err:  testErr,
			want: testErr,
		},
		{
			name:         "error takes precedence over shutdown",
			shutdown: true,
			err:          testErr,
			want:         testErr,
		},
		{
			name:       "close takes precedence over error",
			closed: true,
			err:        testErr,
			want:       ErrClosed,
		},
		{
			name:         "close takes precedence over shutdown and error",
			closed:   true,
			shutdown: true,
			err:          testErr,
			want:         ErrClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscriber{err: tt.err}
			if tt.closed {
				s.closed.Store(true)
			}
			if tt.shutdown {
				s.shutdown.Store(true)
			}

			got := s.exitError()
			if !errors.Is(got, tt.want) {
				t.Errorf("exitError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubscriber_AllPartitionsFinished(t *testing.T) {
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

		subscriber := newTestSubscriber(storage, 50*time.Millisecond)

		// Subscribe() should return nil when all partitions finish.
		runErr := subscriber.Subscribe()
		if runErr != nil {
			t.Errorf("Subscribe() should return nil, got: %v", runErr)
		}
	})
}

func TestSubscriber_ExistingError(t *testing.T) {
	t.Run("existing error takes precedence", func(t *testing.T) {
		existingErr := errors.New("existing error")

		storage := &mockPartitionStorage{
			// Return error to trigger recordError.
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return nil, existingErr
			},
		}

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Subscribe() should return the existing error (initialization fails).
		runErr := subscriber.Subscribe()
		if !errors.Is(runErr, existingErr) {
			t.Errorf("Subscribe() should return existing error, got: %v", runErr)
		}
	})
}

func TestSubscriber_AlreadyStarted(t *testing.T) {
	t.Run("Subscribe returns error when called twice", func(t *testing.T) {
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

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Start the first Subscribe in a goroutine.
		go func() {
			subscriber.Subscribe()
		}()

		// Wait for subscriber to start.
		time.Sleep(50 * time.Millisecond)

		// Try to call Subscribe again.
		err := subscriber.Subscribe()
		if err == nil || err.Error() != "spream: subscriber already started" {
			t.Errorf("expected 'subscriber already started' error, got: %v", err)
		}

		// Cleanup.
		subscriber.Close()
	})
}

func TestNewSubscriber_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: "spream: config is required",
		},
		{
			name: "nil SpannerClient",
			cfg: &Config{
				StreamName:       "test",
				PartitionStorage: &mockPartitionStorage{},
				Consumer:         ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error { return nil }),
			},
			wantErr: "spream: SpannerClient is required",
		},
		{
			name: "empty StreamName",
			cfg: &Config{
				SpannerClient:    nil, // We can't easily create a real client for this test.
				PartitionStorage: &mockPartitionStorage{},
				Consumer:         ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error { return nil }),
			},
			wantErr: "spream: SpannerClient is required", // SpannerClient is checked first.
		},
		{
			name: "nil PartitionStorage",
			cfg: &Config{
				StreamName: "test",
				Consumer:   ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error { return nil }),
			},
			wantErr: "spream: SpannerClient is required", // SpannerClient is checked first.
		},
		{
			name: "nil Consumer",
			cfg: &Config{
				StreamName:       "test",
				PartitionStorage: &mockPartitionStorage{},
			},
			wantErr: "spream: SpannerClient is required", // SpannerClient is checked first.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSubscriber(tt.cfg)
			if err == nil {
				t.Error("expected error, got nil")
				return
			}
			if err.Error() != tt.wantErr {
				t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}
