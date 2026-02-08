package spream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			if diff := cmp.Diff(ErrShutdown, runErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
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
		if diff := cmp.Diff(context.DeadlineExceeded, shutdownErr, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Shutdown() error mismatch (-want +got):\n%s", diff)
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
		if diff := cmp.Diff(ErrClosed, runErr, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
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
		if diff := cmp.Diff(ErrClosed, runErr, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("close takes precedence when both flags are set", func(t *testing.T) {
		// Test exitError priority directly
		s := &Subscriber{}
		s.shutdown.Store(true)
		s.closed.Store(true)

		err := s.exitError()
		if diff := cmp.Diff(ErrClosed, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("exitError() error mismatch (-want +got):\n%s", diff)
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
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("exitError() error mismatch (-want +got):\n%s", diff)
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
		if diff := cmp.Diff(existingErr, runErr, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
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

func TestConsumerFunc(t *testing.T) {
	called := false
	var gotRecord *DataChangeRecord
	f := ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error {
		called = true
		gotRecord = r
		return nil
	})

	record := &DataChangeRecord{TableName: "Users"}
	err := f.Consume(context.Background(), record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("ConsumerFunc was not called")
	}
	if gotRecord.TableName != "Users" {
		t.Errorf("TableName = %v, want %q", gotRecord.TableName, "Users")
	}
}

func TestConsumerFunc_Error(t *testing.T) {
	testErr := errors.New("consume error")
	f := ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error {
		return testErr
	})

	err := f.Consume(context.Background(), &DataChangeRecord{})
	if diff := cmp.Diff(testErr, err, cmpopts.EquateErrors()); diff != "" {
		t.Errorf("Consume() error mismatch (-want +got):\n%s", diff)
	}
}

func TestSubscriber_fail(t *testing.T) {
	t.Run("first error is recorded", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)

		s := &Subscriber{
			ctx:    ctx,
			cancel: cancel,
			done:   make(chan struct{}),
		}

		err1 := errors.New("first error")
		err2 := errors.New("second error")

		s.fail(err1)
		s.fail(err2)

		if s.err != err1 {
			t.Errorf("err = %v, want %v", s.err, err1)
		}
	})

	t.Run("fail cancels context", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)

		s := &Subscriber{
			ctx:    ctx,
			cancel: cancel,
			done:   make(chan struct{}),
		}

		testErr := errors.New("fail error")
		s.fail(testErr)

		select {
		case <-ctx.Done():
			// Context was canceled.
		default:
			t.Error("context was not canceled after fail")
		}
	})
}

func TestSubscriber_detectAndSchedulePartitions(t *testing.T) {
	t.Run("error from GetSchedulablePartitions is propagated", func(t *testing.T) {
		storageErr := errors.New("get schedulable error")
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test",
					Watermark:      time.Now(),
				}, nil
			},
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return nil, storageErr
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.detectAndSchedulePartitions()
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("detectAndSchedulePartitions() error mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("error from GetUnfinishedMinWatermarkPartition is propagated", func(t *testing.T) {
		storageErr := errors.New("get min watermark error")
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return nil, storageErr
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.detectAndSchedulePartitions()
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("detectAndSchedulePartitions() error mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("error from UpdateToScheduled is propagated", func(t *testing.T) {
		storageErr := errors.New("update scheduled error")
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test",
					Watermark:      time.Now(),
				}, nil
			},
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return []*PartitionMetadata{
					{PartitionToken: "child-1"},
				}, nil
			},
			updateToScheduledFunc: func(ctx context.Context, tokens []string) error {
				return storageErr
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.detectAndSchedulePartitions()
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("detectAndSchedulePartitions() error mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("returns nil when no schedulable partitions", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test",
					Watermark:      time.Now(),
				}, nil
			},
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return nil, nil
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.detectAndSchedulePartitions()
		if err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	})
}

func TestSubscriber_initialize(t *testing.T) {
	t.Run("skips root initialization when unfinished partition exists", func(t *testing.T) {
		initCalled := false
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "existing",
					Watermark:      time.Now(),
				}, nil
			},
			initializeRootPartitionFunc: func(ctx context.Context, start, end time.Time, hb time.Duration) error {
				initCalled = true
				return nil
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.initialize()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if initCalled {
			t.Error("InitializeRootPartition should not be called when unfinished partition exists")
		}
	})

	t.Run("returns error when InitializeRootPartition fails", func(t *testing.T) {
		initErr := errors.New("init error")
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return nil, nil
			},
			initializeRootPartitionFunc: func(ctx context.Context, start, end time.Time, hb time.Duration) error {
				return initErr
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.initialize()
		if diff := cmp.Diff(initErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("initialize() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSubscriber_resumeInterruptedPartitions(t *testing.T) {
	t.Run("returns error when GetInterruptedPartitions fails", func(t *testing.T) {
		storageErr := errors.New("interrupted error")
		storage := &mockPartitionStorage{
			getInterruptedPartitionsFunc: func(ctx context.Context) ([]*PartitionMetadata, error) {
				return nil, storageErr
			},
		}

		s := newTestSubscriber(storage, 100*time.Millisecond)
		err := s.resumeInterruptedPartitions()
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("resumeInterruptedPartitions() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSubscriber_drain(t *testing.T) {
	t.Run("drain is idempotent", func(t *testing.T) {
		s := &Subscriber{
			done: make(chan struct{}),
		}

		// drain is safe to call multiple times.
		ch1 := s.drain()
		ch2 := s.drain()

		// The same channel should be returned.
		if ch1 != ch2 {
			t.Error("drain() returned different channels")
		}
	})
}

func TestSubscriber_MainLoopDetectError(t *testing.T) {
	t.Run("detectAndSchedulePartitions error during main loop triggers fail", func(t *testing.T) {
		callCount := 0
		storageErr := errors.New("detect error")
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				callCount++
				if callCount == 1 {
					// Return a partition during initialization.
					return &PartitionMetadata{
						PartitionToken: "test",
						Watermark:      time.Now(),
					}, nil
				}
				// Return an error during the main loop.
				return nil, storageErr
			},
		}

		subscriber := newTestSubscriber(storage, 50*time.Millisecond)
		err := subscriber.Subscribe()
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSubscriber_BaseContextCancellation(t *testing.T) {
	t.Run("base context cancellation stops subscriber", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test",
					Watermark:      time.Now(),
				}, nil
			},
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return nil, nil
			},
		}

		baseCtx, baseCancel := context.WithCancel(context.Background())
		ctx, cancel := context.WithCancelCause(baseCtx)

		subscriber := &Subscriber{
			streamName:                 "test-stream",
			partitionStorage:           storage,
			consumer:                   ConsumerFunc(func(ctx context.Context, record *DataChangeRecord) error { return nil }),
			startTimestamp:             nowFunc(),
			endTimestamp:               defaultEndTimestamp,
			heartbeatInterval:          defaultHeartbeatInterval,
			maxInflight:                defaultMaxInflight,
			partitionDiscoveryInterval: 50 * time.Millisecond,
			readers:                    make(map[string]*partitionReader),
			ctx:                        ctx,
			cancel:                     cancel,
			done:                       make(chan struct{}),
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- subscriber.Subscribe()
		}()

		time.Sleep(50 * time.Millisecond)
		baseCancel()

		select {
		case err := <-runDone:
			// Base context cancellation results in no error (or context.Canceled).
			if err != nil {
				if diff := cmp.Diff(context.Canceled, err, cmpopts.EquateErrors()); diff != "" {
					t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
				}
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Subscribe() did not return after base context cancellation")
		}
	})
}

func TestSubscriber_ResumeError_FromSubscribe(t *testing.T) {
	t.Run("resume interrupted partitions error is returned from Subscribe", func(t *testing.T) {
		resumeErr := errors.New("resume error")
		storage := &mockPartitionStorage{
			// Let initialization complete successfully.
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return nil, nil
			},
			initializeRootPartitionFunc: func(ctx context.Context, start, end time.Time, hb time.Duration) error {
				return nil
			},
			getInterruptedPartitionsFunc: func(ctx context.Context) ([]*PartitionMetadata, error) {
				return nil, resumeErr
			},
		}

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)
		err := subscriber.Subscribe()
		if diff := cmp.Diff(resumeErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSubscriber_Close_WithReaders(t *testing.T) {
	t.Run("close force-closes all active readers", func(t *testing.T) {
		storage := &mockPartitionStorage{
			getUnfinishedMinWatermarkPartitionFunc: func(ctx context.Context) (*PartitionMetadata, error) {
				return &PartitionMetadata{
					PartitionToken: "test",
					Watermark:      time.Now(),
				}, nil
			},
			getSchedulablePartitionsFunc: func(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
				return nil, nil
			},
		}

		subscriber := newTestSubscriber(storage, 100*time.Millisecond)

		// Inject a dummy reader and add an in-flight record.
		tracker := newInflightTracker(10)
		if err := tracker.acquire(context.Background()); err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		tracker.add(time.Now())
		reader := &partitionReader{
			partitionToken: "test-reader",
			tracker:        tracker,
		}
		subscriber.readers["test-reader"] = reader

		runDone := make(chan error, 1)
		go func() {
			runDone <- subscriber.Subscribe()
		}()

		time.Sleep(50 * time.Millisecond)

		// Close force-terminates the readers.
		subscriber.Close()

		select {
		case err := <-runDone:
			if diff := cmp.Diff(ErrClosed, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Subscribe() error mismatch (-want +got):\n%s", diff)
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Subscribe() did not return after Close")
		}

		// The reader's tracker should be closed.
		acquireErr := tracker.acquire(context.Background())
		if diff := cmp.Diff(errTrackerClosed, acquireErr, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("tracker.acquire() error mismatch (-want +got):\n%s", diff)
		}
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
