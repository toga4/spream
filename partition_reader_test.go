package spream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewPartitionReader(t *testing.T) {
	partition := &PartitionMetadata{
		PartitionToken:  "token-1",
		Watermark:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTimestamp:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		HeartbeatMillis: 10000,
	}
	storage := &mockPartitionStorage{}
	consumer := ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error { return nil })

	reader := newPartitionReader(context.Background(), partition, nil, "TestStream", storage, consumer, 5)

	if reader.partitionToken != "token-1" {
		t.Errorf("partitionToken = %v, want %q", reader.partitionToken, "token-1")
	}
	if !reader.startTimestamp.Equal(partition.Watermark) {
		t.Errorf("startTimestamp = %v, want %v", reader.startTimestamp, partition.Watermark)
	}
	if !reader.endTimestamp.Equal(partition.EndTimestamp) {
		t.Errorf("endTimestamp = %v, want %v", reader.endTimestamp, partition.EndTimestamp)
	}
	if reader.heartbeatMillis != 10000 {
		t.Errorf("heartbeatMillis = %v, want 10000", reader.heartbeatMillis)
	}
	if reader.streamName != "TestStream" {
		t.Errorf("streamName = %v, want %q", reader.streamName, "TestStream")
	}
	if reader.tracker == nil {
		t.Error("tracker is nil")
	}
}

func TestPartitionReader_BuildStatement(t *testing.T) {
	t.Run("normal partition", func(t *testing.T) {
		reader := &partitionReader{
			partitionToken:  "child-token-1",
			startTimestamp:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTimestamp:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			heartbeatMillis: 10000,
			streamName:      "MyStream",
		}

		stmt := reader.buildStatement()

		wantSQL := "SELECT ChangeRecord FROM READ_MyStream (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)"
		if stmt.SQL != wantSQL {
			t.Errorf("SQL = %q, want %q", stmt.SQL, wantSQL)
		}
		if stmt.Params["partitionToken"] != "child-token-1" {
			t.Errorf("partitionToken = %v, want %q", stmt.Params["partitionToken"], "child-token-1")
		}
		if stmt.Params["heartbeatMilliseconds"] != int64(10000) {
			t.Errorf("heartbeatMilliseconds = %v, want 10000", stmt.Params["heartbeatMilliseconds"])
		}
	})

	t.Run("root partition uses nil token", func(t *testing.T) {
		reader := &partitionReader{
			partitionToken:  RootPartitionToken,
			startTimestamp:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			endTimestamp:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			heartbeatMillis: 10000,
			streamName:      "MyStream",
		}

		stmt := reader.buildStatement()

		if stmt.Params["partitionToken"] != nil {
			t.Errorf("partitionToken = %v, want nil", stmt.Params["partitionToken"])
		}
	})
}

func TestPartitionReader_ProcessWatermarks(t *testing.T) {
	t.Run("updates watermark via partition storage", func(t *testing.T) {
		var updatedToken string
		var updatedWatermark time.Time

		storage := &mockPartitionStorage{
			updateWatermarkFunc: func(ctx context.Context, token string, watermark time.Time) error {
				updatedToken = token
				updatedWatermark = watermark
				return nil
			},
		}

		tracker := newInflightTracker(10)
		reader := &partitionReader{
			partitionToken:   "token-1",
			partitionStorage: storage,
			tracker:          tracker,
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Run processWatermarks in the background.
		done := make(chan error, 1)
		go func() {
			done <- reader.processWatermarks(ctx)
		}()

		// Send a watermark.
		ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		tracker.watermarks <- ts

		// Wait briefly for the watermark to be processed.
		time.Sleep(50 * time.Millisecond)

		cancel()
		<-done

		if updatedToken != "token-1" {
			t.Errorf("token = %v, want %q", updatedToken, "token-1")
		}
		if !updatedWatermark.Equal(ts) {
			t.Errorf("watermark = %v, want %v", updatedWatermark, ts)
		}
	})

	t.Run("returns error when UpdateWatermark fails", func(t *testing.T) {
		updateErr := errors.New("storage error")
		storage := &mockPartitionStorage{
			updateWatermarkFunc: func(ctx context.Context, token string, watermark time.Time) error {
				return updateErr
			},
		}

		tracker := newInflightTracker(10)
		reader := &partitionReader{
			partitionToken:   "token-1",
			partitionStorage: storage,
			tracker:          tracker,
		}

		done := make(chan error, 1)
		go func() {
			done <- reader.processWatermarks(context.Background())
		}()

		tracker.watermarks <- time.Now()

		select {
		case err := <-done:
			if diff := cmp.Diff(updateErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("processWatermarks() error mismatch (-want +got):\n%s", diff)
			}
		case <-time.After(time.Second):
			t.Fatal("processWatermarks did not return")
		}
	})

	t.Run("returns nil when watermarks channel is closed", func(t *testing.T) {
		storage := &mockPartitionStorage{}
		tracker := newInflightTracker(10)
		reader := &partitionReader{
			partitionToken:   "token-1",
			partitionStorage: storage,
			tracker:          tracker,
		}

		done := make(chan error, 1)
		go func() {
			done <- reader.processWatermarks(context.Background())
		}()

		tracker.close()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("err = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("processWatermarks did not return")
		}
	})
}

func TestPartitionReader_ProcessWatermarks_UsesFlushCtx(t *testing.T) {
	// processWatermarks uses flushCtx for UpdateWatermark so that watermarks
	// are persisted even after the errgroup context (gctx) is canceled.
	var updatedWatermark time.Time
	storage := &mockPartitionStorage{
		updateWatermarkFunc: func(ctx context.Context, token string, watermark time.Time) error {
			updatedWatermark = watermark
			return nil
		},
	}

	tracker := newInflightTracker(10)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	defer flushCancel()

	reader := &partitionReader{
		partitionToken:   "token-1",
		partitionStorage: storage,
		tracker:          tracker,
		flushCtx:         flushCtx,
	}

	// Use a canceled context to simulate graceful shutdown (gctx canceled).
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- reader.processWatermarks(ctx)
	}()

	// Send a watermark while ctx is still active.
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	tracker.watermarks <- ts

	// Wait for the watermark to be processed.
	time.Sleep(50 * time.Millisecond)

	// Cancel ctx (simulates gctx cancellation during graceful shutdown).
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processWatermarks did not return")
	}

	if !updatedWatermark.Equal(ts) {
		t.Errorf("watermark = %v, want %v", updatedWatermark, ts)
	}
}

func TestPartitionReader_ProcessErrors(t *testing.T) {
	t.Run("returns consumer error", func(t *testing.T) {
		tracker := newInflightTracker(10)
		reader := &partitionReader{
			tracker: tracker,
		}

		done := make(chan error, 1)
		go func() {
			done <- reader.processErrors(context.Background())
		}()

		consumerErr := errors.New("consumer failed")
		tracker.errors <- consumerErr

		select {
		case err := <-done:
			if diff := cmp.Diff(consumerErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("processErrors() error mismatch (-want +got):\n%s", diff)
			}
		case <-time.After(time.Second):
			t.Fatal("processErrors did not return")
		}
	})

	t.Run("returns nil when errors channel is closed", func(t *testing.T) {
		tracker := newInflightTracker(10)
		reader := &partitionReader{
			tracker: tracker,
		}

		done := make(chan error, 1)
		go func() {
			done <- reader.processErrors(context.Background())
		}()

		tracker.close()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("err = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("processErrors did not return")
		}
	})

	t.Run("returns context error when canceled", func(t *testing.T) {
		tracker := newInflightTracker(10)
		reader := &partitionReader{
			tracker: tracker,
		}

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			done <- reader.processErrors(ctx)
		}()

		cancel()

		select {
		case err := <-done:
			if diff := cmp.Diff(context.Canceled, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("processErrors() error mismatch (-want +got):\n%s", diff)
			}
		case <-time.After(time.Second):
			t.Fatal("processErrors did not return")
		}
	})
}

func TestPartitionReader_ProcessHeartbeatRecord(t *testing.T) {
	tracker := newInflightTracker(10)
	reader := &partitionReader{
		tracker: tracker,
	}

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	reader.processHeartbeatRecord(&HeartbeatRecord{Timestamp: ts})

	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts) {
			t.Errorf("watermark = %v, want %v", watermark, ts)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update")
	}
}

func TestPartitionReader_ProcessChildPartitionsRecord(t *testing.T) {
	t.Run("persists child partitions and acks", func(t *testing.T) {
		var addedEndTimestamp time.Time
		var addedHeartbeat int64
		var addedRecord *ChildPartitionsRecord

		storage := &mockPartitionStorage{
			addChildPartitionsFunc: func(ctx context.Context, endTs time.Time, heartbeat int64, record *ChildPartitionsRecord) error {
				addedEndTimestamp = endTs
				addedHeartbeat = heartbeat
				addedRecord = record
				return nil
			},
		}

		tracker := newInflightTracker(10)
		endTs := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
		reader := &partitionReader{
			endTimestamp:     endTs,
			heartbeatMillis:  10000,
			partitionStorage: storage,
			tracker:          tracker,
		}

		startTs := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		record := &ChildPartitionsRecord{
			StartTimestamp: startTs,
			RecordSequence: "00000001",
			ChildPartitions: []*ChildPartition{
				{Token: "child-1", ParentPartitionTokens: []string{"parent-1"}},
			},
		}

		err := reader.processChildPartitionsRecord(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !addedEndTimestamp.Equal(endTs) {
			t.Errorf("endTimestamp = %v, want %v", addedEndTimestamp, endTs)
		}
		if addedHeartbeat != 10000 {
			t.Errorf("heartbeat = %v, want 10000", addedHeartbeat)
		}
		if addedRecord != record {
			t.Error("record mismatch")
		}

		// Watermark should be updated.
		select {
		case watermark := <-tracker.watermarks:
			if !watermark.Equal(startTs) {
				t.Errorf("watermark = %v, want %v", watermark, startTs)
			}
		case <-time.After(time.Second):
			t.Fatal("expected watermark update")
		}
	})

	t.Run("returns error when AddChildPartitions fails", func(t *testing.T) {
		storageErr := errors.New("storage error")
		storage := &mockPartitionStorage{
			addChildPartitionsFunc: func(ctx context.Context, endTs time.Time, heartbeat int64, record *ChildPartitionsRecord) error {
				return storageErr
			},
		}

		tracker := newInflightTracker(10)
		reader := &partitionReader{
			partitionStorage: storage,
			tracker:          tracker,
		}

		err := reader.processChildPartitionsRecord(context.Background(), &ChildPartitionsRecord{})
		if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("processChildPartitionsRecord() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestPartitionReader_ProcessDataChangeRecord(t *testing.T) {
	t.Run("processes record via consumer in goroutine", func(t *testing.T) {
		consumed := make(chan *DataChangeRecord, 1)
		consumer := ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error {
			consumed <- r
			return nil
		})

		tracker := newInflightTracker(10)
		reader := &partitionReader{
			consumer: consumer,
			tracker:  tracker,
		}

		commitTs := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		record := &dataChangeRecord{
			CommitTimestamp: commitTs,
			TableName:       "Users",
			ModType:         "INSERT",
		}

		err := reader.processDataChangeRecord(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Wait for Consumer to be called in the goroutine.
		select {
		case got := <-consumed:
			if got.TableName != "Users" {
				t.Errorf("TableName = %v, want %q", got.TableName, "Users")
			}
			if got.ModType != ModType_INSERT {
				t.Errorf("ModType = %v, want %v", got.ModType, ModType_INSERT)
			}
		case <-time.After(time.Second):
			t.Fatal("consumer was not called")
		}

		// Wait for the watermark to be updated.
		select {
		case watermark := <-tracker.watermarks:
			if !watermark.Equal(commitTs) {
				t.Errorf("watermark = %v, want %v", watermark, commitTs)
			}
		case <-time.After(time.Second):
			t.Fatal("expected watermark update")
		}
	})

	t.Run("returns error when acquire fails due to context cancellation", func(t *testing.T) {
		tracker := newInflightTracker(1)
		reader := &partitionReader{
			tracker: tracker,
		}

		// Exhaust the semaphore.
		if err := tracker.acquire(context.Background()); err != nil {
			t.Fatalf("initial acquire failed: %v", err)
		}
		tracker.add(time.Now())

		// Call with a canceled context.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := reader.processDataChangeRecord(ctx, &dataChangeRecord{CommitTimestamp: time.Now()})
		if diff := cmp.Diff(context.Canceled, err, cmpopts.EquateErrors()); diff != "" {
			t.Errorf("processDataChangeRecord() error mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestPartitionReader_ProcessRecords(t *testing.T) {
	t.Run("processes mixed record types", func(t *testing.T) {
		consumed := make(chan *DataChangeRecord, 1)
		consumer := ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error {
			consumed <- r
			return nil
		})

		var addedChildPartitions bool
		storage := &mockPartitionStorage{
			addChildPartitionsFunc: func(ctx context.Context, endTs time.Time, heartbeat int64, record *ChildPartitionsRecord) error {
				addedChildPartitions = true
				return nil
			},
		}

		tracker := newInflightTracker(10)
		reader := &partitionReader{
			consumer:         consumer,
			partitionStorage: storage,
			tracker:          tracker,
		}

		ts := time.Now()
		records := []*changeRecord{
			{
				DataChangeRecords: []*dataChangeRecord{
					{CommitTimestamp: ts, ModType: "INSERT"},
				},
				HeartbeatRecords: []*HeartbeatRecord{
					{Timestamp: ts.Add(time.Second)},
				},
				ChildPartitionsRecords: []*ChildPartitionsRecord{
					{
						StartTimestamp: ts.Add(2 * time.Second),
						ChildPartitions: []*ChildPartition{
							{Token: "child-1", ParentPartitionTokens: []string{"parent"}},
						},
					},
				},
			},
		}

		err := reader.processRecords(context.Background(), records)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Wait for Consumer to be called in the goroutine.
		select {
		case <-consumed:
			// Consumer was called successfully.
		case <-time.After(time.Second):
			t.Fatal("consumer was not called")
		}

		if !addedChildPartitions {
			t.Error("child partitions were not added")
		}
	})
}

func TestPartitionReader_ProcessRecords_DataChangeRecordError(t *testing.T) {
	// Test case where acquire fails with a canceled context.
	tracker := newInflightTracker(1)
	reader := &partitionReader{
		consumer: ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error { return nil }),
		tracker:  tracker,
	}

	// Exhaust the semaphore.
	if err := tracker.acquire(context.Background()); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}
	tracker.add(time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records := []*changeRecord{
		{
			DataChangeRecords: []*dataChangeRecord{
				{CommitTimestamp: time.Now(), ModType: "INSERT"},
			},
		},
	}

	err := reader.processRecords(ctx, records)
	if diff := cmp.Diff(context.Canceled, err, cmpopts.EquateErrors()); diff != "" {
		t.Errorf("processRecords() error mismatch (-want +got):\n%s", diff)
	}
}

func TestPartitionReader_ProcessRecords_ChildPartitionsError(t *testing.T) {
	storageErr := errors.New("storage error")
	storage := &mockPartitionStorage{
		addChildPartitionsFunc: func(ctx context.Context, endTs time.Time, heartbeat int64, record *ChildPartitionsRecord) error {
			return storageErr
		},
	}

	tracker := newInflightTracker(10)
	reader := &partitionReader{
		consumer:         ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error { return nil }),
		partitionStorage: storage,
		tracker:          tracker,
	}

	records := []*changeRecord{
		{
			ChildPartitionsRecords: []*ChildPartitionsRecord{
				{
					StartTimestamp:  time.Now(),
					ChildPartitions: []*ChildPartition{{Token: "child-1"}},
				},
			},
		},
	}

	err := reader.processRecords(context.Background(), records)
	if diff := cmp.Diff(storageErr, err, cmpopts.EquateErrors()); diff != "" {
		t.Errorf("processRecords() error mismatch (-want +got):\n%s", diff)
	}
}

func TestPartitionReader_DrainInflight(t *testing.T) {
	tracker := newInflightTracker(10)
	reader := &partitionReader{
		tracker: tracker,
	}

	// When no in-flight records exist, drain should complete immediately.
	done := make(chan struct{})
	go func() {
		reader.drainInflight()
		close(done)
	}()

	select {
	case <-done:
		// Completed successfully.
	case <-time.After(time.Second):
		t.Fatal("drainInflight timed out")
	}
}

func TestPartitionReader_Close(t *testing.T) {
	tracker := newInflightTracker(10)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	reader := &partitionReader{
		tracker:     tracker,
		flushCtx:    flushCtx,
		flushCancel: flushCancel,
	}

	// Calling close should close the tracker.
	reader.close()

	err := tracker.acquire(context.Background())
	if diff := cmp.Diff(errTrackerClosed, err, cmpopts.EquateErrors()); diff != "" {
		t.Errorf("acquire() error mismatch (-want +got):\n%s", diff)
	}
}

// #5: Watermarks of records completed during shutdown drain must be persisted.

func TestPartitionReader_FlushWatermarksDuringDrain(t *testing.T) {
	flushed := make(chan time.Time, 10)
	storage := &mockPartitionStorage{
		updateWatermarkFunc: func(ctx context.Context, token string, watermark time.Time) error {
			flushed <- watermark
			return nil
		},
	}

	tracker := newInflightTracker(10)
	flushCtx, flushCancel := context.WithCancel(context.Background())
	defer flushCancel()

	reader := &partitionReader{
		partitionToken:   "test-token",
		partitionStorage: storage,
		tracker:          tracker,
		flushCtx:         flushCtx,
		flushCancel:      flushCancel,
	}

	ctx := context.Background()
	ts0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(time.Second)

	_ = tracker.acquire(ctx)
	seq0 := tracker.add(ts0)
	_ = tracker.acquire(ctx)
	seq1 := tracker.add(ts1)

	// Start flushWatermarks concurrently (simulates graceful shutdown).
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		reader.flushWatermarks()
	}()

	// seq0 completes during drain. The watermark is flushed immediately.
	tracker.complete(seq0, nil)

	select {
	case wm := <-flushed:
		if !wm.Equal(ts0) {
			t.Errorf("first flushed watermark = %v, want %v", wm, ts0)
		}
	case <-time.After(time.Second):
		t.Fatal("watermark for seq0 was not flushed during drain")
	}

	// seq1 completes.
	tracker.complete(seq1, nil)

	select {
	case wm := <-flushed:
		if !wm.Equal(ts1) {
			t.Errorf("second flushed watermark = %v, want %v", wm, ts1)
		}
	case <-time.After(time.Second):
		t.Fatal("watermark for seq1 was not flushed during drain")
	}

	// Close the tracker to terminate flushWatermarks.
	tracker.close()
	<-flushDone
}

func TestPartitionReader_ConsumerContextSurvivesParentCancel(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	consumerCtxCh := make(chan context.Context, 1)
	consumer := ConsumerFunc(func(ctx context.Context, _ *DataChangeRecord) error {
		consumerCtxCh <- ctx
		<-ctx.Done()
		return ctx.Err()
	})

	partition := &PartitionMetadata{PartitionToken: "test"}
	reader := newPartitionReader(parentCtx, partition, nil, "test", &mockPartitionStorage{}, consumer, 10)

	record := &dataChangeRecord{CommitTimestamp: time.Now(), ModType: "INSERT"}
	if err := reader.processDataChangeRecord(context.Background(), record); err != nil {
		t.Fatalf("processDataChangeRecord failed: %v", err)
	}

	// Get the context passed to Consumer.
	var cctx context.Context
	select {
	case cctx = <-consumerCtxCh:
	case <-time.After(time.Second):
		t.Fatal("Consumer was not called")
	}

	// Cancel the parent context (simulates Shutdown calling s.cancel).
	parentCancel()

	// Consumer's context should NOT be canceled because flushCtx uses WithoutCancel.
	select {
	case <-cctx.Done():
		t.Fatal("Consumer context should not be canceled when parent context is canceled")
	case <-time.After(50 * time.Millisecond):
	}

	// close() the reader (simulates Close).
	reader.close()

	// Consumer's context should now be canceled.
	select {
	case <-cctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Consumer context should be canceled after close()")
	}
}

func TestPartitionReader_FlushCtxInheritsValues(t *testing.T) {
	type ctxKey struct{}
	parentCtx := context.WithValue(context.Background(), ctxKey{}, "test-value")

	partition := &PartitionMetadata{PartitionToken: "test"}
	reader := newPartitionReader(parentCtx, partition, nil, "test", &mockPartitionStorage{}, nil, 1)
	defer reader.close()

	if v := reader.flushCtx.Value(ctxKey{}); v != "test-value" {
		t.Errorf("flushCtx.Value = %v, want %q", v, "test-value")
	}
}

func TestPartitionReader_FlushWatermarksAbortedByClose(t *testing.T) {
	// Close() is called while A and B are completed but C is still in-flight.
	// Watermarks of A and B must be persisted before Close() returns.

	flushed := make(chan time.Time, 10)
	storage := &mockPartitionStorage{
		updateWatermarkFunc: func(ctx context.Context, token string, watermark time.Time) error {
			flushed <- watermark
			return nil
		},
	}

	tracker := newInflightTracker(10)
	flushCtx, flushCancel := context.WithCancel(context.Background())

	reader := &partitionReader{
		partitionToken:   "test-token",
		partitionStorage: storage,
		tracker:          tracker,
		flushCtx:         flushCtx,
		flushCancel:      flushCancel,
	}

	ctx := context.Background()
	ts0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(time.Second)

	_ = tracker.acquire(ctx)
	seq0 := tracker.add(ts0)
	_ = tracker.acquire(ctx)
	seq1 := tracker.add(ts1)
	_ = tracker.acquire(ctx)
	_ = tracker.add(ts1.Add(time.Second)) // seq2: never completes (slow consumer)

	// Start flushWatermarks.
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		reader.flushWatermarks()
	}()

	// A (seq0) completes.
	tracker.complete(seq0, nil)
	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("watermark for seq0 was not flushed")
	}

	// B (seq1) completes.
	tracker.complete(seq1, nil)
	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("watermark for seq1 was not flushed")
	}

	// Simulate Close(): abort while seq2 is still in-flight.
	reader.close()
	<-flushDone

	// Watermarks of A and B are persisted before Close() returns.
}
