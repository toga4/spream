package spream

import (
	"context"
	"errors"
	"testing"
	"time"
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

	reader := newPartitionReader(partition, nil, "TestStream", storage, consumer, 5)

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

		// processWatermarks をバックグラウンドで実行する。
		done := make(chan error, 1)
		go func() {
			done <- reader.processWatermarks(ctx)
		}()

		// ウォーターマークを送信する。
		ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		tracker.watermarks <- ts

		// ウォーターマークが処理されるまで少し待つ。
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
			if !errors.Is(err, updateErr) {
				t.Errorf("err = %v, want %v", err, updateErr)
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
			if !errors.Is(err, consumerErr) {
				t.Errorf("err = %v, want %v", err, consumerErr)
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
			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
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

		// ウォーターマークが更新されるはず。
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
		if !errors.Is(err, storageErr) {
			t.Errorf("err = %v, want %v", err, storageErr)
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
			TableName:      "Users",
			ModType:        "INSERT",
		}

		err := reader.processDataChangeRecord(context.Background(), record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Consumer がゴルーチンで呼ばれるのを待つ。
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

		// ウォーターマークが更新されるのを待つ。
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

		// セマフォを使い切る。
		if err := tracker.acquire(context.Background()); err != nil {
			t.Fatalf("initial acquire failed: %v", err)
		}
		tracker.add(time.Now())

		// キャンセル済みコンテキストで呼び出す。
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := reader.processDataChangeRecord(ctx, &dataChangeRecord{CommitTimestamp: time.Now()})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
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

		// Consumer がゴルーチンで呼ばれるのを待つ。
		select {
		case <-consumed:
			// 正常に Consumer が呼ばれた。
		case <-time.After(time.Second):
			t.Fatal("consumer was not called")
		}

		if !addedChildPartitions {
			t.Error("child partitions were not added")
		}
	})
}

func TestPartitionReader_ProcessRecords_DataChangeRecordError(t *testing.T) {
	// acquire がキャンセル済みコンテキストで失敗するケース。
	tracker := newInflightTracker(1)
	reader := &partitionReader{
		consumer: ConsumerFunc(func(ctx context.Context, r *DataChangeRecord) error { return nil }),
		tracker:  tracker,
	}

	// セマフォを使い切る。
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
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
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
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want %v", err, storageErr)
	}
}

func TestPartitionReader_DrainInflight(t *testing.T) {
	tracker := newInflightTracker(10)
	reader := &partitionReader{
		tracker: tracker,
	}

	// インフライトレコードがない場合、drain は即座に完了するはず。
	done := make(chan struct{})
	go func() {
		reader.drainInflight()
		close(done)
	}()

	select {
	case <-done:
		// 正常完了。
	case <-time.After(time.Second):
		t.Fatal("drainInflight timed out")
	}
}

func TestPartitionReader_Close(t *testing.T) {
	tracker := newInflightTracker(10)
	reader := &partitionReader{
		tracker: tracker,
	}

	// close を呼ぶとトラッカーが閉じるはず。
	reader.close()

	err := tracker.acquire(context.Background())
	if err != errTrackerClosed {
		t.Errorf("err = %v, want errTrackerClosed", err)
	}
}
