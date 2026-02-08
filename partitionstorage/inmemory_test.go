package partitionstorage

import (
	"context"
	"testing"
	"time"

	"github.com/toga4/spream"
)

func TestNewInmemory(t *testing.T) {
	storage := NewInmemory()
	if storage == nil {
		t.Fatal("NewInmemory() returned nil")
	}
	if storage.m == nil {
		t.Fatal("internal map is nil")
	}
}

func TestInmemoryPartitionStorage_InitializeRootPartition(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	startTs := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTs := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
	heartbeat := 10 * time.Second

	err := storage.InitializeRootPartition(ctx, startTs, endTs, heartbeat)
	if err != nil {
		t.Fatalf("InitializeRootPartition failed: %v", err)
	}

	// ルートパーティションが正しく作成されたか確認する。
	p := storage.m[spream.RootPartitionToken]
	if p == nil {
		t.Fatal("root partition was not created")
	}
	if p.PartitionToken != spream.RootPartitionToken {
		t.Errorf("PartitionToken = %v, want %q", p.PartitionToken, spream.RootPartitionToken)
	}
	if !p.StartTimestamp.Equal(startTs) {
		t.Errorf("StartTimestamp = %v, want %v", p.StartTimestamp, startTs)
	}
	if !p.EndTimestamp.Equal(endTs) {
		t.Errorf("EndTimestamp = %v, want %v", p.EndTimestamp, endTs)
	}
	if p.HeartbeatMillis != heartbeat.Milliseconds() {
		t.Errorf("HeartbeatMillis = %v, want %v", p.HeartbeatMillis, heartbeat.Milliseconds())
	}
	if p.State != spream.StateCreated {
		t.Errorf("State = %v, want %v", p.State, spream.StateCreated)
	}
	if !p.Watermark.Equal(startTs) {
		t.Errorf("Watermark = %v, want %v", p.Watermark, startTs)
	}
	if len(p.ParentTokens) != 0 {
		t.Errorf("len(ParentTokens) = %d, want 0", len(p.ParentTokens))
	}
}

func TestInmemoryPartitionStorage_GetUnfinishedMinWatermarkPartition(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil when no partitions", func(t *testing.T) {
		storage := NewInmemory()

		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got = %v, want nil", got)
		}
	})

	t.Run("returns nil when all partitions are finished", func(t *testing.T) {
		storage := NewInmemory()
		storage.m["token-1"] = &spream.PartitionMetadata{
			PartitionToken: "token-1",
			State:          spream.StateFinished,
			Watermark:      time.Now(),
		}

		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("got = %v, want nil", got)
		}
	})

	t.Run("returns partition with minimum watermark", func(t *testing.T) {
		storage := NewInmemory()
		ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		ts2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

		storage.m["token-1"] = &spream.PartitionMetadata{
			PartitionToken: "token-1",
			State:          spream.StateRunning,
			Watermark:      ts2,
		}
		storage.m["token-2"] = &spream.PartitionMetadata{
			PartitionToken: "token-2",
			State:          spream.StateCreated,
			Watermark:      ts1,
		}
		storage.m["token-3"] = &spream.PartitionMetadata{
			PartitionToken: "token-3",
			State:          spream.StateFinished,
			Watermark:      ts1.Add(-time.Hour), // FINISHED なので無視されるはず。
		}

		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("got nil, want partition")
		}
		if got.PartitionToken != "token-2" {
			t.Errorf("PartitionToken = %v, want %q", got.PartitionToken, "token-2")
		}
	})
}

func TestInmemoryPartitionStorage_GetInterruptedPartitions(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	// InmemoryPartitionStorage は常に nil を返す。
	got, err := storage.GetInterruptedPartitions(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil", got)
	}
}

func TestInmemoryPartitionStorage_GetSchedulablePartitions(t *testing.T) {
	ctx := context.Background()

	t.Run("returns created partitions where minWatermark does not exceed start timestamp", func(t *testing.T) {
		storage := NewInmemory()
		ts := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

		storage.m["token-1"] = &spream.PartitionMetadata{
			PartitionToken: "token-1",
			State:          spream.StateCreated,
			StartTimestamp:  ts, // minWatermark(ts) <= ts なのでスケジュール可能。
		}
		storage.m["token-2"] = &spream.PartitionMetadata{
			PartitionToken: "token-2",
			State:          spream.StateCreated,
			StartTimestamp:  ts.Add(-time.Hour), // minWatermark(ts) > ts-1h なのでスケジュール不可。
		}
		storage.m["token-3"] = &spream.PartitionMetadata{
			PartitionToken: "token-3",
			State:          spream.StateScheduled, // CREATED でないのでスケジュール不可。
			StartTimestamp:  ts,
		}

		got, err := storage.GetSchedulablePartitions(ctx, ts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		if got[0].PartitionToken != "token-1" {
			t.Errorf("PartitionToken = %v, want %q", got[0].PartitionToken, "token-1")
		}
	})

	t.Run("returns empty when no schedulable partitions", func(t *testing.T) {
		storage := NewInmemory()

		got, err := storage.GetSchedulablePartitions(ctx, time.Now())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("len(got) = %d, want 0", len(got))
		}
	})
}

func TestInmemoryPartitionStorage_AddChildPartitions(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	endTs := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
	startTs := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	record := &spream.ChildPartitionsRecord{
		StartTimestamp: startTs,
		RecordSequence: "00000001",
		ChildPartitions: []*spream.ChildPartition{
			{Token: "child-1", ParentPartitionTokens: []string{"parent-1"}},
			{Token: "child-2", ParentPartitionTokens: []string{"parent-1", "parent-2"}},
		},
	}

	err := storage.AddChildPartitions(ctx, endTs, 10000, record)
	if err != nil {
		t.Fatalf("AddChildPartitions failed: %v", err)
	}

	if len(storage.m) != 2 {
		t.Fatalf("len(m) = %d, want 2", len(storage.m))
	}

	child1 := storage.m["child-1"]
	if child1 == nil {
		t.Fatal("child-1 not found")
	}
	if child1.PartitionToken != "child-1" {
		t.Errorf("child1.PartitionToken = %v, want %q", child1.PartitionToken, "child-1")
	}
	if len(child1.ParentTokens) != 1 || child1.ParentTokens[0] != "parent-1" {
		t.Errorf("child1.ParentTokens = %v, want [parent-1]", child1.ParentTokens)
	}
	if !child1.StartTimestamp.Equal(startTs) {
		t.Errorf("child1.StartTimestamp = %v, want %v", child1.StartTimestamp, startTs)
	}
	if !child1.EndTimestamp.Equal(endTs) {
		t.Errorf("child1.EndTimestamp = %v, want %v", child1.EndTimestamp, endTs)
	}
	if child1.HeartbeatMillis != 10000 {
		t.Errorf("child1.HeartbeatMillis = %v, want 10000", child1.HeartbeatMillis)
	}
	if child1.State != spream.StateCreated {
		t.Errorf("child1.State = %v, want %v", child1.State, spream.StateCreated)
	}
	if !child1.Watermark.Equal(startTs) {
		t.Errorf("child1.Watermark = %v, want %v", child1.Watermark, startTs)
	}

	child2 := storage.m["child-2"]
	if child2 == nil {
		t.Fatal("child-2 not found")
	}
	if len(child2.ParentTokens) != 2 {
		t.Errorf("len(child2.ParentTokens) = %d, want 2", len(child2.ParentTokens))
	}
}

func TestInmemoryPartitionStorage_StateTransitions(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	startTs := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTs := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	// ルートパーティションを初期化する。
	err := storage.InitializeRootPartition(ctx, startTs, endTs, 10*time.Second)
	if err != nil {
		t.Fatalf("InitializeRootPartition failed: %v", err)
	}

	token := spream.RootPartitionToken

	// CREATED 状態を確認する。
	p := storage.m[token]
	if p.State != spream.StateCreated {
		t.Fatalf("initial State = %v, want %v", p.State, spream.StateCreated)
	}

	// SCHEDULED に遷移する。
	err = storage.UpdateToScheduled(ctx, []string{token})
	if err != nil {
		t.Fatalf("UpdateToScheduled failed: %v", err)
	}
	if p.State != spream.StateScheduled {
		t.Errorf("State = %v, want %v", p.State, spream.StateScheduled)
	}
	if p.ScheduledAt == nil {
		t.Error("ScheduledAt is nil")
	}

	// RUNNING に遷移する。
	err = storage.UpdateToRunning(ctx, token)
	if err != nil {
		t.Fatalf("UpdateToRunning failed: %v", err)
	}
	if p.State != spream.StateRunning {
		t.Errorf("State = %v, want %v", p.State, spream.StateRunning)
	}
	if p.RunningAt == nil {
		t.Error("RunningAt is nil")
	}

	// ウォーターマークを更新する。
	newWatermark := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	err = storage.UpdateWatermark(ctx, token, newWatermark)
	if err != nil {
		t.Fatalf("UpdateWatermark failed: %v", err)
	}
	if !p.Watermark.Equal(newWatermark) {
		t.Errorf("Watermark = %v, want %v", p.Watermark, newWatermark)
	}

	// FINISHED に遷移する。
	err = storage.UpdateToFinished(ctx, token)
	if err != nil {
		t.Fatalf("UpdateToFinished failed: %v", err)
	}
	if p.State != spream.StateFinished {
		t.Errorf("State = %v, want %v", p.State, spream.StateFinished)
	}
	if p.FinishedAt == nil {
		t.Error("FinishedAt is nil")
	}
}

func TestInmemoryPartitionStorage_UpdateToScheduled_MultipleTokens(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	storage.m["token-1"] = &spream.PartitionMetadata{
		PartitionToken: "token-1",
		State:          spream.StateCreated,
	}
	storage.m["token-2"] = &spream.PartitionMetadata{
		PartitionToken: "token-2",
		State:          spream.StateCreated,
	}

	err := storage.UpdateToScheduled(ctx, []string{"token-1", "token-2"})
	if err != nil {
		t.Fatalf("UpdateToScheduled failed: %v", err)
	}

	for _, token := range []string{"token-1", "token-2"} {
		p := storage.m[token]
		if p.State != spream.StateScheduled {
			t.Errorf("%s: State = %v, want %v", token, p.State, spream.StateScheduled)
		}
		if p.ScheduledAt == nil {
			t.Errorf("%s: ScheduledAt is nil", token)
		}
	}
}
