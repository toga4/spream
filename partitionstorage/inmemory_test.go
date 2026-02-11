package partitionstorage

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

	// Verify the root partition was created correctly.
	got := storage.m[spream.RootPartitionToken]
	if got == nil {
		t.Fatal("root partition was not created")
	}

	want := &spream.PartitionMetadata{
		PartitionToken:  spream.RootPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  startTs,
		EndTimestamp:    endTs,
		HeartbeatMillis: heartbeat.Milliseconds(),
		State:           spream.StateCreated,
		Watermark:       startTs,
	}
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(spream.PartitionMetadata{}, "CreatedAt")); diff != "" {
		t.Errorf("root partition mismatch (-want +got):\n%s", diff)
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
			Watermark:      ts1.Add(-time.Hour), // Should be ignored because it is FINISHED.
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

	// InmemoryPartitionStorage always returns nil.
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
			StartTimestamp: ts, // Schedulable because minWatermark(ts) <= ts.
		}
		storage.m["token-2"] = &spream.PartitionMetadata{
			PartitionToken: "token-2",
			State:          spream.StateCreated,
			StartTimestamp: ts.Add(-time.Hour), // Not schedulable because minWatermark(ts) > ts-1h.
		}
		storage.m["token-3"] = &spream.PartitionMetadata{
			PartitionToken: "token-3",
			State:          spream.StateScheduled, // Not schedulable because state is not CREATED.
			StartTimestamp: ts,
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

	ignoreCreatedAt := cmpopts.IgnoreFields(spream.PartitionMetadata{}, "CreatedAt")

	gotChild1 := storage.m["child-1"]
	if gotChild1 == nil {
		t.Fatal("child-1 not found")
	}
	wantChild1 := &spream.PartitionMetadata{
		PartitionToken:  "child-1",
		ParentTokens:    []string{"parent-1"},
		StartTimestamp:  startTs,
		EndTimestamp:    endTs,
		HeartbeatMillis: 10000,
		State:           spream.StateCreated,
		Watermark:       startTs,
	}
	if diff := cmp.Diff(wantChild1, gotChild1, ignoreCreatedAt); diff != "" {
		t.Errorf("child-1 mismatch (-want +got):\n%s", diff)
	}

	gotChild2 := storage.m["child-2"]
	if gotChild2 == nil {
		t.Fatal("child-2 not found")
	}
	wantChild2 := &spream.PartitionMetadata{
		PartitionToken:  "child-2",
		ParentTokens:    []string{"parent-1", "parent-2"},
		StartTimestamp:  startTs,
		EndTimestamp:    endTs,
		HeartbeatMillis: 10000,
		State:           spream.StateCreated,
		Watermark:       startTs,
	}
	if diff := cmp.Diff(wantChild2, gotChild2, ignoreCreatedAt); diff != "" {
		t.Errorf("child-2 mismatch (-want +got):\n%s", diff)
	}
}

func TestInmemoryPartitionStorage_StateTransitions(t *testing.T) {
	ctx := context.Background()
	storage := NewInmemory()

	startTs := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTs := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	// Initialize the root partition.
	err := storage.InitializeRootPartition(ctx, startTs, endTs, 10*time.Second)
	if err != nil {
		t.Fatalf("InitializeRootPartition failed: %v", err)
	}

	token := spream.RootPartitionToken

	// Verify the state is CREATED.
	p := storage.m[token]
	if p.State != spream.StateCreated {
		t.Fatalf("initial State = %v, want %v", p.State, spream.StateCreated)
	}

	// Transition to SCHEDULED.
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

	// Transition to RUNNING.
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

	// Update the watermark.
	newWatermark := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	err = storage.UpdateWatermark(ctx, token, newWatermark)
	if err != nil {
		t.Fatalf("UpdateWatermark failed: %v", err)
	}
	if !p.Watermark.Equal(newWatermark) {
		t.Errorf("Watermark = %v, want %v", p.Watermark, newWatermark)
	}

	// Transition to FINISHED.
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
