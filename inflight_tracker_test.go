package spream

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInflightTracker_BasicFlow(t *testing.T) {
	tracker := newInflightTracker(3)
	ctx := context.Background()

	// Acquire should succeed.
	if err := tracker.acquire(ctx); err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	// Add a record.
	ts := time.Now()
	seq := tracker.add(ts)
	if seq != 0 {
		t.Fatalf("expected seq 0, got %d", seq)
	}

	// Complete the record.
	tracker.complete(seq, nil)

	// Watermark should be updated.
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts) {
			t.Fatalf("expected watermark %v, got %v", ts, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update")
	}

	tracker.close()
}

func TestInflightTracker_ContinuousAck(t *testing.T) {
	tracker := newInflightTracker(10)
	ctx := context.Background()

	// Add records 0, 1, 2.
	ts0 := time.Now()
	ts1 := ts0.Add(time.Second)
	ts2 := ts0.Add(2 * time.Second)

	tracker.acquire(ctx)
	seq0 := tracker.add(ts0)
	tracker.acquire(ctx)
	seq1 := tracker.add(ts1)
	tracker.acquire(ctx)
	seq2 := tracker.add(ts2)

	// Complete in order: 2, 0, 1.
	// After completing 2: no watermark update (0 and 1 still pending).
	tracker.complete(seq2, nil)
	select {
	case <-tracker.watermarks:
		t.Fatal("unexpected watermark update after completing seq 2")
	default:
	}

	// After completing 0: watermark should be ts0.
	tracker.complete(seq0, nil)
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts0) {
			t.Fatalf("expected watermark %v, got %v", ts0, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update after completing seq 0")
	}

	// After completing 1: watermark should be ts2 (0, 1, 2 all acked).
	tracker.complete(seq1, nil)
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts2) {
			t.Fatalf("expected watermark %v, got %v", ts2, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update after completing seq 1")
	}

	tracker.close()
}

func TestInflightTracker_AckImmediate(t *testing.T) {
	tracker := newInflightTracker(3)

	ts := time.Now()
	tracker.ackImmediate(ts)

	// Watermark should be updated immediately.
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts) {
			t.Fatalf("expected watermark %v, got %v", ts, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update")
	}

	tracker.close()
}

func TestInflightTracker_Error(t *testing.T) {
	tracker := newInflightTracker(3)
	ctx := context.Background()

	tracker.acquire(ctx)
	seq := tracker.add(time.Now())

	// Complete with error.
	testErr := context.DeadlineExceeded
	tracker.complete(seq, testErr)

	// Error should be sent to errors channel.
	select {
	case err := <-tracker.errors:
		if err != testErr {
			t.Fatalf("expected error %v, got %v", testErr, err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected error")
	}

	// No watermark update should occur.
	select {
	case <-tracker.watermarks:
		t.Fatal("unexpected watermark update on error")
	default:
	}

	tracker.close()
}

func TestInflightTracker_Backpressure(t *testing.T) {
	tracker := newInflightTracker(2)
	ctx := context.Background()

	// Acquire 2 slots.
	tracker.acquire(ctx)
	tracker.add(time.Now())
	tracker.acquire(ctx)
	tracker.add(time.Now())

	// Third acquire should block.
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := tracker.acquire(ctxWithTimeout)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	tracker.close()
}

func TestInflightTracker_GracefulShutdown(t *testing.T) {
	tracker := newInflightTracker(10)
	ctx := context.Background()

	// Add some records.
	tracker.acquire(ctx)
	seq0 := tracker.add(time.Now())
	tracker.acquire(ctx)
	seq1 := tracker.add(time.Now().Add(time.Second))

	// Initiate shutdown.
	tracker.initiateShutdown()

	// Complete records.
	tracker.complete(seq0, nil)
	<-tracker.watermarks

	tracker.complete(seq1, nil)
	<-tracker.watermarks

	// Wait for all to complete.
	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()

	err := tracker.waitAllCompleted(waitCtx)
	if err != nil {
		t.Fatalf("waitAllCompleted failed: %v", err)
	}

	tracker.close()
}

func TestInflightTracker_Closed(t *testing.T) {
	tracker := newInflightTracker(3)
	tracker.close()

	// Acquire should fail on closed tracker.
	err := tracker.acquire(context.Background())
	if err != errTrackerClosed {
		t.Fatalf("expected errTrackerClosed, got %v", err)
	}
}

func TestInflightTracker_ConcurrentAccess(t *testing.T) {
	tracker := newInflightTracker(100)
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 50

	// Launch concurrent goroutines.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := tracker.acquire(ctx); err != nil {
				return
			}
			seq := tracker.add(time.Now().Add(time.Duration(i) * time.Millisecond))
			time.Sleep(time.Millisecond)
			tracker.complete(seq, nil)
		}(i)
	}

	wg.Wait()

	// Drain watermarks.
	tracker.initiateShutdown()

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()

	if err := tracker.waitAllCompleted(waitCtx); err != nil {
		t.Fatalf("waitAllCompleted failed: %v", err)
	}

	tracker.close()
}

func TestInflightTracker_MixedAck(t *testing.T) {
	tracker := newInflightTracker(10)
	ctx := context.Background()

	// Mix of DataChangeRecord (acquire + add + complete) and HeartbeatRecord (ackImmediate).
	ts0 := time.Now()
	ts1 := ts0.Add(time.Second)
	ts2 := ts0.Add(2 * time.Second)
	ts3 := ts0.Add(3 * time.Second)

	// DataChangeRecord at ts0.
	tracker.acquire(ctx)
	seq0 := tracker.add(ts0)

	// HeartbeatRecord at ts1.
	tracker.ackImmediate(ts1)

	// DataChangeRecord at ts2.
	tracker.acquire(ctx)
	seq2 := tracker.add(ts2)

	// HeartbeatRecord at ts3.
	tracker.ackImmediate(ts3)

	// At this point:
	// - seq0 (ts0): pending
	// - seq1 (ts1): acked (heartbeat)
	// - seq2 (ts2): pending
	// - seq3 (ts3): acked (heartbeat)
	// No watermark update yet because seq0 is pending.

	// Complete seq0.
	tracker.complete(seq0, nil)

	// Watermark should advance to ts1 (continuous ack: seq0, seq1).
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts1) {
			t.Fatalf("expected watermark %v, got %v", ts1, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update")
	}

	// Complete seq2.
	tracker.complete(seq2, nil)

	// Watermark should advance to ts3 (continuous ack: seq0, seq1, seq2, seq3).
	select {
	case watermark := <-tracker.watermarks:
		if !watermark.Equal(ts3) {
			t.Fatalf("expected watermark %v, got %v", ts3, watermark)
		}
	case <-time.After(time.Second):
		t.Fatal("expected watermark update")
	}

	tracker.close()
}
