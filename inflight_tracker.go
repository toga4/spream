package spream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// inflightTracker tracks in-flight records within a partition.
type inflightTracker struct {
	// mu protects sequence management and uncommitted records.
	mu sync.Mutex

	// sendMu synchronizes channel sends and close operations.
	// Senders use RLock (concurrent), close uses Lock (exclusive).
	sendMu sync.RWMutex

	// Semaphore for backpressure control.
	sem *semaphore.Weighted

	// Sequence management.
	nextSeq int64

	// State tracking (acked flag integrated into uncommitted).
	uncommitted map[int64]*uncommittedRecord

	// Continuous ack tracking.
	lastContinuousAcked int64

	// Number of unresolved records (decremented on ack or error).
	// errored records remain in uncommitted to block watermark advancement,
	// so len(uncommitted) cannot be used for drain completion.
	pending int

	// Event notification channels (separated for normal and error cases).
	watermarks chan time.Time
	errors     chan error

	// Stop accepting new records (blocks acquire).
	closed atomic.Bool

	// Graceful shutdown.
	// When draining=true and pending reaches 0, done is closed.
	draining atomic.Bool

	// Drain signaling: closeDone closes done when all in-flight records complete.
	done      chan struct{}
	closeDone func()
}

type uncommittedRecord struct {
	seq       int64
	timestamp time.Time
	acked     bool
	// errored marks Consumer failure. The record stays in uncommitted
	// so that advanceWatermark stops here instead of skipping past it.
	errored bool
}

// newInflightTracker creates a new inflightTracker.
func newInflightTracker(maxInflight int) *inflightTracker {
	t := &inflightTracker{
		sem:                 semaphore.NewWeighted(int64(maxInflight)),
		uncommitted:         make(map[int64]*uncommittedRecord),
		lastContinuousAcked: -1, // Sequence numbers start from 0.
		watermarks:          make(chan time.Time, maxInflight),
		errors:              make(chan error, maxInflight),
		done:                make(chan struct{}),
	}
	// closeDone may be called from multiple code paths; OnceFunc prevents double-close panic on the channel.
	t.closeDone = sync.OnceFunc(func() { close(t.done) })
	return t
}

// sendWatermark sends a watermark to the channel. It does nothing if watermark is zero.
// Non-blocking: if the channel is full (consumer exited or slow), the watermark is dropped.
// Dropping watermarks is safe because the at-least-once guarantee allows re-processing
// from the last committed watermark on restart.
func (t *inflightTracker) sendWatermark(watermark time.Time) {
	if watermark.IsZero() {
		return
	}
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if !t.closed.Load() {
		select {
		case t.watermarks <- watermark:
		default:
		}
	}
}

// sendError sends an error to the channel.
// Non-blocking: if the channel is full, the error is dropped.
// The first error triggers errgroup cancellation, so subsequent errors are not critical.
func (t *inflightTracker) sendError(err error) {
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if !t.closed.Load() {
		select {
		case t.errors <- err:
		default:
		}
	}
}

// acquire acquires the semaphore (MaxInflight control).
// Blocks if in-flight count has reached maxInflight.
// semaphore.Weighted handles context cancellation automatically.
func (t *inflightTracker) acquire(ctx context.Context) error {
	if t.closed.Load() {
		return errTrackerClosed
	}
	return t.sem.Acquire(ctx, 1)
}

// add adds a new record to in-flight and returns the sequence number.
func (t *inflightTracker) add(timestamp time.Time) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeq
	t.nextSeq++

	t.uncommitted[seq] = &uncommittedRecord{
		seq:       seq,
		timestamp: timestamp,
	}
	t.pending++

	return seq
}

// complete notifies the completion of record processing and emits events as needed.
// This function is called from goroutines (for DataChangeRecord).
func (t *inflightTracker) complete(seq int64, err error) {
	if err != nil {
		t.mu.Lock()
		t.uncommitted[seq].errored = true
		t.pending--
		if t.draining.Load() && t.pending == 0 {
			t.closeDone()
		}
		t.mu.Unlock()
		t.sem.Release(1)
		t.sendError(err)
		return
	}

	watermark := t.completeRecord(seq)
	t.sendWatermark(watermark)
}

// completeRecord marks the record as acked and returns the updated watermark.
func (t *inflightTracker) completeRecord(seq int64) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer t.sem.Release(1)

	t.pending--
	if rec, ok := t.uncommitted[seq]; ok {
		rec.acked = true
	}
	return t.advanceWatermark()
}

// ackImmediate is for records that ack immediately (HeartbeatRecord, ChildPartitionsRecord).
// Does not use semaphore (no goroutine is spawned).
func (t *inflightTracker) ackImmediate(timestamp time.Time) {
	watermark := t.ackImmediateRecord(timestamp)
	t.sendWatermark(watermark)
}

// ackImmediateRecord adds a record as immediately acked and returns the updated watermark.
func (t *inflightTracker) ackImmediateRecord(timestamp time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeq
	t.nextSeq++

	t.uncommitted[seq] = &uncommittedRecord{
		seq:       seq,
		timestamp: timestamp,
		acked:     true, // Immediately acked.
	}

	return t.advanceWatermark()
}

// advanceWatermark calculates continuous ack and advances the watermark.
// Must be called while holding mu.Lock().
// Returns: updated watermark (zero value if no update).
//
// Errored records block watermark advancement to preserve at-least-once semantics.
// The watermark stops at the first errored record, preventing subsequent acked records
// from advancing the watermark past unprocessed data.
func (t *inflightTracker) advanceWatermark() time.Time {
	var watermark time.Time

	for {
		nextExpected := t.lastContinuousAcked + 1
		if nextExpected >= t.nextSeq {
			break // All assigned sequences have been processed.
		}

		rec, ok := t.uncommitted[nextExpected]
		if !ok {
			break
		}
		if rec.errored {
			break
		}
		if !rec.acked {
			break
		}

		watermark = rec.timestamp

		t.lastContinuousAcked = nextExpected
		delete(t.uncommitted, nextExpected)
	}

	// Signal drain completion when all pending records are resolved.
	if t.draining.Load() && t.pending == 0 {
		t.closeDone()
	}

	return watermark
}

// drain initiates graceful shutdown and waits until all in-flight records are completed.
// Call after readStream() has finished.
func (t *inflightTracker) drain() {
	t.draining.Store(true)

	// All records may already be completed before drain; signal completion immediately in that case.
	t.mu.Lock()
	if t.pending == 0 {
		t.closeDone()
	}
	t.mu.Unlock()

	<-t.done
}

// close closes the tracker.
// This method is idempotent; calling it multiple times is safe.
func (t *inflightTracker) close() {
	// Use sendMu to ensure no goroutine is sending to channels.
	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	if t.closed.Swap(true) {
		return // Already closed.
	}
	close(t.watermarks)
	close(t.errors)
	t.closeDone()
}
