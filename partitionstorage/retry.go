package partitionstorage

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

// RetryPolicy controls how SpannerPartitionStorage retries
// transient Spanner errors on write operations (Apply).
// Read operations are not retried here because
// the Spanner client already handles transient read errors internally.
type RetryPolicy struct {
	MaxAttempts  int // Total attempts including the first call. 0 or 1 means no retry.
	MinBackoff   time.Duration
	MaxBackoff   time.Duration // If MinBackoff > MaxBackoff, MinBackoff is used as the cap.
	Multiplier   float64       // Exponential multiplier. Values < 1.0 are treated as 1.0.
	JitterFactor float64       // Fraction of backoff to randomize (0.0–1.0).
	rng          *rand.Rand    // nil uses the global rand source.
}

// defaultRetryPolicy is the retry policy applied when no WithRetryPolicy option is given.
var defaultRetryPolicy = RetryPolicy{
	MaxAttempts:  5,
	MinBackoff:   100 * time.Millisecond,
	MaxBackoff:   5 * time.Second,
	Multiplier:   2.0,
	JitterFactor: 0.2,
}

// attempts returns a range-over-func iterator that yields up to MaxAttempts times.
// The first yield has no delay; subsequent yields sleep with exponential backoff.
// If ctx is canceled during sleep, the iterator stops without yielding.
func (p RetryPolicy) attempts(ctx context.Context) func(yield func() bool) {
	return func(yield func() bool) {
		for i := range max(p.MaxAttempts, 1) {
			if i > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(p.backoff(i)):
				}
			}
			if !yield() {
				return
			}
		}
	}
}

// backoff calculates the delay for the given attempt (0-indexed, where attempt >= 1).
func (p RetryPolicy) backoff(attempt int) time.Duration {
	minBackoff := float64(max(p.MinBackoff, 0))
	maxBackoff := float64(max(p.MaxBackoff, p.MinBackoff, 0))
	multiplier := max(p.Multiplier, 1.0)
	exponent := float64(attempt - 1)
	jitterFactor := min(max(p.JitterFactor, 0), 1.0)

	base := min(minBackoff*math.Pow(multiplier, exponent), maxBackoff)
	scale := 1 + jitterFactor*(2*p.random()-1)

	return time.Duration(base * scale)
}

func (p RetryPolicy) random() float64 {
	if p.rng != nil {
		return p.rng.Float64()
	}
	return rand.Float64()
}

// isRetryable reports whether err is a transient Spanner error worth retrying.
func isRetryable(err error) bool {
	switch spanner.ErrCode(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal:
		return true
	default:
		return false
	}
}
