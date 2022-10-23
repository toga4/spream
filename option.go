package spream

import (
	"time"

	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type Option interface {
	Apply(*config)
}

type withSpannerRequestPriority sppb.RequestOptions_Priority

func (w withSpannerRequestPriority) Apply(c *config) {
	c.spannerPriority = sppb.RequestOptions_Priority(w)
}

func WithSpannerRequestPriority(p sppb.RequestOptions_Priority) Option {
	return withSpannerRequestPriority(p)
}

type withEndTimestamp struct{ t *time.Time }

func (w withEndTimestamp) Apply(c *config) {
	c.endTimestamp = w.t
}

func WithEndTimestamp(t time.Time) Option {
	return withEndTimestamp{t: &t}
}

type withHeartbeatMilliseconds int64

func (w withHeartbeatMilliseconds) Apply(c *config) {
	c.heartbeatMilliseconds = int64(w)
}

func WithHeartbeatMilliseconds(ms int64) Option {
	return withHeartbeatMilliseconds(ms)
}

type withWatermarker Watermarker

func (w withWatermarker) Apply(c *config) {
	c.watermarker = Watermarker(w)
}

func WithWatermarker(f Watermarker) Option {
	return withWatermarker(f)
}

type withOnPartitionClosed OnPartitionClosed

func (w withOnPartitionClosed) Apply(c *config) {
	c.onPartitionClosed = OnPartitionClosed(w)
}

func WithOnPartitionClosed(f OnPartitionClosed) Option {
	return withOnPartitionClosed(f)
}
