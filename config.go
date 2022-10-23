package spream

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type config struct {
	spannerClient   *spanner.Client
	spannerPriority sppb.RequestOptions_Priority

	changeStreamName      string
	endTimestamp          *time.Time
	heartbeatMilliseconds int64

	changeSink        ChangeSink
	watermarker       Watermarker
	onPartitionClosed OnPartitionClosed

	partitionCh chan Partition
}

var (
	defaultEndTimestamp          *time.Time  = nil
	defaultHeartbeatMilliseconds int64       = 10000
	defaultWatermarker           Watermarker = func(context.Context, string, time.Time) error { return nil }
)

func (c *config) newPartitionStream(partitionToken PartitionToken, startTimestamp time.Time) *partitionStream {
	dao := &dao{
		client:   c.spannerClient,
		priority: c.spannerPriority,
	}
	return &partitionStream{
		dao:                   dao,
		streamName:            c.changeStreamName,
		startTimestamp:        startTimestamp,
		endTimestamp:          c.endTimestamp,
		partitionToken:        partitionToken,
		heartbeatMilliseconds: c.heartbeatMilliseconds,
		changeSink:            c.changeSink,
		watermarker:           c.watermarker,
		onPartitionClosed:     c.onPartitionClosed,
		partitionCh:           c.partitionCh,
	}
}
