package spream

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
)

func NewController(
	spannerClient *spanner.Client,
	changeStreamName string,
	changeSink ChangeSink,
	options ...Option,
) *Controller {
	config := &config{
		endTimestamp:          defaultEndTimestamp,
		heartbeatMilliseconds: defaultHeartbeatMilliseconds,
		watermarker:           defaultWatermarker,
	}
	for _, o := range options {
		o.Apply(config)
	}
	config.spannerClient = spannerClient
	config.changeStreamName = changeStreamName
	config.changeSink = changeSink
	config.partitionCh = make(chan Partition)

	return &Controller{
		config:             config,
		partitionCh:        config.partitionCh,
		trackingPartitions: newPartitionSet(),
	}
}

type Controller struct {
	config      *config
	partitionCh <-chan Partition

	trackingPartitions *partitionSet

	wg *errgroup.Group
}

func (t *Controller) Start(ctx context.Context) error {
	p := Partition{
		PartitionToken: RootPartition,
		StartTimestamp: time.Now(),
	}
	return t.StartWithPartitions(ctx, p)
}

func (t *Controller) StartWithPartitions(ctx context.Context, partitions ...Partition) error {
	t.wg, ctx = errgroup.WithContext(ctx)

	t.wg.Go(func() error {
		for {
			select {
			case p := <-t.partitionCh:
				t.startTrack(ctx, p.PartitionToken, p.StartTimestamp)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	for _, p := range partitions {
		t.startTrack(ctx, p.PartitionToken, p.StartTimestamp)
	}

	return t.wg.Wait()
}

func (t *Controller) startTrack(ctx context.Context, partitionToken PartitionToken, startTimestamp time.Time) {
	if dup := t.trackingPartitions.add(partitionToken); dup {
		return
	}

	t.wg.Go(func() error {
		defer t.trackingPartitions.remove(partitionToken)

		stream := t.config.newPartitionStream(partitionToken, startTimestamp)
		return stream.start(ctx)
	})
}

type partitionSet struct {
	m  map[PartitionToken]struct{}
	mu sync.Mutex
}

func newPartitionSet() *partitionSet {
	return &partitionSet{
		m: make(map[PartitionToken]struct{}),
	}
}

func (s *partitionSet) add(t PartitionToken) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[t]; ok {
		return true
	}

	s.m[t] = struct{}{}
	return false
}

func (s *partitionSet) remove(t PartitionToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, t)
}
