package partitionstorage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/toga4/spream"
)

// InmemoryPartitionStorage implements PartitionStorage that stores PartitionMetadata in memory.
type InmemoryPartitionStorage struct {
	mu sync.Mutex
	m  map[string]*spream.PartitionMetadata
}

// NewInmemory creates new instance of InmemoryPartitionStorage
func NewInmemory() *InmemoryPartitionStorage {
	return &InmemoryPartitionStorage{
		m: make(map[string]*spream.PartitionMetadata),
	}
}

// Assert that InmemoryPartitionStorage implements PartitionStorage.
var _ spream.PartitionStorage = (*InmemoryPartitionStorage)(nil)

func (s *InmemoryPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*spream.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*spream.PartitionMetadata{}
	for _, p := range s.m {
		if p.State != spream.StateFinished {
			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		return nil, nil
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Watermark.Before(partitions[j].Watermark) })
	return partitions[0], nil
}

func (s *InmemoryPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*spream.PartitionMetadata, error) {
	// InmemoryPartitionStorage can't return any partitions
	return nil, nil
}

func (s *InmemoryPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := &spream.PartitionMetadata{
		PartitionToken:  spream.RootPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatInterval.Milliseconds(),
		State:           spream.StateCreated,
		Watermark:       startTimestamp,
		CreatedAt:       time.Now(),
	}
	s.m[p.PartitionToken] = p

	return nil
}

func (s *InmemoryPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*spream.PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*spream.PartitionMetadata{}
	for _, p := range s.m {
		if p.State == spream.StateCreated && !minWatermark.After(p.StartTimestamp) {
			partitions = append(partitions, p)
		}
	}

	return partitions, nil
}

func (s *InmemoryPartitionStorage) AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, r *spream.ChildPartitionsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range r.ChildPartitions {
		p := &spream.PartitionMetadata{
			PartitionToken:  v.Token,
			ParentTokens:    v.ParentPartitionTokens,
			StartTimestamp:  r.StartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           spream.StateCreated,
			Watermark:       r.StartTimestamp,
		}
		s.m[p.PartitionToken] = p
	}

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToScheduled(ctx context.Context, partitionTokens []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, partitionToken := range partitionTokens {
		p := s.m[partitionToken]
		p.ScheduledAt = &now
		p.State = spream.StateScheduled
	}

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToRunning(ctx context.Context, partitionToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partitionToken]
	p.RunningAt = &now
	p.State = spream.StateRunning

	return nil
}

func (s *InmemoryPartitionStorage) UpdateToFinished(ctx context.Context, partitionToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partitionToken]
	p.FinishedAt = &now
	p.State = spream.StateFinished

	return nil
}

func (s *InmemoryPartitionStorage) UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[partitionToken].Watermark = watermark

	return nil
}
