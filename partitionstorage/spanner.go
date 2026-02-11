package partitionstorage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// SpannerPartitionStorage implements PartitionStorage that stores PartitionMetadata in Cloud Spanner.
type SpannerPartitionStorage struct {
	client    *spanner.Client
	tableName string
}

// NewSpanner creates a new instance of SpannerPartitionStorage.
func NewSpanner(client *spanner.Client, tableName string) *SpannerPartitionStorage {
	return &SpannerPartitionStorage{
		client:    client,
		tableName: tableName,
	}
}

// Assert that SpannerPartitionStorage implements PartitionStorage.
var _ spream.PartitionStorage = (*SpannerPartitionStorage)(nil)

const (
	columnPartitionToken  = "PartitionToken"
	columnParentTokens    = "ParentTokens"
	columnStartTimestamp  = "StartTimestamp"
	columnEndTimestamp    = "EndTimestamp"
	columnHeartbeatMillis = "HeartbeatMillis"
	columnState           = "State"
	columnWatermark       = "Watermark"
	columnCreatedAt       = "CreatedAt"
	columnScheduledAt     = "ScheduledAt"
	columnRunningAt       = "RunningAt"
	columnFinishedAt      = "FinishedAt"
)

func (s *SpannerPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*spream.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State != @state ORDER BY Watermark ASC LIMIT 1", s.tableName),
		Params: map[string]any{
			"state": spream.StateFinished,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	r, err := iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return nil, nil
		}
		return nil, err
	}

	partition := new(spream.PartitionMetadata)
	if err := r.ToStruct(partition); err != nil {
		return nil, err
	}

	return partition, nil
}

func (s *SpannerPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*spream.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State IN UNNEST(@states) ORDER BY Watermark ASC", s.tableName),
		Params: map[string]any{
			"states": []spream.State{spream.StateScheduled, spream.StateRunning},
		},
	}

	iter := s.client.Single().Query(ctx, stmt)

	partitions := []*spream.PartitionMetadata{}
	if err := iter.Do(func(r *spanner.Row) error {
		p := new(spream.PartitionMetadata)
		if err := r.ToStruct(p); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

func (s *SpannerPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	m := spanner.InsertOrUpdateMap(s.tableName, map[string]any{
		columnPartitionToken:  spream.RootPartitionToken,
		columnParentTokens:    []string{},
		columnStartTimestamp:  startTimestamp,
		columnEndTimestamp:    endTimestamp,
		columnHeartbeatMillis: heartbeatInterval.Milliseconds(),
		columnState:           spream.StateCreated,
		columnWatermark:       startTimestamp,
		columnCreatedAt:       spanner.CommitTimestamp,
		columnScheduledAt:     nil,
		columnRunningAt:       nil,
		columnFinishedAt:      nil,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func (s *SpannerPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*spream.PartitionMetadata, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT * FROM %s WHERE State = @state AND StartTimestamp >= @minWatermark ORDER BY StartTimestamp ASC", s.tableName),
		Params: map[string]any{
			"state":        spream.StateCreated,
			"minWatermark": minWatermark,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)

	partitions := []*spream.PartitionMetadata{}
	if err := iter.Do(func(r *spanner.Row) error {
		p := new(spream.PartitionMetadata)
		if err := r.ToStruct(p); err != nil {
			return err
		}
		partitions = append(partitions, p)
		return nil
	}); err != nil {
		return nil, err
	}

	return partitions, nil
}

func (s *SpannerPartitionStorage) AddChildPartitions(ctx context.Context, endTimestamp time.Time, heartbeatMillis int64, r *spream.ChildPartitionsRecord) error {
	for _, p := range r.ChildPartitions {
		m := spanner.InsertMap(s.tableName, map[string]any{
			columnPartitionToken:  p.Token,
			columnParentTokens:    p.ParentPartitionTokens,
			columnStartTimestamp:  r.StartTimestamp,
			columnEndTimestamp:    endTimestamp,
			columnHeartbeatMillis: heartbeatMillis,
			columnState:           spream.StateCreated,
			columnWatermark:       r.StartTimestamp,
			columnCreatedAt:       spanner.CommitTimestamp,
		})

		if _, err := s.client.Apply(ctx, []*spanner.Mutation{m}); err != nil {
			// Ignore the AlreadyExists error because a child partition can be found multiple times if partitions are merged.
			if spanner.ErrCode(err) == codes.AlreadyExists {
				continue
			}
			return err
		}
	}

	return nil
}

func (s *SpannerPartitionStorage) UpdateToScheduled(ctx context.Context, partitionTokens []string) error {
	mutations := make([]*spanner.Mutation, 0, len(partitionTokens))
	for _, partitionToken := range partitionTokens {
		m := spanner.UpdateMap(s.tableName, map[string]any{
			columnPartitionToken: partitionToken,
			columnState:          spream.StateScheduled,
			columnScheduledAt:    spanner.CommitTimestamp,
		})
		mutations = append(mutations, m)
	}

	_, err := s.client.Apply(ctx, mutations)
	return err
}

func (s *SpannerPartitionStorage) UpdateToRunning(ctx context.Context, partitionToken string) error {
	m := spanner.UpdateMap(s.tableName, map[string]any{
		columnPartitionToken: partitionToken,
		columnState:          spream.StateRunning,
		columnRunningAt:      spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func (s *SpannerPartitionStorage) UpdateToFinished(ctx context.Context, partitionToken string) error {
	m := spanner.UpdateMap(s.tableName, map[string]any{
		columnPartitionToken: partitionToken,
		columnState:          spream.StateFinished,
		columnFinishedAt:     spanner.CommitTimestamp,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}

func (s *SpannerPartitionStorage) UpdateWatermark(ctx context.Context, partitionToken string, watermark time.Time) error {
	m := spanner.UpdateMap(s.tableName, map[string]any{
		columnPartitionToken: partitionToken,
		columnWatermark:      watermark,
	})

	_, err := s.client.Apply(ctx, []*spanner.Mutation{m})
	return err
}
