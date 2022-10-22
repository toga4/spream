package spream

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

type dao struct {
	client   *spanner.Client
	priority sppb.RequestOptions_Priority
}

func (d *dao) query(
	ctx context.Context,
	streamName string,
	startTimestamp time.Time,
	endTimestamp *time.Time,
	partitionToken *string,
	heartbeatMilliseconds int64,
	f func(ctx context.Context, record []*ChangeRecord) error,
) error {
	statement := spanner.Statement{
		SQL: fmt.Sprintf(`
			SELECT ChangeRecord FROM READ_%s (
				start_timestamp => @startTimestamp, 
				end_timestamp => @endTimestamp,
				partition_token => @partitionToken,
				heartbeat_milliseconds => @heartbeatMilliseconds
			)
		`, streamName),
		Params: map[string]interface{}{
			"startTimestamp":        startTimestamp,
			"endTimestamp":          endTimestamp,
			"partitionToken":        partitionToken,
			"heartbeatMilliseconds": heartbeatMilliseconds,
		},
	}

	return d.client.
		Single().
		QueryWithOptions(ctx, statement, spanner.QueryOptions{
			Priority: d.priority,
		}).
		Do(func(r *spanner.Row) error {
			v := []*ChangeRecord{}
			if err := r.Columns(&v); err != nil {
				return err
			}
			if err := f(ctx, v); err != nil {
				return err
			}
			return nil
		})
}
