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

func (d *dao) getSnapshot(
	ctx context.Context,
	tableName string,
	commitTimestamp time.Time,
	keyColumns,
	columns []Column,
	keys Values,
) (Values, error) {
	columnNames := make([]string, 0, len(keyColumns)+len(columns))
	for _, c := range keyColumns {
		columnNames = append(columnNames, c.Name)
	}
	for _, c := range columns {
		columnNames = append(columnNames, c.Name)
	}

	keyValues := spanner.Key{}
	for _, col := range keyColumns {
		v, err := decodePrimaryKeyValue(keys[col.Name], col.Type.Code)
		if err != nil {
			return nil, err
		}
		keyValues = append(keyValues, v)
	}

	row, err := d.client.
		Single().
		WithTimestampBound(spanner.ReadTimestamp(commitTimestamp)).
		ReadRowWithOptions(ctx, tableName, keyValues, columnNames, &spanner.ReadOptions{
			Priority: d.priority,
		})
	if err != nil {
		return nil, err
	}

	snapshot := Values{}
	for _, columnName := range row.ColumnNames() {
		var col spanner.GenericColumnValue
		if err := row.ColumnByName(columnName, &col); err != nil {
			return nil, err
		}
		v, err := decodeColumnValue(col)
		if err != nil {
			return nil, err
		}
		snapshot[columnName] = v
	}

	return snapshot, nil
}
