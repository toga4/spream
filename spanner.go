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

type ChangeRecord struct {
	DataChangeRecords      []*DataChangeRecord      `spanner:"data_change_record"`
	HeartbeatRecords       []*HeartbeatRecord       `spanner:"heartbeat_record"`
	ChildPartitionsRecords []*ChildPartitionsRecord `spanner:"child_partitions_record"`
}

type DataChangeRecord struct {
	CommitTimestamp                      time.Time     `spanner:"commit_timestamp"`
	RecordSequence                       string        `spanner:"record_sequence"`
	ServerTransactionID                  string        `spanner:"server_transaction_id"`
	IsLastRecordInTransactionInPartition bool          `spanner:"is_last_record_in_transaction_in_partition"`
	TableName                            string        `spanner:"table_name"`
	ColumnTypes                          []*ColumnType `spanner:"column_types"`
	Mods                                 []*Mod        `spanner:"mods"`
	ModType                              ModType       `spanner:"mod_type"`
	ValueCaptureType                     string        `spanner:"value_capture_type"`
	NumberOfRecordsInTransaction         int64         `spanner:"number_of_records_in_transaction"`
	NumberOfPartitionsInTransaction      int64         `spanner:"number_of_partitions_in_transaction"`
	TransactionTag                       string        `spanner:"transaction_tag"`
	IsSystemTransaction                  bool          `spanner:"is_system_transaction"`
}

type ColumnType struct {
	Name            string           `spanner:"name"`
	Type            spanner.NullJSON `spanner:"type"`
	IsPrimaryKey    bool             `spanner:"is_primary_key"`
	OrdinalPosition int64            `spanner:"ordinal_position"`
}

type Mod struct {
	Keys      spanner.NullJSON `spanner:"keys"`
	NewValues spanner.NullJSON `spanner:"new_values"`
	OldValues spanner.NullJSON `spanner:"old_values"`
}

type HeartbeatRecord struct {
	Timestamp time.Time `spanner:"timestamp"`
}

type ChildPartitionsRecord struct {
	StartTimestamp  time.Time         `spanner:"start_timestamp"`
	RecordSequence  string            `spanner:"record_sequence"`
	ChildPartitions []*ChildPartition `spanner:"child_partitions"`
}

type ChildPartition struct {
	Token                 string   `spanner:"token"`
	ParentPartitionTokens []string `spanner:"parent_partition_tokens"`
}

func decodeColumnTypeJSONToType(t *ColumnType) Type {
	m := t.Type.Value.(map[string]interface{})
	code := TypeCode(m["code"].(string))

	if t, ok := m["arrayElementType"].(map[string]interface{}); ok {
		arrayElementType := TypeCode(t["code"].(string))
		return Type{
			Code:             code,
			ArrayElementType: &arrayElementType,
		}
	}

	return Type{
		Code: code,
	}
}

func decodeNullJSONToMap(j spanner.NullJSON) map[string]interface{} {
	if j.IsNull() {
		return nil
	}
	return j.Value.(map[string]interface{})
}
