package spream

import (
	"context"
	"log"
	"sort"
	"time"
)

type partitionStream struct {
	dao *dao

	streamName            string
	startTimestamp        time.Time
	endTimestamp          *time.Time
	partitionToken        PartitionToken
	heartbeatMilliseconds int64

	partitionCh chan<- Partition

	watermarker   Watermarker
	changeHandler ChangeHandler
}

func (t *partitionStream) start(ctx context.Context) error {
	return t.dao.query(ctx, t.streamName, t.startTimestamp, t.endTimestamp, t.partitionToken.asParameter(), t.heartbeatMilliseconds, t.handle)
}

func (t *partitionStream) handle(ctx context.Context, records []*ChangeRecord) error {
	for _, cr := range records {
		for _, record := range cr.DataChangeRecords {
			if err := t.handleDataChangeRecord(ctx, record); err != nil {
				return err
			}
		}
		for _, record := range cr.HeartbeatRecords {
			if err := t.handleHeartbeatRecord(ctx, record); err != nil {
				return err
			}
		}
		for _, record := range cr.ChildPartitionsRecords {
			if err := t.handleChildPartitionsRecord(ctx, record); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *partitionStream) handleDataChangeRecord(ctx context.Context, r *DataChangeRecord) error {
	// dump(t.partitionToken, "data_change_record", r)

	keyColumns := []Column{}
	columns := []Column{}

	// ColumnTypes の並びは PrimaryKey 列 → 残りの列 の順になっている
	// このとき PrimaryKey 列が複数ある場合は ColumnTypes の先頭にその順序どおりに並んでいる
	// OrdinalPosition はテーブル作成時の DDL に定義されている順序が設定されている
	// つまり ColumnTypes の順序と OrdinalPosition は必ずしも一致しない
	var keyOrdinalPosition int64 = 0
	for i, t := range r.ColumnTypes {
		log.Printf("r.ColumnsTypes[%v]: %#v", i, t)
		cc := Column{
			Name:               t.Name,
			Type:               decodeColumnTypeJSONToType(t),
			OrdinalPosition:    t.OrdinalPosition,
			KeyOrdinalPosition: -1,
		}
		if t.IsPrimaryKey {
			keyOrdinalPosition++
			cc.KeyOrdinalPosition = keyOrdinalPosition
			keyColumns = append(keyColumns, cc)
		} else {
			columns = append(columns, cc)
		}
	}

	sort.Slice(keyColumns, func(i, j int) bool { return keyColumns[i].KeyOrdinalPosition < keyColumns[j].KeyOrdinalPosition })
	sort.Slice(columns, func(i, j int) bool { return columns[i].OrdinalPosition < columns[j].OrdinalPosition })

	for _, m := range r.Mods {
		ch := &Change{
			ModType:                              r.ModType,
			TableName:                            r.TableName,
			CommitTimestamp:                      r.CommitTimestamp,
			KeyColumns:                           keyColumns,
			Columns:                              columns,
			Keys:                                 m.KeysMap(),
			NewValues:                            m.NewValuesMap(),
			OldValues:                            m.OldValuesMap(),
			ServerTransactionID:                  r.ServerTransactionID,
			RecordSequence:                       r.RecordSequence,
			IsLastRecordInTransactionInPartition: r.IsLastRecordInTransactionInPartition,
			NumberOfRecordsInTransaction:         r.NumberOfRecordsInTransaction,
			NumberOfPartitionsInTransaction:      r.NumberOfPartitionsInTransaction,
		}

		if r.ModType == ModType_UPDATE {
			snapshot, err := t.dao.getSnapshot(ctx, ch.TableName, ch.CommitTimestamp, ch.KeyColumns, ch.Columns, ch.Keys)
			if err != nil {
				return err
			}
			ch.Snapshot = snapshot
		}

		if err := t.changeHandler(ctx, ch); err != nil {
			return err
		}
	}

	return t.watermarker(ctx, string(t.partitionToken), r.CommitTimestamp)
}

func (t *partitionStream) handleHeartbeatRecord(ctx context.Context, record *HeartbeatRecord) error {
	// dump(t.partitionToken, "heartbeat_record", record)
	return t.watermarker(ctx, string(t.partitionToken), record.Timestamp)
}

func (t *partitionStream) handleChildPartitionsRecord(ctx context.Context, record *ChildPartitionsRecord) error {
	// dump(t.partitionToken, "child_partitions_record", record)

	for _, cp := range record.ChildPartitions {
		p := Partition{
			StartTimestamp: record.StartTimestamp,
			PartitionToken: PartitionToken(cp.Token),
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t.partitionCh <- p:
		}
	}

	return t.watermarker(ctx, string(t.partitionToken), record.StartTimestamp)
}

// func dump(partitionToken PartitionToken, recordType string, record interface{}) {
// 	m := map[string]interface{}{
// 		"partition_token": partitionToken,
// 		"record_type":     recordType,
// 		"record":          record,
// 	}

// 	if b, err := json.MarshalIndent(m, "", "  "); err == nil {
// 		log.Printf("%s", b)
// 	}
// }
