package spream

import (
	"context"
	"encoding/json"
	"log"
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

	changeSink        ChangeSink
	watermarker       Watermarker
	onPartitionClosed OnPartitionClosed
}

func (t *partitionStream) start(ctx context.Context) error {
	if err := t.dao.query(ctx, t.streamName, t.startTimestamp, t.endTimestamp, t.partitionToken.asParameter(), t.heartbeatMilliseconds, t.handle); err != nil {
		return err
	}
	return t.onPartitionClosed(ctx, string(t.partitionToken))
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

	columns := []Column{}
	for _, t := range r.ColumnTypes {
		cc := Column{
			Name:            t.Name,
			Type:            decodeColumnTypeJSONToType(t),
			IsPrimaryKey:    t.IsPrimaryKey,
			OrdinalPosition: t.OrdinalPosition,
		}
		columns = append(columns, cc)
	}

	for _, m := range r.Mods {
		ch := &Change{
			ModType:                              r.ModType,
			TableName:                            r.TableName,
			CommitTimestamp:                      r.CommitTimestamp,
			Columns:                              columns,
			Keys:                                 decodeNullJSONToMap(m.Keys),
			NewValues:                            decodeNullJSONToMap(m.NewValues),
			OldValues:                            decodeNullJSONToMap(m.OldValues),
			ServerTransactionID:                  r.ServerTransactionID,
			RecordSequence:                       r.RecordSequence,
			IsLastRecordInTransactionInPartition: r.IsLastRecordInTransactionInPartition,
			NumberOfRecordsInTransaction:         r.NumberOfRecordsInTransaction,
			NumberOfPartitionsInTransaction:      r.NumberOfPartitionsInTransaction,
		}

		if err := t.changeSink(ctx, ch); err != nil {
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

func dump(partitionToken PartitionToken, recordType string, record interface{}) {
	m := map[string]interface{}{
		"partition_token": partitionToken,
		"record_type":     recordType,
		"record":          record,
	}

	if b, err := json.MarshalIndent(m, "", "  "); err == nil {
		log.Printf("%s", b)
	}
}
