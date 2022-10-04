package spream

import (
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

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

func (m *Mod) KeysMap() Values {
	if m.Keys.IsNull() {
		return nil
	}
	return m.Keys.Value.(map[string]interface{})
}

func (m *Mod) NewValuesMap() Values {
	if m.NewValues.IsNull() {
		return nil
	}
	return m.NewValues.Value.(map[string]interface{})
}

func (m *Mod) OldValuesMap() Values {
	if m.OldValues.IsNull() {
		return nil
	}
	return m.OldValues.Value.(map[string]interface{})
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

type Change struct {
	ModType         ModType
	TableName       string
	CommitTimestamp time.Time

	KeyColumns []Column
	Columns    []Column

	Keys      Values
	NewValues Values
	OldValues Values
	Snapshot  Values

	ServerTransactionID                  string
	RecordSequence                       string
	IsLastRecordInTransactionInPartition bool
	NumberOfRecordsInTransaction         int64
	NumberOfPartitionsInTransaction      int64
}

type ModType string

const (
	ModType_CREATE = "CREATE"
	ModType_UPDATE = "UPDATE"
	ModType_DELETE = "DELETE"
)

type Column struct {
	Name               string
	Type               Type
	OrdinalPosition    int64
	KeyOrdinalPosition int64
}

type Type struct {
	Code             TypeCode
	ArrayElementType *TypeCode
}

type TypeCode string

const (
	TypeCode_BOOL      TypeCode = "BOOL"
	TypeCode_INT64     TypeCode = "INT64"
	TypeCode_FLOAT64   TypeCode = "FLOAT64"
	TypeCode_TIMESTAMP TypeCode = "TIMESTAMP"
	TypeCode_DATE      TypeCode = "DATE"
	TypeCode_STRING    TypeCode = "STRING"
	TypeCode_BYTES     TypeCode = "BYTES"
	TypeCode_NUMERIC   TypeCode = "NUMERIC"
	TypeCode_JSON      TypeCode = "JSON"
	TypeCode_ARRAY     TypeCode = "ARRAY"
)

type Values map[string]interface{}

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

func decodePrimaryKeyValue(v interface{}, code TypeCode) (interface{}, error) {
	if v != nil {
		return v, nil
	}
	switch code {
	case TypeCode_BOOL:
		return spanner.NullBool{}, nil
	case TypeCode_INT64:
		return spanner.NullInt64{}, nil
	case TypeCode_FLOAT64:
		return spanner.NullFloat64{}, nil
	case TypeCode_TIMESTAMP:
		return spanner.NullTime{}, nil
	case TypeCode_DATE:
		return spanner.NullDate{}, nil
	case TypeCode_STRING:
		return spanner.NullString{}, nil
	case TypeCode_BYTES:
		return []byte(nil), nil
	case TypeCode_NUMERIC:
		return spanner.NullNumeric{}, nil
	default:
		return nil, fmt.Errorf("unsupported spanner type: %s, with null value for primary key", code)
	}
}

func decodeColumnValue(col spanner.GenericColumnValue) (interface{}, error) {
	t := col.Type
	switch t.Code {
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_DATE:
		var v spanner.NullDate
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_STRING:
		var v spanner.NullString
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_BYTES:
		var v []byte
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_NUMERIC:
		var v spanner.NullNumeric
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_JSON:
		var v spanner.NullJSON
		if err := col.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_ARRAY:
		if t.ArrayElementType == nil {
			return nil, fmt.Errorf("array type %v is with nil array element type", t)
		}
		switch t.ArrayElementType.Code {
		case sppb.TypeCode_BOOL:
			var v []spanner.NullBool
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_INT64:
			var v []spanner.NullInt64
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_FLOAT64:
			var v []spanner.NullFloat64
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_TIMESTAMP:
			var v []spanner.NullTime
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_DATE:
			var v []spanner.NullDate
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_STRING:
			var v []spanner.NullString
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_BYTES:
			var v [][]byte
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_NUMERIC:
			var v []spanner.NullNumeric
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		case sppb.TypeCode_JSON:
			var v []spanner.NullJSON
			if err := col.Decode(&v); err != nil {
				return nil, err
			}
			return v, nil
		}
	}

	return nil, fmt.Errorf("unsupported spanner type: %v, column value", t)
}
