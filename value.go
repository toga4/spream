package spream

import (
	"time"
)

type Change struct {
	ModType         ModType
	TableName       string
	CommitTimestamp time.Time

	KeyColumns []Column
	Columns    []Column

	Keys      map[string]interface{}
	NewValues map[string]interface{}
	OldValues map[string]interface{}

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
