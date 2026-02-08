package spream

import (
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestDecodeNullJSONToMap(t *testing.T) {
	t.Run("null JSON returns nil", func(t *testing.T) {
		j := spanner.NullJSON{Value: nil, Valid: false}
		got := decodeNullJSONToMap(j)
		if got != nil {
			t.Errorf("decodeNullJSONToMap(null) = %v, want nil", got)
		}
	})

	t.Run("valid JSON returns map", func(t *testing.T) {
		m := map[string]any{"key": "value"}
		j := spanner.NullJSON{Value: m, Valid: true}
		got := decodeNullJSONToMap(j)
		if got == nil {
			t.Fatal("decodeNullJSONToMap(valid) returned nil")
		}
		if got["key"] != "value" {
			t.Errorf("got[\"key\"] = %v, want \"value\"", got["key"])
		}
	})
}

func TestDecodeColumnTypeJSONToType(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		want  Type
	}{
		{name: "STRING", input: map[string]any{"code": "STRING"}, want: Type{Code: TypeCode_STRING}},
		{name: "BOOL", input: map[string]any{"code": "BOOL"}, want: Type{Code: TypeCode_BOOL}},
		{name: "INT64", input: map[string]any{"code": "INT64"}, want: Type{Code: TypeCode_INT64}},
		{name: "FLOAT64", input: map[string]any{"code": "FLOAT64"}, want: Type{Code: TypeCode_FLOAT64}},
		{name: "FLOAT32", input: map[string]any{"code": "FLOAT32"}, want: Type{Code: TypeCode_FLOAT32}},
		{name: "TIMESTAMP", input: map[string]any{"code": "TIMESTAMP"}, want: Type{Code: TypeCode_TIMESTAMP}},
		{name: "DATE", input: map[string]any{"code": "DATE"}, want: Type{Code: TypeCode_DATE}},
		{name: "BYTES", input: map[string]any{"code": "BYTES"}, want: Type{Code: TypeCode_BYTES}},
		{name: "NUMERIC", input: map[string]any{"code": "NUMERIC"}, want: Type{Code: TypeCode_NUMERIC}},
		{name: "JSON", input: map[string]any{"code": "JSON"}, want: Type{Code: TypeCode_JSON}},
		{name: "PROTO", input: map[string]any{"code": "PROTO"}, want: Type{Code: TypeCode_PROTO}},
		{name: "ENUM", input: map[string]any{"code": "ENUM"}, want: Type{Code: TypeCode_ENUM}},
		{name: "UUID", input: map[string]any{"code": "UUID"}, want: Type{Code: TypeCode_UUID}},
		{
			name:  "ARRAY of INT64",
			input: map[string]any{"code": "ARRAY", "array_element_type": map[string]any{"code": "INT64"}},
			want:  Type{Code: TypeCode_ARRAY, ArrayElementType: &Type{Code: TypeCode_INT64}},
		},
		{
			name:  "PROTO with proto_type_fqn",
			input: map[string]any{"code": "PROTO", "proto_type_fqn": "com.example.MyMessage"},
			want:  Type{Code: TypeCode_PROTO, ProtoTypeFQN: "com.example.MyMessage"},
		},
		{
			name:  "ENUM with proto_type_fqn",
			input: map[string]any{"code": "ENUM", "proto_type_fqn": "com.example.MyEnum"},
			want:  Type{Code: TypeCode_ENUM, ProtoTypeFQN: "com.example.MyEnum"},
		},
		{
			name: "ARRAY of PROTO with proto_type_fqn",
			input: map[string]any{
				"code": "ARRAY",
				"array_element_type": map[string]any{
					"code":           "PROTO",
					"proto_type_fqn": "com.example.MyMessage",
				},
			},
			want: Type{Code: TypeCode_ARRAY, ArrayElementType: &Type{Code: TypeCode_PROTO, ProtoTypeFQN: "com.example.MyMessage"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := spanner.NullJSON{Value: tt.input, Valid: true}
			got := decodeColumnTypeJSONToType(j)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("decodeColumnTypeJSONToType() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDataChangeRecord_DecodeToNonSpannerType(t *testing.T) {
	commitTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	record := &dataChangeRecord{
		CommitTimestamp:                      commitTime,
		RecordSequence:                       "00000001",
		ServerTransactionID:                  "tx-123",
		IsLastRecordInTransactionInPartition: true,
		TableName:                            "Users",
		ColumnTypes: []*columnType{
			{
				Name:            "UserID",
				Type:            spanner.NullJSON{Value: map[string]any{"code": "STRING"}, Valid: true},
				IsPrimaryKey:    true,
				OrdinalPosition: 1,
			},
			{
				Name:            "Tags",
				Type:            spanner.NullJSON{Value: map[string]any{"code": "ARRAY", "array_element_type": map[string]any{"code": "STRING"}}, Valid: true},
				IsPrimaryKey:    false,
				OrdinalPosition: 2,
			},
		},
		Mods: []*mod{
			{
				Keys:      spanner.NullJSON{Value: map[string]any{"UserID": "user-1"}, Valid: true},
				NewValues: spanner.NullJSON{Value: map[string]any{"Tags": json.RawMessage(`["tag1","tag2"]`)}, Valid: true},
				OldValues: spanner.NullJSON{Value: nil, Valid: false},
			},
		},
		ModType:                         "INSERT",
		ValueCaptureType:                "OLD_AND_NEW_VALUES",
		NumberOfRecordsInTransaction:    1,
		NumberOfPartitionsInTransaction: 1,
		TransactionTag:                  "app=myapp",
		IsSystemTransaction:             false,
	}

	want := &DataChangeRecord{
		CommitTimestamp:                      commitTime,
		RecordSequence:                       "00000001",
		ServerTransactionID:                  "tx-123",
		IsLastRecordInTransactionInPartition: true,
		TableName:                            "Users",
		ColumnTypes: []*ColumnType{
			{
				Name:            "UserID",
				Type:            Type{Code: TypeCode_STRING},
				IsPrimaryKey:    true,
				OrdinalPosition: 1,
			},
			{
				Name:            "Tags",
				Type:            Type{Code: TypeCode_ARRAY, ArrayElementType: &Type{Code: TypeCode_STRING}},
				IsPrimaryKey:    false,
				OrdinalPosition: 2,
			},
		},
		Mods: []*Mod{
			{
				Keys:      map[string]any{"UserID": "user-1"},
				NewValues: map[string]any{"Tags": json.RawMessage(`["tag1","tag2"]`)},
				OldValues: nil,
			},
		},
		ModType:                         ModType_INSERT,
		ValueCaptureType:                "OLD_AND_NEW_VALUES",
		NumberOfRecordsInTransaction:    1,
		NumberOfPartitionsInTransaction: 1,
		TransactionTag:                  "app=myapp",
		IsSystemTransaction:             false,
	}

	got := record.decodeToNonSpannerType()

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("decodeToNonSpannerType() mismatch (-want +got):\n%s", diff)
	}
}

func TestDataChangeRecord_DecodeToNonSpannerType_EmptySlices(t *testing.T) {
	record := &dataChangeRecord{
		CommitTimestamp: time.Now(),
		ColumnTypes:    nil,
		Mods:           nil,
		ModType:        "DELETE",
	}

	want := &DataChangeRecord{
		ColumnTypes: []*ColumnType{},
		Mods:        []*Mod{},
		ModType:     ModType_DELETE,
	}

	got := record.decodeToNonSpannerType()

	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(DataChangeRecord{}, "CommitTimestamp")); diff != "" {
		t.Errorf("decodeToNonSpannerType() mismatch (-want +got):\n%s", diff)
	}
}
