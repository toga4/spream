package spream

import (
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
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
	simpleTests := []struct {
		name string
		code TypeCode
	}{
		{"STRING", TypeCode_STRING},
		{"BOOL", TypeCode_BOOL},
		{"INT64", TypeCode_INT64},
		{"FLOAT64", TypeCode_FLOAT64},
		{"FLOAT32", TypeCode_FLOAT32},
		{"TIMESTAMP", TypeCode_TIMESTAMP},
		{"DATE", TypeCode_DATE},
		{"BYTES", TypeCode_BYTES},
		{"NUMERIC", TypeCode_NUMERIC},
		{"JSON", TypeCode_JSON},
		{"PROTO", TypeCode_PROTO},
		{"ENUM", TypeCode_ENUM},
		{"UUID", TypeCode_UUID},
		{"INTERVAL", TypeCode_INTERVAL},
	}

	for _, tt := range simpleTests {
		t.Run(tt.name, func(t *testing.T) {
			j := spanner.NullJSON{
				Value: map[string]any{"code": string(tt.code)},
				Valid: true,
			}
			got := decodeColumnTypeJSONToType(j)
			if got.Code != tt.code {
				t.Errorf("Code = %v, want %v", got.Code, tt.code)
			}
			if got.ArrayElementType != TypeCode_NONE {
				t.Errorf("ArrayElementType = %v, want %v", got.ArrayElementType, TypeCode_NONE)
			}
		})
	}

	t.Run("array type", func(t *testing.T) {
		j := spanner.NullJSON{
			Value: map[string]any{
				"code":               "ARRAY",
				"array_element_type": map[string]any{"code": "INT64"},
			},
			Valid: true,
		}
		got := decodeColumnTypeJSONToType(j)
		if got.Code != TypeCode_ARRAY {
			t.Errorf("Code = %v, want %v", got.Code, TypeCode_ARRAY)
		}
		if got.ArrayElementType != TypeCode_INT64 {
			t.Errorf("ArrayElementType = %v, want %v", got.ArrayElementType, TypeCode_INT64)
		}
	})
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

	got := record.decodeToNonSpannerType()

	if !got.CommitTimestamp.Equal(commitTime) {
		t.Errorf("CommitTimestamp = %v, want %v", got.CommitTimestamp, commitTime)
	}
	if got.RecordSequence != "00000001" {
		t.Errorf("RecordSequence = %v, want %q", got.RecordSequence, "00000001")
	}
	if got.ServerTransactionID != "tx-123" {
		t.Errorf("ServerTransactionID = %v, want %q", got.ServerTransactionID, "tx-123")
	}
	if !got.IsLastRecordInTransactionInPartition {
		t.Error("IsLastRecordInTransactionInPartition = false, want true")
	}
	if got.TableName != "Users" {
		t.Errorf("TableName = %v, want %q", got.TableName, "Users")
	}
	if got.ModType != ModType_INSERT {
		t.Errorf("ModType = %v, want %v", got.ModType, ModType_INSERT)
	}
	if got.ValueCaptureType != "OLD_AND_NEW_VALUES" {
		t.Errorf("ValueCaptureType = %v, want %q", got.ValueCaptureType, "OLD_AND_NEW_VALUES")
	}
	if got.NumberOfRecordsInTransaction != 1 {
		t.Errorf("NumberOfRecordsInTransaction = %v, want 1", got.NumberOfRecordsInTransaction)
	}
	if got.NumberOfPartitionsInTransaction != 1 {
		t.Errorf("NumberOfPartitionsInTransaction = %v, want 1", got.NumberOfPartitionsInTransaction)
	}
	if got.TransactionTag != "app=myapp" {
		t.Errorf("TransactionTag = %v, want %q", got.TransactionTag, "app=myapp")
	}
	if got.IsSystemTransaction {
		t.Error("IsSystemTransaction = true, want false")
	}

	// ColumnTypes の検証
	if len(got.ColumnTypes) != 2 {
		t.Fatalf("len(ColumnTypes) = %d, want 2", len(got.ColumnTypes))
	}

	ct0 := got.ColumnTypes[0]
	if ct0.Name != "UserID" {
		t.Errorf("ColumnTypes[0].Name = %v, want %q", ct0.Name, "UserID")
	}
	if ct0.Type.Code != TypeCode_STRING {
		t.Errorf("ColumnTypes[0].Type.Code = %v, want %v", ct0.Type.Code, TypeCode_STRING)
	}
	if !ct0.IsPrimaryKey {
		t.Error("ColumnTypes[0].IsPrimaryKey = false, want true")
	}
	if ct0.OrdinalPosition != 1 {
		t.Errorf("ColumnTypes[0].OrdinalPosition = %v, want 1", ct0.OrdinalPosition)
	}

	ct1 := got.ColumnTypes[1]
	if ct1.Type.Code != TypeCode_ARRAY {
		t.Errorf("ColumnTypes[1].Type.Code = %v, want %v", ct1.Type.Code, TypeCode_ARRAY)
	}
	if ct1.Type.ArrayElementType != TypeCode_STRING {
		t.Errorf("ColumnTypes[1].Type.ArrayElementType = %v, want %v", ct1.Type.ArrayElementType, TypeCode_STRING)
	}

	// Mods の検証
	if len(got.Mods) != 1 {
		t.Fatalf("len(Mods) = %d, want 1", len(got.Mods))
	}

	m := got.Mods[0]
	if m.Keys["UserID"] != "user-1" {
		t.Errorf("Mods[0].Keys[\"UserID\"] = %v, want %q", m.Keys["UserID"], "user-1")
	}
	if m.NewValues == nil {
		t.Error("Mods[0].NewValues = nil, want non-nil")
	}
	if m.OldValues != nil {
		t.Errorf("Mods[0].OldValues = %v, want nil", m.OldValues)
	}
}

func TestDataChangeRecord_DecodeToNonSpannerType_EmptySlices(t *testing.T) {
	record := &dataChangeRecord{
		CommitTimestamp: time.Now(),
		ColumnTypes:    nil,
		Mods:           nil,
		ModType:        "DELETE",
	}

	got := record.decodeToNonSpannerType()

	if len(got.ColumnTypes) != 0 {
		t.Errorf("len(ColumnTypes) = %d, want 0", len(got.ColumnTypes))
	}
	if len(got.Mods) != 0 {
		t.Errorf("len(Mods) = %d, want 0", len(got.Mods))
	}
	if got.ModType != ModType_DELETE {
		t.Errorf("ModType = %v, want %v", got.ModType, ModType_DELETE)
	}
}
