package spream

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
)

func Test_decodeColumnValue(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		want    interface{}
		wantErr bool
	}{
		{
			name:    "bool",
			value:   true,
			want:    spanner.NullBool{Bool: true, Valid: true},
			wantErr: false,
		},
		{
			name:    "int64",
			value:   42,
			want:    spanner.NullInt64{Int64: 42, Valid: true},
			wantErr: false,
		},
		{
			name:    "float64",
			value:   4.2,
			want:    spanner.NullFloat64{Float64: 4.2, Valid: true},
			wantErr: false,
		},
		{
			name:    "timestamp",
			value:   time.Date(2022, 1, 23, 4, 56, 7, 89, time.UTC),
			want:    spanner.NullTime{Time: time.Date(2022, 1, 23, 4, 56, 7, 89, time.UTC), Valid: true},
			wantErr: false,
		},
		{
			name:    "date",
			value:   civil.Date{Year: 2022, Month: 1, Day: 23},
			want:    spanner.NullDate{Date: civil.Date{Year: 2022, Month: 1, Day: 23}, Valid: true},
			wantErr: false,
		},
		{
			name:    "string",
			value:   "42",
			want:    spanner.NullString{StringVal: "42", Valid: true},
			wantErr: false,
		},
		{
			name:    "bytes",
			value:   []byte("42"),
			want:    []byte("42"),
			wantErr: false,
		},
		{
			name:    "numeric",
			value:   big.NewRat(1, 2),
			want:    spanner.NullNumeric{Numeric: *big.NewRat(1, 2), Valid: true},
			wantErr: false,
		},
		{
			name:    "json",
			value:   spanner.NullJSON{Value: map[string]interface{}{"hoge": "fuga"}, Valid: true},
			want:    spanner.NullJSON{Value: map[string]interface{}{"hoge": "fuga"}, Valid: true},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row, err := spanner.NewRow([]string{"col"}, []interface{}{tt.value})
			if err != nil {
				t.Errorf("cannot create new row with value %v: %v", tt.value, err)
				return
			}

			var col spanner.GenericColumnValue
			if err := row.ColumnByName("col", &col); err != nil {
				t.Error(err)
				return
			}

			got, err := decodeColumnValue(col)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeColumnValue() error = %v, wantErr = %v, col = %v", err, tt.wantErr, col)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeColumnValue() got = %v, want = %v, col = %v", got, tt.want, col)
			}

			t.Logf("decodeColumnValue(%v) got = %v", col, got)
		})
	}
}
