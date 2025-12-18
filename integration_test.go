package spream_test

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

var (
	projectID    string = os.Getenv("GCP_PROJECT")
	databaseName string
)

func TestMain(m *testing.M) {
	tearDown := setup()
	exitCode := m.Run()
	tearDown()
	os.Exit(exitCode)
}

func setup() func() {
	// skip if env var GCP_PROJECT not set
	if projectID == "" {
		return func() {}
	}

	ctx := context.Background()

	log.Print("Creating spanner instance...")
	instanceName, err := createInstance(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create spanner instance: %v", err)
	}
	log.Printf("Created spanner instance: %q", instanceName)

	log.Print("Creating spanner database...")
	_databaseName, err := createDatabase(ctx, instanceName)
	if err != nil {
		log.Printf("Deleting spanner database: %q...", instanceName)
		if err := deleteInstance(ctx, instanceName); err != nil {
			log.Fatalf("Failed to delete spanner instance: %v", err)
		}
		log.Printf("Deleted spanner database: %q", instanceName)
		log.Fatalf("Failed to create spanner database: %v", err)
	}
	databaseName = _databaseName
	log.Printf("Created spanner database: %q", databaseName)

	return func() {
		log.Printf("Deleting spanner database: %q...", instanceName)
		if err := deleteInstance(ctx, instanceName); err != nil {
			log.Fatalf("Failed to delete spanner instance: %v", err)
		}
		log.Printf("Deleted spanner database: %q", instanceName)
	}
}

func generateUniqueId(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, strconv.FormatUint(rand.Uint64(), 36))
}

func generateUniqueName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, strconv.FormatUint(rand.Uint64(), 36))
}

func createInstance(ctx context.Context, parentProjectID string) (string, error) {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer instanceAdminClient.Close()

	instanceID := generateUniqueId("instance")

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + parentProjectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:          "projects/spream/instanceConfigs/regional-us-central1",
			DisplayName:     instanceID,
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}

func deleteInstance(ctx context.Context, instanceName string) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

	return instanceAdminClient.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: instanceName,
	})
}

func createDatabase(ctx context.Context, parentInstanceName string) (string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer databaseAdminClient.Close()

	databaseID := generateUniqueId("database")

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parentInstanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}

func createTableAndChangeStream(ctx context.Context, databaseName string) (string, string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", "", err
	}
	defer databaseAdminClient.Close()

	tableName := generateUniqueName("table")
	streamName := generateUniqueName("stream")

	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				Bool            BOOL,
				Int64           INT64,
				Float64         FLOAT64,
				Timestamp       TIMESTAMP,
				Date            DATE,
				String          STRING(MAX),
				Bytes           BYTES(MAX),
				Numeric         NUMERIC,
				Json            JSON,
				BoolArray       ARRAY<BOOL>,
				Int64Array      ARRAY<INT64>,
				Float64Array    ARRAY<FLOAT64>,
				TimestampArray  ARRAY<TIMESTAMP>,
				DateArray       ARRAY<DATE>,
				StringArray     ARRAY<STRING(MAX)>,
				BytesArray      ARRAY<BYTES(MAX)>,
				NumericArray    ARRAY<NUMERIC>,
				JsonArray       ARRAY<JSON>,
			) PRIMARY KEY (Int64)`, tableName),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		},
	})
	if err != nil {
		return "", "", err
	}

	if err := op.Wait(ctx); err != nil {
		return "", "", err
	}

	return tableName, streamName, err
}

type consumer struct {
	changes []*spream.DataChangeRecord
}

func (c *consumer) Consume(_ context.Context, change *spream.DataChangeRecord) error {
	c.changes = append(c.changes, change)
	return nil
}

func TestSubscriber(t *testing.T) {
	if projectID == "" {
		t.Skip("Skip integration tests: env var GCP_PROJECT not set.")
	}

	ctx := context.Background()

	spannerClient, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		t.Errorf("Failed to setup: %v", err)
		return
	}

	t.Log("Creating table and change stream...")
	tableName, streamName, err := createTableAndChangeStream(ctx, spannerClient.DatabaseName())
	if err != nil {
		t.Errorf("Failed to create table and change stream: %v", err)
		return
	}
	t.Logf("Created table: %q, change stream: %q", tableName, streamName)

	tests := []struct {
		name       string
		statements []string
		expected   []*spream.DataChangeRecord
	}{
		{
			name: "change",
			statements: []string{
				fmt.Sprintf(`
					INSERT INTO %s
						(Bool, Int64, Float64, Timestamp, Date, String, Bytes, Numeric, Json, BoolArray, Int64Array, Float64Array, TimestampArray, DateArray, StringArray, BytesArray, NumericArray, JsonArray)
					VALUES (
						TRUE,
						1,
						0.5,
						'2023-12-31T23:59:59.999999999Z',
						'2023-01-01',
						'string',
						B'bytes',
						NUMERIC '123.456',
						JSON '{"name":"foobar"}',
						[TRUE, FALSE],
						[1, 2],
						[0.5, 0.25],
						[TIMESTAMP '2023-12-31T23:59:59.999999999Z', TIMESTAMP '2023-01-01T00:00:00Z'],
						[DATE '2023-01-01', DATE '2023-02-01'],
						['string1', 'string2'],
						[B'bytes1', B'bytes2'],
						[NUMERIC '12.345', NUMERIC '67.89'],
						[JSON '{"name":"foobar"}', JSON '{"name":"barbaz"}']
					)
				`, tableName),
				fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 1`, tableName),
				fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 1`, tableName),
			},
			expected: []*spream.DataChangeRecord{
				{
					RecordSequence:                       "00000000",
					IsLastRecordInTransactionInPartition: false,
					TableName:                            tableName,
					ColumnTypes: []*spream.ColumnType{
						{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
						{Name: "Float64", Type: spream.Type{Code: spream.TypeCode_FLOAT64}, OrdinalPosition: 3},
						{Name: "Timestamp", Type: spream.Type{Code: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 4},
						{Name: "Date", Type: spream.Type{Code: spream.TypeCode_DATE}, OrdinalPosition: 5},
						{Name: "String", Type: spream.Type{Code: spream.TypeCode_STRING}, OrdinalPosition: 6},
						{Name: "Bytes", Type: spream.Type{Code: spream.TypeCode_BYTES}, OrdinalPosition: 7},
						{Name: "Numeric", Type: spream.Type{Code: spream.TypeCode_NUMERIC}, OrdinalPosition: 8},
						{Name: "Json", Type: spream.Type{Code: spream.TypeCode_JSON}, OrdinalPosition: 9},
						{Name: "BoolArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_BOOL}, OrdinalPosition: 10},
						{Name: "Int64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_INT64}, OrdinalPosition: 11},
						{Name: "Float64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_FLOAT64}, OrdinalPosition: 12},
						{Name: "TimestampArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 13},
						{Name: "DateArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_DATE}, OrdinalPosition: 14},
						{Name: "StringArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_STRING}, OrdinalPosition: 15},
						{Name: "BytesArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_BYTES}, OrdinalPosition: 16},
						{Name: "NumericArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_NUMERIC}, OrdinalPosition: 17},
						{Name: "JsonArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_JSON}, OrdinalPosition: 18},
					},
					Mods: []*spream.Mod{
						{
							Keys: map[string]any{"Int64": "1"},
							NewValues: map[string]any{
								"Bool":           true,
								"BoolArray":      []any{true, false},
								"Bytes":          "Ynl0ZXM=",
								"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
								"Date":           "2023-01-01",
								"DateArray":      []any{"2023-01-01", "2023-02-01"},
								"Float64":        0.5,
								"Float64Array":   []any{0.5, 0.25},
								"Int64Array":     []any{"1", "2"},
								"Json":           "{\"name\":\"foobar\"}",
								"JsonArray":      []any{"{\"name\":\"foobar\"}", "{\"name\":\"barbaz\"}"},
								"Numeric":        "123.456",
								"NumericArray":   []any{"12.345", "67.89"},
								"String":         "string",
								"StringArray":    []any{"string1", "string2"},
								"Timestamp":      "2023-12-31T23:59:59.999999999Z",
								"TimestampArray": []any{"2023-12-31T23:59:59.999999999Z", "2023-01-01T00:00:00Z"},
							},
							OldValues: map[string]any{},
						},
					},
					ModType:                         spream.ModType_INSERT,
					ValueCaptureType:                "OLD_AND_NEW_VALUES",
					NumberOfRecordsInTransaction:    3,
					NumberOfPartitionsInTransaction: 1,
					TransactionTag:                  "",
					IsSystemTransaction:             false,
				},
				{
					RecordSequence:                       "00000001",
					IsLastRecordInTransactionInPartition: false,
					TableName:                            tableName,
					ColumnTypes: []*spream.ColumnType{
						{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
					},
					Mods: []*spream.Mod{
						{
							Keys:      map[string]any{"Int64": "1"},
							NewValues: map[string]any{"Bool": false},
							OldValues: map[string]any{"Bool": true},
						},
					},
					ModType:                         spream.ModType_UPDATE,
					ValueCaptureType:                "OLD_AND_NEW_VALUES",
					NumberOfRecordsInTransaction:    3,
					NumberOfPartitionsInTransaction: 1,
					TransactionTag:                  "",
					IsSystemTransaction:             false,
				},
				{
					RecordSequence:                       "00000002",
					IsLastRecordInTransactionInPartition: true,
					TableName:                            tableName,
					ColumnTypes: []*spream.ColumnType{
						{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, OrdinalPosition: 2, IsPrimaryKey: true},
						{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
						{Name: "Float64", Type: spream.Type{Code: spream.TypeCode_FLOAT64}, OrdinalPosition: 3},
						{Name: "Timestamp", Type: spream.Type{Code: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 4},
						{Name: "Date", Type: spream.Type{Code: spream.TypeCode_DATE}, OrdinalPosition: 5},
						{Name: "String", Type: spream.Type{Code: spream.TypeCode_STRING}, OrdinalPosition: 6},
						{Name: "Bytes", Type: spream.Type{Code: spream.TypeCode_BYTES}, OrdinalPosition: 7},
						{Name: "Numeric", Type: spream.Type{Code: spream.TypeCode_NUMERIC}, OrdinalPosition: 8},
						{Name: "Json", Type: spream.Type{Code: spream.TypeCode_JSON}, OrdinalPosition: 9},
						{Name: "BoolArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_BOOL}, OrdinalPosition: 10},
						{Name: "Int64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_INT64}, OrdinalPosition: 11},
						{Name: "Float64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_FLOAT64}, OrdinalPosition: 12},
						{Name: "TimestampArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 13},
						{Name: "DateArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_DATE}, OrdinalPosition: 14},
						{Name: "StringArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_STRING}, OrdinalPosition: 15},
						{Name: "BytesArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_BYTES}, OrdinalPosition: 16},
						{Name: "NumericArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_NUMERIC}, OrdinalPosition: 17},
						{Name: "JsonArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: spream.TypeCode_JSON}, OrdinalPosition: 18},
					},
					Mods: []*spream.Mod{
						{
							Keys:      map[string]any{"Int64": "1"},
							NewValues: map[string]any{},
							OldValues: map[string]any{
								"Bool":           false,
								"BoolArray":      []any{true, false},
								"Bytes":          "Ynl0ZXM=",
								"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
								"Date":           "2023-01-01",
								"DateArray":      []any{"2023-01-01", "2023-02-01"},
								"Float64":        0.5,
								"Float64Array":   []any{0.5, 0.25},
								"Int64Array":     []any{"1", "2"},
								"Json":           "{\"name\":\"foobar\"}",
								"JsonArray":      []any{"{\"name\":\"foobar\"}", "{\"name\":\"barbaz\"}"},
								"Numeric":        "123.456",
								"NumericArray":   []any{"12.345", "67.89"},
								"String":         "string",
								"StringArray":    []any{"string1", "string2"},
								"Timestamp":      "2023-12-31T23:59:59.999999999Z",
								"TimestampArray": []any{"2023-12-31T23:59:59.999999999Z", "2023-01-01T00:00:00Z"},
							},
						},
					},
					ModType:                         spream.ModType_DELETE,
					ValueCaptureType:                "OLD_AND_NEW_VALUES",
					NumberOfRecordsInTransaction:    3,
					NumberOfPartitionsInTransaction: 1,
					TransactionTag:                  "",
					IsSystemTransaction:             false,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			partitionStorage := partitionstorage.NewSpanner(spannerClient, generateUniqueName("partition"))

			t.Log("Creating metadata table...")
			if err := partitionStorage.CreateTableIfNotExists(ctx); err != nil {
				t.Errorf("Failed to create metadata table: %v", err)
				return
			}

			subscriber := spream.NewSubscriber(spannerClient, streamName, partitionStorage)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			consumer := &consumer{}
			go func() {
				_ = subscriber.Subscribe(ctx, consumer)
			}()
			t.Log("Subscribe started.")

			t.Log("Executing DML statements...")
			if _, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				for _, stmt := range test.statements {
					if _, err := tx.Update(ctx, spanner.NewStatement(stmt)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Errorf("Failed to execute change statements: %v", err)
				return
			}

			t.Log("Waiting subscription...")
			time.Sleep(5 * time.Second)
			cancel()

			opt := cmpopts.IgnoreFields(spream.DataChangeRecord{}, "CommitTimestamp", "ServerTransactionID")
			if diff := cmp.Diff(test.expected, consumer.changes, opt); diff != "" {
				t.Errorf("diff = %v", diff)
			}
		})
	}
}
