package spream_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	tcspanner "github.com/testcontainers/testcontainers-go/modules/gcloud/spanner"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type spannerBackend int

const (
	backendNone     spannerBackend = iota // Spanner is not available
	backendEmulator                       // emulator
	backendReal                           // real Spanner
)

const (
	// realTestInstanceID is the fixed instance name reused for real Spanner tests.
	realTestInstanceID = "spream-test"

	// cloudTasksQueuePath is the Cloud Tasks queue path for scheduling deletion tasks.
	cloudTasksQueuePath = "projects/spream/locations/us-central1/queues/spream-test-cleanup"

	// cloudTasksServiceAccount is the service account Cloud Tasks uses for DELETE requests.
	cloudTasksServiceAccount = "github-actions@spream.iam.gserviceaccount.com"
)

var (
	testProjectID    = "test-project"
	testInstanceID   = "test-instance"
	testInstancePath string

	backend              spannerBackend
	spannerClientOptions []option.ClientOption
)

func TestMain(m *testing.M) {
	var cleanup func()

	if projectID := os.Getenv("SPANNER_PROJECT_ID"); projectID != "" {
		// Real Spanner mode
		testProjectID = projectID
		testInstanceID = realTestInstanceID
		testInstancePath = fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID)

		ctx := context.Background()
		if err := ensureInstance(ctx); err != nil {
			log.Fatalf("Failed to ensure Spanner instance: %v", err)
		}
		// Instance deletion is delegated to Cloud Tasks, so no cleanup is needed.
		backend = backendReal
	} else {
		// Emulator mode
		testInstancePath = fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID)

		var err error
		cleanup, err = tryLaunchEmulatorOnDocker()
		if err != nil {
			log.Printf("Spanner emulator not available: %v. Skipping integration tests.", err)
		} else {
			backend = backendEmulator
		}
	}

	code := m.Run()

	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

// requireSpanner skips the test when the Spanner backend (emulator or real Spanner) is not available.
func requireSpanner(t *testing.T) {
	t.Helper()
	if backend == backendNone {
		t.Skip("Spanner not available")
	}
}

// requireRealSpanner skips the test when not running against real Spanner.
func requireRealSpanner(t *testing.T) {
	t.Helper()
	if backend != backendReal {
		t.Skip("Real Spanner required")
	}
}

// ensureInstance reuses an existing instance or creates a new one if none exists.
// On creation, it schedules a Cloud Tasks deletion task before creating the instance.
func ensureInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer func() { _ = instanceAdminClient.Close() }()

	// Reuse the instance if it already exists.
	_, err = instanceAdminClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: testInstancePath,
	})
	if err == nil {
		log.Printf("Reusing existing Spanner instance: %s", testInstancePath)
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetInstance failed: %w", err)
	}

	// The instance does not exist, so create a new one.
	// Schedule a Cloud Tasks deletion task before creating the instance.
	// If task creation fails, do not create the instance (to prevent billing).
	if err := scheduleInstanceDeletion(ctx); err != nil {
		return fmt.Errorf("scheduleInstanceDeletion failed (aborting instance creation): %w", err)
	}

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + testProjectID,
		InstanceId: testInstanceID,
		Instance: &instancepb.Instance{
			Config:          fmt.Sprintf("projects/%s/instanceConfigs/regional-us-central1", testProjectID),
			DisplayName:     testInstanceID,
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		// Reuse the instance if another concurrent test run has already created it.
		if status.Code(err) == codes.AlreadyExists {
			log.Printf("Instance already created by another process, reusing: %s", testInstancePath)
			return nil
		}
		return fmt.Errorf("CreateInstance failed: %w", err)
	}

	if _, err = op.Wait(ctx); err != nil {
		return fmt.Errorf("CreateInstance operation failed: %w", err)
	}
	log.Printf("Created new Spanner instance: %s", testInstancePath)
	return nil
}

// scheduleInstanceDeletion creates a Cloud Tasks HTTP task to delete the instance after 55 minutes.
// It uses the GitHub Actions SA OAuthToken.
func scheduleInstanceDeletion(ctx context.Context) error {
	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("cloudtasks.NewClient: %w", err)
	}
	defer func() { _ = client.Close() }()

	task := &cloudtaskspb.Task{
		ScheduleTime: timestamppb.New(time.Now().Add(55 * time.Minute)),
		MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{
				HttpMethod: cloudtaskspb.HttpMethod_DELETE,
				Url:        fmt.Sprintf("https://spanner.googleapis.com/v1/%s", testInstancePath),
				AuthorizationHeader: &cloudtaskspb.HttpRequest_OauthToken{
					OauthToken: &cloudtaskspb.OAuthToken{
						ServiceAccountEmail: cloudTasksServiceAccount,
					},
				},
			},
		},
	}

	created, err := client.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: cloudTasksQueuePath,
		Task:   task,
	})
	if err != nil {
		return fmt.Errorf("CreateTask: %w", err)
	}
	log.Printf("Scheduled instance deletion task: %s (executes at %s)", created.GetName(), created.GetScheduleTime().AsTime())
	return nil
}

// tryLaunchEmulatorOnDocker attempts to launch the emulator and returns a cleanup function.
// It recovers from panics caused by Docker not being available.
func tryLaunchEmulatorOnDocker() (cleanup func(), err error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		cleanup = launchEmulatorOnDocker()
	}()
	<-done
	return cleanup, err
}

func launchEmulatorOnDocker() func() {
	ctx := context.Background()

	container, err := tcspanner.Run(ctx, "gcr.io/cloud-spanner-emulator/emulator:latest")
	if err != nil {
		log.Fatalf("Could not launch emulator on docker: %v", err)
	}

	spannerClientOptions = []option.ClientOption{
		option.WithEndpoint(container.URI()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		internaloption.SkipDialSettingsValidation(),
	}

	if err := createInstance(ctx); err != nil {
		log.Fatalf("Could not create instance: %v", err)
	}

	return func() {
		if err := container.Terminate(ctx); err != nil {
			log.Fatalf("Could not terminate emulator on docker: %v", err)
		}
	}
}

func createInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer func() { _ = instanceAdminClient.Close() }()

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + testProjectID,
		InstanceId: testInstanceID,
		Instance: &instancepb.Instance{
			Config:      "emulator-config",
			DisplayName: testInstanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		return err
	}

	_, err = op.Wait(ctx)
	return err
}

func generateUniqueName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, strconv.FormatUint(rand.Uint64(), 36))
}

// createTestDatabase creates a unique database with the given DDL statements and
// returns the fully qualified database path. Each test gets its own database so
// that DDL operations do not contend for the same schema lock.
// When using real Spanner, t.Cleanup registers DropDatabase.
func createTestDatabase(ctx context.Context, t *testing.T, ddlStatements ...string) string {
	t.Helper()
	dbID := generateUniqueName("db")

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create database admin client: %v", err)
	}

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          testInstancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbID),
		ExtraStatements: ddlStatements,
	})
	if err != nil {
		_ = databaseAdminClient.Close()
		t.Fatalf("Failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		_ = databaseAdminClient.Close()
		t.Fatalf("Failed to create database: %v", err)
	}

	dbPath := testInstancePath + "/databases/" + dbID

	if backend == backendReal {
		t.Cleanup(func() {
			if err := databaseAdminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
				Database: dbPath,
			}); err != nil {
				t.Logf("Failed to drop database %s: %v", dbPath, err)
			}
			_ = databaseAdminClient.Close()
		})
	} else {
		_ = databaseAdminClient.Close()
	}

	return dbPath
}

// partitionMetadataTableDDL returns the DDL statement for a partition metadata table.
func partitionMetadataTableDDL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
  PartitionToken STRING(MAX) NOT NULL,
  ParentTokens ARRAY<STRING(MAX)> NOT NULL,
  StartTimestamp TIMESTAMP NOT NULL,
  EndTimestamp TIMESTAMP NOT NULL,
  HeartbeatMillis INT64 NOT NULL,
  State STRING(MAX) NOT NULL,
  Watermark TIMESTAMP NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  ScheduledAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  RunningAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  FinishedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (PartitionToken),
  ROW DELETION POLICY (OLDER_THAN(FinishedAt, INTERVAL 1 DAY))`, tableName)
}

// setupSubscriberTest creates a dedicated database with a simple table, change stream,
// and partition metadata table. It returns the Spanner client, stream name, table name,
// and partition storage.
func setupSubscriberTest(t *testing.T, ctx context.Context) (*spanner.Client, string, string, *partitionstorage.SpannerPartitionStorage) {
	t.Helper()

	tableName := generateUniqueName("table")
	streamName := generateUniqueName("stream")
	partitionTableName := generateUniqueName("partition")

	dbPath := createTestDatabase(ctx, t,
		fmt.Sprintf(`CREATE TABLE %s (
			Key INT64,
			Value STRING(MAX),
		) PRIMARY KEY (Key)`, tableName),
		fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		partitionMetadataTableDDL(partitionTableName),
	)

	spannerClient, err := spanner.NewClient(ctx, dbPath, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create spanner client: %v", err)
	}
	t.Cleanup(func() { spannerClient.Close() })

	storage := partitionstorage.NewSpanner(spannerClient, partitionTableName)
	return spannerClient, streamName, tableName, storage
}

// insertRows inserts rows with the given keys into the table.
func insertRows(ctx context.Context, client *spanner.Client, tableName string, keys ...int64) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, key := range keys {
			if _, err := tx.Update(ctx, spanner.Statement{
				SQL:    fmt.Sprintf("INSERT INTO %s (Key, Value) VALUES (@key, @value)", tableName),
				Params: map[string]any{"key": key, "value": fmt.Sprintf("value-%d", key)},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// waitForRecords waits until the consumer has received at least n records or the timeout is reached.
func waitForRecords(consumer *recordingConsumer, n int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return consumer.count() >= n
		case <-time.After(100 * time.Millisecond):
			if consumer.count() >= n {
				return true
			}
		}
	}
}

// recordingConsumer records received DataChangeRecords in a thread-safe manner.
type recordingConsumer struct {
	mu      sync.Mutex
	records []*spream.DataChangeRecord
}

func (c *recordingConsumer) Consume(_ context.Context, change *spream.DataChangeRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, change)
	return nil
}

func (c *recordingConsumer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.records)
}

func (c *recordingConsumer) snapshot() []*spream.DataChangeRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := make([]*spream.DataChangeRecord, len(c.records))
	copy(s, c.records)
	return s
}

func TestSubscriber_DataChangeRecord(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	tableName := generateUniqueName("table")
	streamName := generateUniqueName("stream")
	partitionTableName := generateUniqueName("partition")

	dbPath := createTestDatabase(ctx, t,
		fmt.Sprintf(`CREATE TABLE %s (
			Bool            BOOL,
			Int64           INT64,
			Float32         FLOAT32,
			Float64         FLOAT64,
			Timestamp       TIMESTAMP,
			Date            DATE,
			String          STRING(MAX),
			Bytes           BYTES(MAX),
			Numeric         NUMERIC,
			Json            JSON,
			BoolArray       ARRAY<BOOL>,
			Int64Array      ARRAY<INT64>,
			Float32Array    ARRAY<FLOAT32>,
			Float64Array    ARRAY<FLOAT64>,
			TimestampArray  ARRAY<TIMESTAMP>,
			DateArray       ARRAY<DATE>,
			StringArray     ARRAY<STRING(MAX)>,
			BytesArray      ARRAY<BYTES(MAX)>,
			NumericArray    ARRAY<NUMERIC>,
			JsonArray       ARRAY<JSON>,
		) PRIMARY KEY (Int64)`, tableName),
		fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		partitionMetadataTableDDL(partitionTableName),
	)

	spannerClient, err := spanner.NewClient(ctx, dbPath, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create spanner client: %v", err)
	}
	defer spannerClient.Close()

	storage := partitionstorage.NewSpanner(spannerClient, partitionTableName)
	consumer := &recordingConsumer{}

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()
	defer func() { _ = subscriber.Close() }()

	// The emulator may optimize INSERT followed by DELETE within the same transaction to zero records.
	// Execute INSERT, UPDATE, DELETE in separate transactions.

	// 1. INSERT
	if _, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.NewStatement(fmt.Sprintf(`INSERT INTO %s
			(Bool, Int64, Float32, Float64, Timestamp, Date, String, Bytes, Numeric, Json, BoolArray, Int64Array, Float32Array, Float64Array, TimestampArray, DateArray, StringArray, BytesArray, NumericArray, JsonArray)
		VALUES (
			TRUE, 1, CAST(0.25 AS FLOAT32), 0.5,
			'2023-12-31T23:59:59.999999999Z',
			'2023-01-01',
			'string',
			B'bytes',
			NUMERIC '123.456',
			JSON '{"name":"foobar"}',
			[TRUE, FALSE],
			[1, 2],
			[CAST(0.25 AS FLOAT32), CAST(0.75 AS FLOAT32)],
			[0.5, 0.25],
			[TIMESTAMP '2023-12-31T23:59:59.999999999Z', TIMESTAMP '2023-01-01T00:00:00Z'],
			[DATE '2023-01-01', DATE '2023-02-01'],
			['string1', 'string2'],
			[B'bytes1', B'bytes2'],
			[NUMERIC '12.345', NUMERIC '67.89'],
			[JSON '{"name":"foobar"}', JSON '{"name":"barbaz"}']
		)`, tableName)))
		return err
	}); err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	// 2. UPDATE
	if _, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.NewStatement(fmt.Sprintf(`UPDATE %s SET Bool = FALSE WHERE Int64 = 1`, tableName)))
		return err
	}); err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	// 3. DELETE
	if _, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.NewStatement(fmt.Sprintf(`DELETE FROM %s WHERE Int64 = 1`, tableName)))
		return err
	}); err != nil {
		t.Fatalf("Failed to execute DELETE: %v", err)
	}

	if !waitForRecords(consumer, 3, 30*time.Second) {
		select {
		case err := <-subscribeDone:
			t.Fatalf("Subscribe returned early with error: %v (got %d records)", err, consumer.count())
		default:
		}
		t.Fatalf("Timed out waiting for records: got %d, want 3", consumer.count())
	}

	got := consumer.snapshot()

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}

	// Change stream returns values via JSON, so Go type representations are as follows:
	//   INT64           -> string      (e.g. "1")
	//   FLOAT32/FLOAT64 -> float64     (e.g. 0.25)
	//   BOOL            -> bool        (e.g. true)
	//   TIMESTAMP       -> string      (RFC3339; emulator truncates nanoseconds)
	//   DATE            -> string      (e.g. "2023-01-01")
	//   STRING          -> string      (e.g. "string")
	//   BYTES           -> string      (base64, e.g. "Ynl0ZXM=")
	//   NUMERIC         -> string      (e.g. "123.456")
	//   JSON            -> string      (JSON string, e.g. "{\"name\":\"foobar\"}")
	//   ARRAY<T>        -> []any       (each element follows the types above)

	wantRecords := []*spream.DataChangeRecord{
		{
			TableName:                            tableName,
			IsLastRecordInTransactionInPartition: true,
			ColumnTypes: []*spream.ColumnType{
				{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
				{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, IsPrimaryKey: true, OrdinalPosition: 2},
				{Name: "Float32", Type: spream.Type{Code: spream.TypeCode_FLOAT32}, OrdinalPosition: 3},
				{Name: "Float64", Type: spream.Type{Code: spream.TypeCode_FLOAT64}, OrdinalPosition: 4},
				{Name: "Timestamp", Type: spream.Type{Code: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 5},
				{Name: "Date", Type: spream.Type{Code: spream.TypeCode_DATE}, OrdinalPosition: 6},
				{Name: "String", Type: spream.Type{Code: spream.TypeCode_STRING}, OrdinalPosition: 7},
				{Name: "Bytes", Type: spream.Type{Code: spream.TypeCode_BYTES}, OrdinalPosition: 8},
				{Name: "Numeric", Type: spream.Type{Code: spream.TypeCode_NUMERIC}, OrdinalPosition: 9},
				{Name: "Json", Type: spream.Type{Code: spream.TypeCode_JSON}, OrdinalPosition: 10},
				{Name: "BoolArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BOOL}}, OrdinalPosition: 11},
				{Name: "Int64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_INT64}}, OrdinalPosition: 12},
				{Name: "Float32Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT32}}, OrdinalPosition: 13},
				{Name: "Float64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT64}}, OrdinalPosition: 14},
				{Name: "TimestampArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_TIMESTAMP}}, OrdinalPosition: 15},
				{Name: "DateArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_DATE}}, OrdinalPosition: 16},
				{Name: "StringArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_STRING}}, OrdinalPosition: 17},
				{Name: "BytesArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BYTES}}, OrdinalPosition: 18},
				{Name: "NumericArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_NUMERIC}}, OrdinalPosition: 19},
				{Name: "JsonArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_JSON}}, OrdinalPosition: 20},
			},
			Mods: []*spream.Mod{{
				Keys: map[string]any{"Int64": "1"},
				NewValues: map[string]any{
					"Bool": true, "Float32": 0.25, "Float64": 0.5,
					"Timestamp": "2023-12-31T23:59:59Z", "Date": "2023-01-01",
					"String": "string", "Bytes": "Ynl0ZXM=",
					"Numeric": "123.456", "Json": `{"name":"foobar"}`,
					"BoolArray":      []any{true, false},
					"Int64Array":     []any{"1", "2"},
					"Float32Array":   []any{0.25, 0.75},
					"Float64Array":   []any{0.5, 0.25},
					"TimestampArray": []any{"2023-12-31T23:59:59Z", "2023-01-01T00:00:00Z"},
					"DateArray":      []any{"2023-01-01", "2023-02-01"},
					"StringArray":    []any{"string1", "string2"},
					"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
					"NumericArray":   []any{"12.345", "67.89"},
					"JsonArray":      []any{`{"name":"foobar"}`, `{"name":"barbaz"}`},
				},
				OldValues: map[string]any{},
			}},
			ModType:                         spream.ModType_INSERT,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    1,
			NumberOfPartitionsInTransaction: 1,
		},
		{
			TableName:                            tableName,
			IsLastRecordInTransactionInPartition: true,
			ColumnTypes: []*spream.ColumnType{
				{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
				{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, IsPrimaryKey: true, OrdinalPosition: 2},
				{Name: "Float32", Type: spream.Type{Code: spream.TypeCode_FLOAT32}, OrdinalPosition: 3},
				{Name: "Float64", Type: spream.Type{Code: spream.TypeCode_FLOAT64}, OrdinalPosition: 4},
				{Name: "Timestamp", Type: spream.Type{Code: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 5},
				{Name: "Date", Type: spream.Type{Code: spream.TypeCode_DATE}, OrdinalPosition: 6},
				{Name: "String", Type: spream.Type{Code: spream.TypeCode_STRING}, OrdinalPosition: 7},
				{Name: "Bytes", Type: spream.Type{Code: spream.TypeCode_BYTES}, OrdinalPosition: 8},
				{Name: "Numeric", Type: spream.Type{Code: spream.TypeCode_NUMERIC}, OrdinalPosition: 9},
				{Name: "Json", Type: spream.Type{Code: spream.TypeCode_JSON}, OrdinalPosition: 10},
				{Name: "BoolArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BOOL}}, OrdinalPosition: 11},
				{Name: "Int64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_INT64}}, OrdinalPosition: 12},
				{Name: "Float32Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT32}}, OrdinalPosition: 13},
				{Name: "Float64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT64}}, OrdinalPosition: 14},
				{Name: "TimestampArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_TIMESTAMP}}, OrdinalPosition: 15},
				{Name: "DateArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_DATE}}, OrdinalPosition: 16},
				{Name: "StringArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_STRING}}, OrdinalPosition: 17},
				{Name: "BytesArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BYTES}}, OrdinalPosition: 18},
				{Name: "NumericArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_NUMERIC}}, OrdinalPosition: 19},
				{Name: "JsonArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_JSON}}, OrdinalPosition: 20},
			},
			Mods: []*spream.Mod{{
				Keys: map[string]any{"Int64": "1"},
				NewValues: map[string]any{
					"Bool": false, "Float32": 0.25, "Float64": 0.5,
					"Timestamp": "2023-12-31T23:59:59Z", "Date": "2023-01-01",
					"String": "string", "Bytes": "Ynl0ZXM=",
					"Numeric": "123.456", "Json": `{"name":"foobar"}`,
					"BoolArray":      []any{true, false},
					"Int64Array":     []any{"1", "2"},
					"Float32Array":   []any{0.25, 0.75},
					"Float64Array":   []any{0.5, 0.25},
					"TimestampArray": []any{"2023-12-31T23:59:59Z", "2023-01-01T00:00:00Z"},
					"DateArray":      []any{"2023-01-01", "2023-02-01"},
					"StringArray":    []any{"string1", "string2"},
					"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
					"NumericArray":   []any{"12.345", "67.89"},
					"JsonArray":      []any{`{"name":"foobar"}`, `{"name":"barbaz"}`},
				},
				OldValues: map[string]any{
					"Bool": true, "Float32": 0.25, "Float64": 0.5,
					"Timestamp": "2023-12-31T23:59:59Z", "Date": "2023-01-01",
					"String": "string", "Bytes": "Ynl0ZXM=",
					"Numeric": "123.456", "Json": `{"name":"foobar"}`,
					"BoolArray":      []any{true, false},
					"Int64Array":     []any{"1", "2"},
					"Float32Array":   []any{0.25, 0.75},
					"Float64Array":   []any{0.5, 0.25},
					"TimestampArray": []any{"2023-12-31T23:59:59Z", "2023-01-01T00:00:00Z"},
					"DateArray":      []any{"2023-01-01", "2023-02-01"},
					"StringArray":    []any{"string1", "string2"},
					"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
					"NumericArray":   []any{"12.345", "67.89"},
					"JsonArray":      []any{`{"name":"foobar"}`, `{"name":"barbaz"}`},
				},
			}},
			ModType:                         spream.ModType_UPDATE,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    1,
			NumberOfPartitionsInTransaction: 1,
		},
		{
			TableName:                            tableName,
			IsLastRecordInTransactionInPartition: true,
			ColumnTypes: []*spream.ColumnType{
				{Name: "Bool", Type: spream.Type{Code: spream.TypeCode_BOOL}, OrdinalPosition: 1},
				{Name: "Int64", Type: spream.Type{Code: spream.TypeCode_INT64}, IsPrimaryKey: true, OrdinalPosition: 2},
				{Name: "Float32", Type: spream.Type{Code: spream.TypeCode_FLOAT32}, OrdinalPosition: 3},
				{Name: "Float64", Type: spream.Type{Code: spream.TypeCode_FLOAT64}, OrdinalPosition: 4},
				{Name: "Timestamp", Type: spream.Type{Code: spream.TypeCode_TIMESTAMP}, OrdinalPosition: 5},
				{Name: "Date", Type: spream.Type{Code: spream.TypeCode_DATE}, OrdinalPosition: 6},
				{Name: "String", Type: spream.Type{Code: spream.TypeCode_STRING}, OrdinalPosition: 7},
				{Name: "Bytes", Type: spream.Type{Code: spream.TypeCode_BYTES}, OrdinalPosition: 8},
				{Name: "Numeric", Type: spream.Type{Code: spream.TypeCode_NUMERIC}, OrdinalPosition: 9},
				{Name: "Json", Type: spream.Type{Code: spream.TypeCode_JSON}, OrdinalPosition: 10},
				{Name: "BoolArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BOOL}}, OrdinalPosition: 11},
				{Name: "Int64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_INT64}}, OrdinalPosition: 12},
				{Name: "Float32Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT32}}, OrdinalPosition: 13},
				{Name: "Float64Array", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_FLOAT64}}, OrdinalPosition: 14},
				{Name: "TimestampArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_TIMESTAMP}}, OrdinalPosition: 15},
				{Name: "DateArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_DATE}}, OrdinalPosition: 16},
				{Name: "StringArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_STRING}}, OrdinalPosition: 17},
				{Name: "BytesArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_BYTES}}, OrdinalPosition: 18},
				{Name: "NumericArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_NUMERIC}}, OrdinalPosition: 19},
				{Name: "JsonArray", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_JSON}}, OrdinalPosition: 20},
			},
			Mods: []*spream.Mod{{
				Keys:      map[string]any{"Int64": "1"},
				NewValues: map[string]any{},
				OldValues: map[string]any{
					"Bool": false, "Float32": 0.25, "Float64": 0.5,
					"Timestamp": "2023-12-31T23:59:59Z", "Date": "2023-01-01",
					"String": "string", "Bytes": "Ynl0ZXM=",
					"Numeric": "123.456", "Json": `{"name":"foobar"}`,
					"BoolArray":      []any{true, false},
					"Int64Array":     []any{"1", "2"},
					"Float32Array":   []any{0.25, 0.75},
					"Float64Array":   []any{0.5, 0.25},
					"TimestampArray": []any{"2023-12-31T23:59:59Z", "2023-01-01T00:00:00Z"},
					"DateArray":      []any{"2023-01-01", "2023-02-01"},
					"StringArray":    []any{"string1", "string2"},
					"BytesArray":     []any{"Ynl0ZXMx", "Ynl0ZXMy"},
					"NumericArray":   []any{"12.345", "67.89"},
					"JsonArray":      []any{`{"name":"foobar"}`, `{"name":"barbaz"}`},
				},
			}},
			ModType:                         spream.ModType_DELETE,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    1,
			NumberOfPartitionsInTransaction: 1,
		},
	}

	diffOpts := cmp.Options{
		cmpopts.IgnoreFields(spream.DataChangeRecord{}, "CommitTimestamp", "RecordSequence", "ServerTransactionID"),
		cmpopts.SortSlices(func(a, b *spream.ColumnType) bool { return a.Name < b.Name }),
	}
	if diff := cmp.Diff(wantRecords, got, diffOpts...); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}
}

func TestSubscriber_Shutdown(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Consumer uses a channel to signal completion, verifying that it drains after Shutdown.
	var completed atomic.Int32
	consumerStarted := make(chan struct{}, 1)
	gate := make(chan struct{})
	consumer := spream.ConsumerFunc(func(_ context.Context, _ *spream.DataChangeRecord) error {
		select {
		case consumerStarted <- struct{}{}:
		default:
		}
		<-gate // Block until Shutdown is called.
		completed.Add(1)
		return nil
	})

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()

	// Insert data so that Consumer enters a blocked state.
	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	// Wait until Consumer actually enters the blocked state.
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		_ = subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// Subscribe returns ErrShutdown immediately.
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		shutdownDone <- subscriber.Shutdown(shutdownCtx)
	}()

	// Verify that Subscribe returns ErrShutdown.
	select {
	case err := <-subscribeDone:
		if !errors.Is(err, spream.ErrShutdown) {
			t.Errorf("Subscribe() = %v, want ErrShutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe() did not return after Shutdown")
	}

	// Unblock Consumer to complete the drain.
	close(gate)

	// Verify that Shutdown waits for drain completion and returns nil.
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("Shutdown() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown() did not return after drain")
	}

	if got := completed.Load(); got != 1 {
		t.Errorf("completed = %d, want 1", got)
	}
}

func TestSubscriber_Close(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Block Consumer indefinitely. Notify via channel when Consumer is invoked.
	consumerStarted := make(chan struct{}, 1)
	consumer := spream.ConsumerFunc(func(ctx context.Context, _ *spream.DataChangeRecord) error {
		select {
		case consumerStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()

	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	// Wait until Consumer actually enters the blocked state.
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		_ = subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// Close stops immediately without waiting for in-flight processing to complete.
	if err := subscriber.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	select {
	case err := <-subscribeDone:
		if !errors.Is(err, spream.ErrClosed) {
			t.Errorf("Subscribe() = %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe() did not return after Close")
	}
}

func TestSubscriber_ConsumerError(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	consumerErr := errors.New("consumer error")
	consumer := spream.ConsumerFunc(func(_ context.Context, _ *spream.DataChangeRecord) error {
		return consumerErr
	})

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()

	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	select {
	case err := <-subscribeDone:
		if !errors.Is(err, consumerErr) {
			t.Errorf("Subscribe() = %v, want %v", err, consumerErr)
		}
	case <-time.After(30 * time.Second):
		_ = subscriber.Close()
		t.Fatal("Subscribe() did not return after consumer error")
	}
}

func TestSubscriber_ShutdownTimeout(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Block Consumer indefinitely. Notify via channel when Consumer is invoked.
	consumerStarted := make(chan struct{}, 1)
	consumer := spream.ConsumerFunc(func(ctx context.Context, _ *spream.DataChangeRecord) error {
		select {
		case consumerStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()

	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	// Wait until Consumer actually enters the blocked state.
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		_ = subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// Shutdown with a short timeout. Times out because Consumer is blocking.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutdownCancel()

	err = subscriber.Shutdown(shutdownCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() = %v, want context.DeadlineExceeded", err)
	}

	// Subscribe returns ErrShutdown at the point Shutdown is called.
	select {
	case err := <-subscribeDone:
		if !errors.Is(err, spream.ErrShutdown) {
			t.Errorf("Subscribe() = %v, want ErrShutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe() did not return after Shutdown")
	}

	// Fall back to Close.
	if err := subscriber.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestSubscriber_AtLeastOnce(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Round 1: Insert data, confirm receipt, then interrupt with Close.
	consumer1 := &recordingConsumer{}
	subscriber1, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer1,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone1 := make(chan error, 1)
	go func() {
		subscribeDone1 <- subscriber1.Subscribe()
	}()

	// The emulator may merge multiple DMLs within the same transaction into a single DataChangeRecord.
	// Insert each row in a separate transaction.
	for _, key := range []int64{1, 2, 3} {
		if err := insertRows(ctx, spannerClient, tableName, key); err != nil {
			t.Fatalf("Failed to insert row %d: %v", key, err)
		}
	}

	if !waitForRecords(consumer1, 3, 30*time.Second) {
		_ = subscriber1.Close()
		t.Fatalf("First subscriber: got %d records, want 3", consumer1.count())
	}

	// Force-interrupt with Close. Some records may be redelivered depending on watermark update timing.
	_ = subscriber1.Close()
	<-subscribeDone1

	// Round 2: Create a new Subscriber with the same PartitionStorage.
	consumer2 := &recordingConsumer{}
	subscriber2, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer2,
	})
	if err != nil {
		t.Fatalf("Failed to create second subscriber: %v", err)
	}

	go func() {
		_ = subscriber2.Subscribe()
	}()

	// Insert additional data.
	if err := insertRows(ctx, spannerClient, tableName, 4); err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// The second subscriber receives at least the INSERT for key=4.
	// Depending on the watermark position, some of key=1,2,3 may be redelivered (at-least-once).
	if !waitForRecords(consumer2, 1, 30*time.Second) {
		_ = subscriber2.Close()
		t.Fatalf("Second subscriber: got %d records, want >= 1", consumer2.count())
	}

	_ = subscriber2.Close()

	// Verify that the total across round 1 and round 2 is at least 4 records (at-least-once).
	total := consumer1.count() + consumer2.count()
	if total < 4 {
		t.Errorf("Total records = %d (sub1=%d, sub2=%d), want >= 4", total, consumer1.count(), consumer2.count())
	}
}

func TestSubscriber_MaxInflight(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	const maxInflight = 2

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	consumer := spream.ConsumerFunc(func(_ context.Context, _ *spream.DataChangeRecord) error {
		cur := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		// Record the maximum concurrency.
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}

		// Wait briefly to observe concurrent processing.
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
		MaxInflight:      maxInflight,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	go func() {
		_ = subscriber.Subscribe()
	}()
	defer func() { _ = subscriber.Close() }()

	// Insert multiple rows. Each row is in a separate transaction to produce independent DataChangeRecords.
	for i := int64(1); i <= 5; i++ {
		if err := insertRows(ctx, spannerClient, tableName, i); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Wait for Consumer to process all records.
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for all records to be consumed")
		case <-time.After(200 * time.Millisecond):
		}
		if currentConcurrent.Load() == 0 && maxConcurrent.Load() > 0 {
			// All processing is complete.
			break
		}
	}

	got := maxConcurrent.Load()
	if got > maxInflight {
		t.Errorf("Max concurrent consumers = %d, want <= %d", got, maxInflight)
	}
}

// TestSubscriber_DataChangeRecord_ExtendedTypes verifies change stream records for PROTO, ENUM,
// and UUID types. This test runs only on real Spanner because the emulator does not support
// PROTO/ENUM types.
func TestSubscriber_DataChangeRecord_ExtendedTypes(t *testing.T) {
	requireRealSpanner(t)
	t.Parallel()
	ctx := context.Background()

	tableName := generateUniqueName("table")
	streamName := generateUniqueName("stream")
	partitionTableName := generateUniqueName("partition")

	// Programmatically build a proto descriptor using descriptorpb.
	const (
		protoPackage = "spream.test"
		msgName      = "TestMessage"
		enumName     = "TestEnum"
	)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    ptr("spream/test/test.proto"),
				Package: ptr(protoPackage),
				Syntax:  ptr("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: ptr(msgName),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   ptr("value"),
								Number: ptr(int32(1)),
								Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
							},
						},
					},
				},
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: ptr(enumName),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{Name: ptr("TEST_ENUM_UNSPECIFIED"), Number: ptr(int32(0))},
							{Name: ptr("TEST_ENUM_ONE"), Number: ptr(int32(1))},
						},
					},
				},
			},
		},
	}

	protoBytes, err := proto.Marshal(fds)
	if err != nil {
		t.Fatalf("Failed to marshal FileDescriptorSet: %v", err)
	}

	msgFqn := protoPackage + "." + msgName
	enumFqn := protoPackage + "." + enumName

	ddl := []string{
		fmt.Sprintf("CREATE PROTO BUNDLE (%s, %s)", msgFqn, enumFqn),
		fmt.Sprintf(`CREATE TABLE %s (
			Key           INT64 NOT NULL,
			ProtoCol      %s,
			EnumCol       %s,
			UuidCol       UUID,
			ProtoArrayCol ARRAY<%s>,
			EnumArrayCol  ARRAY<%s>,
			UuidArrayCol  ARRAY<UUID>,
		) PRIMARY KEY (Key)`, tableName, msgFqn, enumFqn, msgFqn, enumFqn),
		fmt.Sprintf("CREATE CHANGE STREAM %s FOR %s", streamName, tableName),
		partitionMetadataTableDDL(partitionTableName),
	}

	// createTestDatabase does not support passing ProtoDescriptors, so create the database directly in this test.
	dbID := generateUniqueName("db")

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create database admin client: %v", err)
	}

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:           testInstancePath,
		CreateStatement:  fmt.Sprintf("CREATE DATABASE `%s`", dbID),
		ExtraStatements:  ddl,
		ProtoDescriptors: protoBytes,
	})
	if err != nil {
		_ = databaseAdminClient.Close()
		t.Fatalf("Failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		_ = databaseAdminClient.Close()
		t.Fatalf("Failed to wait for database creation: %v", err)
	}

	dbPath := testInstancePath + "/databases/" + dbID
	t.Cleanup(func() {
		if err := databaseAdminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
			Database: dbPath,
		}); err != nil {
			t.Logf("Failed to drop database %s: %v", dbPath, err)
		}
		_ = databaseAdminClient.Close()
	})

	spannerClient, err := spanner.NewClient(ctx, dbPath, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create spanner client: %v", err)
	}
	t.Cleanup(func() { spannerClient.Close() })

	storage := partitionstorage.NewSpanner(spannerClient, partitionTableName)
	consumer := &recordingConsumer{}

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()
	defer func() { _ = subscriber.Close() }()

	// Execute INSERT.
	insertSQL := fmt.Sprintf(`INSERT INTO %s (Key, ProtoCol, EnumCol, UuidCol) VALUES (@key, @proto, @enum, @uuid)`, tableName)
	params := map[string]any{
		"key":   int64(1),
		"proto": []byte{}, // empty proto message
		"enum":  int64(0), // TEST_ENUM_UNSPECIFIED
		"uuid":  "550e8400-e29b-41d4-a716-446655440000",
	}

	if _, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: insertSQL, Params: params})
		return err
	}); err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	if !waitForRecords(consumer, 1, 30*time.Second) {
		select {
		case err := <-subscribeDone:
			t.Fatalf("Subscribe returned early with error: %v (got %d records)", err, consumer.count())
		default:
		}
		t.Fatalf("Timed out waiting for records: got %d, want 1", consumer.count())
	}

	got := consumer.snapshot()
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}

	// Change stream returns values via JSON, so Go type representations are as follows:
	//   PROTO -> string (base64-encoded binary; "" for empty message)
	//   ENUM  -> string (numeric string, e.g. "0")
	//   UUID  -> string (e.g. "550e8400-e29b-41d4-a716-446655440000")
	//   Unset ARRAY column -> nil

	wantRecords := []*spream.DataChangeRecord{
		{
			TableName:                            tableName,
			IsLastRecordInTransactionInPartition: true,
			ColumnTypes: []*spream.ColumnType{
				{Name: "Key", Type: spream.Type{Code: spream.TypeCode_INT64}, IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "ProtoCol", Type: spream.Type{Code: spream.TypeCode_PROTO, ProtoTypeFQN: msgFqn}, OrdinalPosition: 2},
				{Name: "EnumCol", Type: spream.Type{Code: spream.TypeCode_ENUM, ProtoTypeFQN: enumFqn}, OrdinalPosition: 3},
				{Name: "UuidCol", Type: spream.Type{Code: spream.TypeCode_UUID}, OrdinalPosition: 4},
				{Name: "ProtoArrayCol", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_PROTO, ProtoTypeFQN: msgFqn}}, OrdinalPosition: 5},
				{Name: "EnumArrayCol", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_ENUM, ProtoTypeFQN: enumFqn}}, OrdinalPosition: 6},
				{Name: "UuidArrayCol", Type: spream.Type{Code: spream.TypeCode_ARRAY, ArrayElementType: &spream.Type{Code: spream.TypeCode_UUID}}, OrdinalPosition: 7},
			},
			Mods: []*spream.Mod{{
				Keys: map[string]any{"Key": "1"},
				NewValues: map[string]any{
					"ProtoCol":      "",
					"EnumCol":       "0",
					"UuidCol":       "550e8400-e29b-41d4-a716-446655440000",
					"ProtoArrayCol": nil,
					"EnumArrayCol":  nil,
					"UuidArrayCol":  nil,
				},
				OldValues: map[string]any{},
			}},
			ModType:                         spream.ModType_INSERT,
			ValueCaptureType:                "OLD_AND_NEW_VALUES",
			NumberOfRecordsInTransaction:    1,
			NumberOfPartitionsInTransaction: 1,
		},
	}

	diffOpts := cmp.Options{
		cmpopts.IgnoreFields(spream.DataChangeRecord{}, "CommitTimestamp", "RecordSequence", "ServerTransactionID"),
		cmpopts.SortSlices(func(a, b *spream.ColumnType) bool { return a.Name < b.Name }),
	}
	if diff := cmp.Diff(wantRecords, got, diffOpts...); diff != "" {
		t.Errorf("records mismatch (-want +got):\n%s", diff)
	}
}

func ptr[T any](v T) *T { return &v }

// TestSubscriber_EndTimestamp verifies that Subscribe returns nil and terminates normally
// when EndTimestamp is reached.
func TestSubscriber_EndTimestamp(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)
	consumer := &recordingConsumer{}

	// Set EndTimestamp to slightly ahead of the current time.
	endTimestamp := time.Now().Add(15 * time.Second)

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:    spannerClient,
		StreamName:       streamName,
		PartitionStorage: storage,
		Consumer:         consumer,
		EndTimestamp:     endTimestamp,
	})
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- subscriber.Subscribe()
	}()

	// Insert data to confirm the change stream is operating.
	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	if !waitForRecords(consumer, 1, 30*time.Second) {
		select {
		case err := <-subscribeDone:
			t.Fatalf("Subscribe returned early with error: %v", err)
		default:
		}
		t.Fatalf("Timed out waiting for records: got %d, want >= 1", consumer.count())
	}

	// After reaching EndTimestamp, Subscribe returns nil and terminates normally.
	select {
	case err := <-subscribeDone:
		if err != nil {
			t.Errorf("Subscribe() = %v, want nil", err)
		}
	case <-time.After(60 * time.Second):
		_ = subscriber.Close()
		t.Fatal("Subscribe() did not return after EndTimestamp")
	}
}
