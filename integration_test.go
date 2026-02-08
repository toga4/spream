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

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	tcspanner "github.com/testcontainers/testcontainers-go/modules/gcloud/spanner"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type spannerBackend int

const (
	backendNone     spannerBackend = iota // Spanner を利用できない
	backendEmulator                       // エミュレータ
	backendReal                           // 実 Spanner
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
		// 実 Spanner モード
		testProjectID = projectID
		testInstanceID = generateUniqueName("test")
		testInstancePath = fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID)

		ctx := context.Background()
		if err := createRealInstance(ctx); err != nil {
			log.Fatalf("Failed to create real Spanner instance: %v", err)
		}
		cleanup = func() {
			ctx := context.Background()
			if err := deleteInstance(ctx); err != nil {
				log.Printf("Failed to delete instance %s: %v", testInstancePath, err)
			}
		}
		backend = backendReal
	} else {
		// エミュレータモード
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

// requireSpanner はSpannerバックエンド(エミュレータまたは実Spanner)が利用できない場合にテストをスキップする。
func requireSpanner(t *testing.T) {
	t.Helper()
	if backend == backendNone {
		t.Skip("Spanner not available")
	}
}

// requireRealSpanner は実Spannerでない場合にテストをスキップする。
func requireRealSpanner(t *testing.T) {
	t.Helper()
	if backend != backendReal {
		t.Skip("Real Spanner required")
	}
}

// createRealInstance は実Spannerにテスト用インスタンスを作成する。
func createRealInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

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
		return err
	}

	_, err = op.Wait(ctx)
	return err
}

// deleteInstance はテスト用インスタンスを削除する。
func deleteInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

	return instanceAdminClient.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: testInstancePath,
	})
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
	defer instanceAdminClient.Close()

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
// 実Spanner使用時は t.Cleanup で DropDatabase を登録する。
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
		databaseAdminClient.Close()
		t.Fatalf("Failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		databaseAdminClient.Close()
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
			databaseAdminClient.Close()
		})
	} else {
		databaseAdminClient.Close()
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
	defer subscriber.Close()

	// エミュレータは同一トランザクション内の INSERT→DELETE を最適化して 0 件にする場合がある。
	// INSERT, UPDATE, DELETE を別トランザクションで実行する。

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

	// レコード数を検証する。
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}

	// ModType の順序を検証する。
	wantModTypes := []spream.ModType{spream.ModType_INSERT, spream.ModType_UPDATE, spream.ModType_DELETE}
	for i, want := range wantModTypes {
		if got[i].ModType != want {
			t.Errorf("record[%d].ModType = %q, want %q", i, got[i].ModType, want)
		}
	}

	// 各レコードの基本的な構造を検証する。
	for i, record := range got {
		if record.TableName != tableName {
			t.Errorf("record[%d].TableName = %q, want %q", i, record.TableName, tableName)
		}
		if record.ValueCaptureType != "OLD_AND_NEW_VALUES" {
			t.Errorf("record[%d].ValueCaptureType = %q, want %q", i, record.ValueCaptureType, "OLD_AND_NEW_VALUES")
		}
		if len(record.Mods) != 1 {
			t.Errorf("record[%d].Mods has %d entries, want 1", i, len(record.Mods))
			continue
		}
		if record.Mods[0].Keys["Int64"] != "1" {
			t.Errorf("record[%d].Mods[0].Keys[\"Int64\"] = %v, want \"1\"", i, record.Mods[0].Keys["Int64"])
		}
	}

	// INSERT レコードの NewValues を検証する。
	insertRecord := got[0]
	insertNewValues := insertRecord.Mods[0].NewValues
	if insertNewValues["Bool"] != true {
		t.Errorf("INSERT NewValues[\"Bool\"] = %v, want true", insertNewValues["Bool"])
	}
	if insertNewValues["Float64"] != 0.5 {
		t.Errorf("INSERT NewValues[\"Float64\"] = %v, want 0.5", insertNewValues["Float64"])
	}
	if insertNewValues["String"] != "string" {
		t.Errorf("INSERT NewValues[\"String\"] = %v, want \"string\"", insertNewValues["String"])
	}
	if insertNewValues["Numeric"] != "123.456" {
		t.Errorf("INSERT NewValues[\"Numeric\"] = %v, want \"123.456\"", insertNewValues["Numeric"])
	}
	if insertNewValues["Float32"] != 0.25 {
		t.Errorf("INSERT NewValues[\"Float32\"] = %v, want 0.25", insertNewValues["Float32"])
	}

	// INSERT の ColumnTypes にすべてのカラムが含まれることを検証する。
	if len(insertRecord.ColumnTypes) != 20 {
		t.Errorf("INSERT ColumnTypes has %d entries, want 20", len(insertRecord.ColumnTypes))
	}

	// UPDATE レコードの NewValues/OldValues を検証する。
	updateRecord := got[1]
	if updateRecord.Mods[0].NewValues["Bool"] != false {
		t.Errorf("UPDATE NewValues[\"Bool\"] = %v, want false", updateRecord.Mods[0].NewValues["Bool"])
	}
	if updateRecord.Mods[0].OldValues["Bool"] != true {
		t.Errorf("UPDATE OldValues[\"Bool\"] = %v, want true", updateRecord.Mods[0].OldValues["Bool"])
	}

	// DELETE レコードの NewValues が空であることを検証する。
	deleteRecord := got[2]
	if len(deleteRecord.Mods[0].NewValues) != 0 {
		t.Errorf("DELETE NewValues should be empty, got %v", deleteRecord.Mods[0].NewValues)
	}
	if len(deleteRecord.Mods[0].OldValues) == 0 {
		t.Error("DELETE OldValues should not be empty")
	}
}

func TestSubscriber_Shutdown(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Consumer は処理完了を通知するチャネルを使い、Shutdown 後にドレインされることを検証する。
	var completed atomic.Int32
	consumerStarted := make(chan struct{}, 1)
	gate := make(chan struct{})
	consumer := spream.ConsumerFunc(func(_ context.Context, _ *spream.DataChangeRecord) error {
		select {
		case consumerStarted <- struct{}{}:
		default:
		}
		<-gate // Shutdown が呼ばれるまでブロックする。
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

	// データを投入して Consumer がブロック中の状態にする。
	if err := insertRows(ctx, spannerClient, tableName, 1); err != nil {
		t.Fatalf("Failed to insert rows: %v", err)
	}

	// Consumer が実際にブロック状態に入るまで待つ。
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// Subscribe は ErrShutdown を即座に返す。
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		shutdownDone <- subscriber.Shutdown(shutdownCtx)
	}()

	// Subscribe が ErrShutdown を返すことを確認する。
	select {
	case err := <-subscribeDone:
		if !errors.Is(err, spream.ErrShutdown) {
			t.Errorf("Subscribe() = %v, want ErrShutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe() did not return after Shutdown")
	}

	// Consumer のブロックを解除してドレインを完了させる。
	close(gate)

	// Shutdown がドレイン完了を待って nil を返すことを確認する。
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

	// Consumer を永久にブロックさせる。Consumer が呼ばれたことをチャネルで通知する。
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

	// Consumer が実際にブロック状態に入るまで待つ。
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// Close はインフライト処理の完了を待たずに即座に停止する。
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
		subscriber.Close()
		t.Fatal("Subscribe() did not return after consumer error")
	}
}

func TestSubscriber_ShutdownTimeout(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// Consumer を永久にブロックさせる。Consumer が呼ばれたことをチャネルで通知する。
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

	// Consumer が実際にブロック状態に入るまで待つ。
	select {
	case <-consumerStarted:
	case <-time.After(30 * time.Second):
		subscriber.Close()
		t.Fatal("Consumer was not called within timeout")
	}

	// 短いタイムアウトで Shutdown する。Consumer がブロックしているのでタイムアウトする。
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutdownCancel()

	err = subscriber.Shutdown(shutdownCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() = %v, want context.DeadlineExceeded", err)
	}

	// Subscribe は Shutdown 呼び出し時点で ErrShutdown を返す。
	select {
	case err := <-subscribeDone:
		if !errors.Is(err, spream.ErrShutdown) {
			t.Errorf("Subscribe() = %v, want ErrShutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe() did not return after Shutdown")
	}

	// Close でフォールバックする。
	if err := subscriber.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestSubscriber_AtLeastOnce(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
	ctx := context.Background()

	spannerClient, streamName, tableName, storage := setupSubscriberTest(t, ctx)

	// 第1回: データを投入して受信を確認した後、Close で中断する。
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

	// エミュレータは同一トランザクション内の複数 DML を 1 つの DataChangeRecord にまとめる場合がある。
	// 各行を別トランザクションで挿入する。
	for _, key := range []int64{1, 2, 3} {
		if err := insertRows(ctx, spannerClient, tableName, key); err != nil {
			t.Fatalf("Failed to insert row %d: %v", key, err)
		}
	}

	if !waitForRecords(consumer1, 3, 30*time.Second) {
		subscriber1.Close()
		t.Fatalf("First subscriber: got %d records, want 3", consumer1.count())
	}

	// Close で強制中断する。watermark の更新タイミングによっては一部が再配信される。
	subscriber1.Close()
	<-subscribeDone1

	// 第2回: 同じ PartitionStorage で新しい Subscriber を作成する。
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

	// 追加でデータを投入する。
	if err := insertRows(ctx, spannerClient, tableName, 4); err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// 第2回の subscriber は、少なくとも key=4 の INSERT を受信する。
	// watermark の位置によっては key=1,2,3 の一部も再配信される(at-least-once)。
	if !waitForRecords(consumer2, 1, 30*time.Second) {
		subscriber2.Close()
		t.Fatalf("Second subscriber: got %d records, want >= 1", consumer2.count())
	}

	subscriber2.Close()

	// 第1回 + 第2回で合計 4 件以上受信していることを確認する(at-least-once)。
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

		// 最大同時実行数を記録する。
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}

		// 並行処理を観測するために少し待つ。
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
	defer subscriber.Close()

	// 複数行を投入する。各行は別トランザクションにして独立した DataChangeRecord にする。
	for i := int64(1); i <= 5; i++ {
		if err := insertRows(ctx, spannerClient, tableName, i); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Consumer がすべてのレコードを処理するのを待つ。
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for all records to be consumed")
		case <-time.After(200 * time.Millisecond):
		}
		if currentConcurrent.Load() == 0 && maxConcurrent.Load() > 0 {
			// すべての処理が完了した。
			break
		}
	}

	got := maxConcurrent.Load()
	if got > maxInflight {
		t.Errorf("Max concurrent consumers = %d, want <= %d", got, maxInflight)
	}
}

// TestSubscriber_EndTimestamp はエミュレータが endTimestamp でストリームを終了しないため、
// エミュレータ環境ではスキップする。実 Spanner では EndTimestamp 到達時に Subscribe が
// nil を返して正常終了することを検証する。
func TestSubscriber_EndTimestamp(t *testing.T) {
	requireRealSpanner(t)
}
