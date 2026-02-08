package partitionstorage

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	tcspanner "github.com/testcontainers/testcontainers-go/modules/gcloud/spanner"
	"github.com/toga4/spream"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testProjectID    = "test-project"
	testInstanceID   = "test-instance"
	testDatabaseID   = "test-database"
	testProjectPath  = "projects/" + testProjectID
	testInstancePath = testProjectPath + "/instances/" + testInstanceID
	testDatabasePath = testInstancePath + "/databases/" + testDatabaseID
)

// spannerEmulatorAvailable indicates whether the Spanner emulator is running.
// Spanner tests call requireEmulator to skip when the emulator is not available.
var (
	spannerEmulatorAvailable bool
	spannerClientOptions     []option.ClientOption
)

func TestMain(m *testing.M) {
	cleanup, err := tryLaunchEmulatorOnDocker()
	if err != nil {
		log.Printf("Spanner emulator not available: %v. Skipping Spanner tests.", err)
	} else {
		spannerEmulatorAvailable = true
	}

	code := m.Run()

	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

// requireEmulator skips the test if the Spanner emulator is not available.
func requireEmulator(t *testing.T) {
	t.Helper()
	if !spannerEmulatorAvailable {
		t.Skip("Spanner emulator not available")
	}
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
	if err := createDatabase(ctx); err != nil {
		log.Fatalf("Could not create database: %v", err)
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
		Parent:     testProjectPath,
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

func createDatabase(ctx context.Context) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          testInstancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabaseID),
	})
	if err != nil {
		return err
	}

	_, err = op.Wait(ctx)
	return err
}

func createTable(ctx context.Context, databaseName, tableName string) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
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
  ROW DELETION POLICY (OLDER_THAN(FinishedAt, INTERVAL 1 DAY))`, tableName),
		},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func setupSpannerPartitionStorage(t *testing.T, ctx context.Context) *SpannerPartitionStorage {
	t.Helper()

	client, err := spanner.NewClient(ctx, testDatabasePath, spannerClientOptions...)
	if err != nil {
		t.Error(err)
		return nil
	}
	t.Cleanup(func() {
		client.Close()
	})

	if err := createTable(ctx, testDatabasePath, t.Name()); err != nil {
		t.Error(err)
		return nil
	}

	return NewSpanner(client, t.Name())
}

func TestSpannerPartitionStorage_InitializeRootPartition(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	storage := setupSpannerPartitionStorage(t, ctx)

	tests := []struct {
		startTimestamp    time.Time
		endTimestamp      time.Time
		heartbeatInterval time.Duration
		want              spream.PartitionMetadata
	}{
		{
			startTimestamp:    time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			endTimestamp:      time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
			heartbeatInterval: 10 * time.Second,
			want: spream.PartitionMetadata{
				PartitionToken:  spream.RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTimestamp:    time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
				HeartbeatMillis: 10000,
				State:           spream.StateCreated,
				Watermark:       time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			startTimestamp:    time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			endTimestamp:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			heartbeatInterval: time.Hour,
			want: spream.PartitionMetadata{
				PartitionToken:  spream.RootPartitionToken,
				ParentTokens:    []string{},
				StartTimestamp:  time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
				EndTimestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				HeartbeatMillis: 3600000,
				State:           spream.StateCreated,
				Watermark:       time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC),
			},
		},
	}
	for _, test := range tests {
		if err := storage.InitializeRootPartition(ctx, test.startTimestamp, test.endTimestamp, test.heartbeatInterval); err != nil {
			t.Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
			continue
		}

		columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}
		row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{spream.RootPartitionToken}, columns)
		if err != nil {
			t.Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
			continue
		}

		got := spream.PartitionMetadata{}
		if err := row.ToStruct(&got); err != nil {
			t.Errorf("InitializeRootPartition(%q, %q, %q): %v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, err)
			continue
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("InitializeRootPartition(%q, %q, %q): got = %+v, want %+v", test.startTimestamp, test.endTimestamp, test.heartbeatInterval, got, test.want)
		}
	}
}

func TestSpannerPartitionStorage_Read(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	storage := setupSpannerPartitionStorage(t, ctx)

	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	insert := func(token string, start time.Time, state spream.State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]any{
			columnPartitionToken:  token,
			columnParentTokens:    []string{},
			columnStartTimestamp:  start,
			columnEndTimestamp:    time.Time{},
			columnHeartbeatMillis: 0,
			columnState:           state,
			columnWatermark:       start,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	if _, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert("created1", timestamp, spream.StateCreated),
		insert("created2", timestamp.Add(-2*time.Second), spream.StateCreated),
		insert("scheduled", timestamp.Add(time.Second), spream.StateScheduled),
		insert("running", timestamp.Add(2*time.Second), spream.StateRunning),
		insert("finished", timestamp.Add(-time.Second), spream.StateFinished),
	}); err != nil {
		t.Error(err)
		return
	}

	t.Run("GetUnfinishedMinWatermarkPartition", func(t *testing.T) {
		got, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
		if err != nil {
			t.Errorf("GetUnfinishedMinWatermarkPartition(ctx): %v", err)
			return
		}

		want := "created2"
		if got.PartitionToken != want {
			t.Errorf("GetUnfinishedMinWatermarkPartition(ctx) = %v, want = %v", got.PartitionToken, want)
		}
	})

	t.Run("GetInterruptedPartitions", func(t *testing.T) {
		partitions, err := storage.GetInterruptedPartitions(ctx)
		if err != nil {
			t.Errorf("GetInterruptedPartitions(ctx): %v", err)
			return
		}

		got := []string{}
		for _, p := range partitions {
			got = append(got, p.PartitionToken)
		}

		want := []string{"scheduled", "running"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetInterruptedPartitions(ctx) = %+v, want = %+v", got, want)
		}
	})

	t.Run("GetSchedulablePartitions", func(t *testing.T) {
		partitions, err := storage.GetSchedulablePartitions(ctx, timestamp)
		if err != nil {
			t.Errorf("GetSchedulablePartitions(ctx, %q): %v", timestamp, err)
			return
		}

		got := []string{}
		for _, p := range partitions {
			got = append(got, p.PartitionToken)
		}

		want := []string{"created1"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetSchedulablePartitions(ctx, %q) = %+v, want = %+v", timestamp, got, want)
		}
	})
}

func TestSpannerPartitionStorage_AddChildPartitions(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	storage := setupSpannerPartitionStorage(t, ctx)

	childStartTimestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var heartbeatMillis int64 = 10000

	parent := &spream.PartitionMetadata{
		PartitionToken:  "parent1",
		ParentTokens:    []string{},
		StartTimestamp:  time.Time{},
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatMillis,
		State:           spream.StateRunning,
		Watermark:       time.Time{},
	}
	record := &spream.ChildPartitionsRecord{
		StartTimestamp: childStartTimestamp,
		ChildPartitions: []*spream.ChildPartition{
			{Token: "token1", ParentPartitionTokens: []string{"parent1"}},
			{Token: "token2", ParentPartitionTokens: []string{"parent1"}},
		},
	}
	if err := storage.AddChildPartitions(ctx, endTimestamp, heartbeatMillis, record); err != nil {
		t.Errorf("GetSchedulablePartitions(ctx, %+v, %+v): %v", parent, record, err)
		return
	}

	columns := []string{columnPartitionToken, columnParentTokens, columnStartTimestamp, columnEndTimestamp, columnHeartbeatMillis, columnState, columnWatermark}

	got := []spream.PartitionMetadata{}
	if err := storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), columns).Do(func(r *spanner.Row) error {
		p := spream.PartitionMetadata{}
		if err := r.ToStruct(&p); err != nil {
			return err
		}
		got = append(got, p)
		return nil
	}); err != nil {
		t.Errorf("GetSchedulablePartitions(ctx, %+v, %+v): %v", parent, record, err)
		return
	}
	want := []spream.PartitionMetadata{
		{
			PartitionToken:  "token1",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           spream.StateCreated,
			Watermark:       childStartTimestamp,
		},
		{
			PartitionToken:  "token2",
			ParentTokens:    []string{"parent1"},
			StartTimestamp:  childStartTimestamp,
			EndTimestamp:    endTimestamp,
			HeartbeatMillis: heartbeatMillis,
			State:           spream.StateCreated,
			Watermark:       childStartTimestamp,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetSchedulablePartitions(ctx, %+v, %+v): got = %+v, want %+v", parent, record, got, want)
	}
}

func TestSpannerPartitionStorage_Update(t *testing.T) {
	requireEmulator(t)
	ctx := context.Background()
	storage := setupSpannerPartitionStorage(t, ctx)

	create := func(token string) *spream.PartitionMetadata {
		return &spream.PartitionMetadata{
			PartitionToken:  token,
			ParentTokens:    []string{},
			StartTimestamp:  time.Time{},
			EndTimestamp:    time.Time{},
			HeartbeatMillis: 0,
			State:           spream.StateCreated,
			Watermark:       time.Time{},
		}
	}

	insert := func(p *spream.PartitionMetadata) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]any{
			columnPartitionToken:  p.PartitionToken,
			columnParentTokens:    p.ParentTokens,
			columnStartTimestamp:  p.StartTimestamp,
			columnEndTimestamp:    p.EndTimestamp,
			columnHeartbeatMillis: p.HeartbeatMillis,
			columnState:           p.State,
			columnWatermark:       p.Watermark,
			columnCreatedAt:       spanner.CommitTimestamp,
		})
	}

	partitions := []*spream.PartitionMetadata{create("token1"), create("token2")}

	if _, err := storage.client.Apply(ctx, []*spanner.Mutation{
		insert(partitions[0]),
		insert(partitions[1]),
	}); err != nil {
		t.Error(err)
		return
	}

	t.Run("UpdateToScheduled", func(t *testing.T) {
		if err := storage.UpdateToScheduled(ctx, []string{partitions[0].PartitionToken, partitions[1].PartitionToken}); err != nil {
			t.Errorf("UpdateToScheduled(ctx, %+v): %v", partitions, err)
			return
		}

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string       `spanner:"PartitionToken"`
			State          spream.State `spanner:"State"`
		}
		got := []partition{}

		if err := storage.client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), columns).Do(func(r *spanner.Row) error {
			p := partition{}
			if err := r.ToStruct(&p); err != nil {
				return err
			}
			got = append(got, p)
			return nil
		}); err != nil {
			t.Errorf("UpdateToScheduled(ctx, %+v): %v", partitions, err)
			return
		}

		want := []partition{
			{PartitionToken: "token1", State: spream.StateScheduled},
			{PartitionToken: "token2", State: spream.StateScheduled},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("UpdateToScheduled(ctx, %+v): got = %+v, want %+v", partitions, got, want)
		}
	})

	t.Run("UpdateToRunning", func(t *testing.T) {
		if err := storage.UpdateToRunning(ctx, partitions[0].PartitionToken); err != nil {
			t.Errorf("UpdateToRunning(ctx, %+v): %v", partitions[0], err)
			return
		}

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string       `spanner:"PartitionToken"`
			State          spream.State `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		if err != nil {
			t.Errorf("UpdateToRunning(ctx, %+v): %v", partitions[0], err)
			return
		}

		got := partition{}
		if err := r.ToStruct(&got); err != nil {
			t.Errorf("UpdateToRunning(ctx, %+v): %v", partitions[0], err)
			return
		}

		want := partition{PartitionToken: "token1", State: spream.StateRunning}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("UpdateToRunning(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	t.Run("UpdateToFinished", func(t *testing.T) {
		if err := storage.UpdateToFinished(ctx, partitions[0].PartitionToken); err != nil {
			t.Errorf("UpdateToFinished(ctx, %+v): %v", partitions[0], err)
			return
		}

		columns := []string{columnPartitionToken, columnState}

		type partition struct {
			PartitionToken string       `spanner:"PartitionToken"`
			State          spream.State `spanner:"State"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		if err != nil {
			t.Errorf("UpdateToFinished(ctx, %+v): %v", partitions[0], err)
			return
		}

		got := partition{}
		if err := r.ToStruct(&got); err != nil {
			t.Errorf("UpdateToFinished(ctx, %+v): %v", partitions[0], err)
			return
		}

		want := partition{PartitionToken: "token1", State: spream.StateFinished}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("UpdateToFinished(ctx, %+v): got = %+v, want %+v", partitions[0], got, want)
		}
	})

	t.Run("UpdateWatermark", func(t *testing.T) {
		timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

		if err := storage.UpdateWatermark(ctx, partitions[0].PartitionToken, timestamp); err != nil {
			t.Errorf("UpdateWatermark(ctx, %+v, %q): %v", partitions[0], timestamp, err)
			return
		}

		columns := []string{columnPartitionToken, columnWatermark}

		type partition struct {
			PartitionToken string    `spanner:"PartitionToken"`
			Watermark      time.Time `spanner:"Watermark"`
		}

		r, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{"token1"}, columns)
		if err != nil {
			t.Errorf("UpdateWatermark(ctx, %+v, %q): %v", partitions[0], timestamp, err)
			return
		}

		got := partition{}
		if err := r.ToStruct(&got); err != nil {
			t.Errorf("UpdateWatermark(ctx, %+v, %q): %v", partitions[0], timestamp, err)
			return
		}

		want := partition{PartitionToken: "token1", Watermark: timestamp}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("UpdateWatermark(ctx, %+v, %q): got = %+v, want %+v", partitions[0], timestamp, got, want)
		}
	})

}
