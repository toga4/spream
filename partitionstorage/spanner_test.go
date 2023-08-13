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
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/toga4/spream"
	"google.golang.org/api/iterator"
)

const (
	testProjectID    = "test-project"
	testInstanceID   = "test-instance"
	testDatabaseID   = "test-database"
	testProjectPath  = "projects/" + testProjectID
	testInstancePath = testProjectPath + "/instances/" + testInstanceID
	testDatabasePath = testInstancePath + "/databases/" + testDatabaseID
	testTableName    = "PartitionMetadata"
)

func TestMain(m *testing.M) {
	close := launchEmulatorOnDocker()
	code := m.Run()
	close()
	os.Exit(code)
}

func launchEmulatorOnDocker() func() {
	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}
	pool.MaxWait = 10 * time.Second

	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "gcr.io/cloud-spanner-emulator/emulator",
			Tag:        "latest",
		},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %v", err)
	}

	os.Setenv("SPANNER_EMULATOR_HOST", resource.GetHostPort("9010/tcp"))

	if err := pool.Retry(func() error {
		return createInstance(ctx)
	}); err != nil {
		log.Fatalf("Could not create instance: %v", err)
	}

	if err := createDatabase(ctx); err != nil {
		log.Fatalf("Could not create database: %v", err)
	}

	return func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}

func createInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
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
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
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

func TestSpannerPartitionStorage_CreateTableIfNotExists(t *testing.T) {
	ctx := context.Background()

	client, err := spanner.NewClient(ctx, testDatabasePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer client.Close()

	storage := &SpannerPartitionStorage{
		client:    client,
		tableName: t.Name(),
	}

	if err := storage.CreateTableIfNotExists(ctx); err != nil {
		t.Error(err)
		return
	}

	iter := client.Single().Read(ctx, storage.tableName, spanner.AllKeys(), []string{columnPartitionToken})
	defer iter.Stop()

	if _, err := iter.Next(); err != iterator.Done {
		t.Errorf("Read from %s after SpannerPartitionStorage.CreateTableIfNotExists() = %v, want %v", storage.tableName, err, iterator.Done)
	}

	existsTable, err := existsTable(ctx, client, storage.tableName)
	if err != nil {
		t.Error(err)
		return
	}
	if !existsTable {
		t.Errorf("SpannerPartitionStorage.existsTable() = %v, want %v", existsTable, false)
	}
}

func existsTable(ctx context.Context, client *spanner.Client, tableName string) (bool, error) {
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT 1 FROM information_schema.tables WHERE table_catalog = '' AND table_schema = '' AND table_name = @tableName",
		Params: map[string]interface{}{
			"tableName": tableName,
		},
	})
	defer iter.Stop()

	if _, err := iter.Next(); err != nil {
		if err == iterator.Done {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func setupSpannerPartitionStorage(t *testing.T, ctx context.Context) *SpannerPartitionStorage {
	t.Helper()

	client, err := spanner.NewClient(ctx, testDatabasePath)
	if err != nil {
		t.Error(err)
		return nil
	}
	t.Cleanup(func() {
		client.Close()
	})

	storage := &SpannerPartitionStorage{
		client:    client,
		tableName: t.Name(),
	}

	if err := storage.CreateTableIfNotExists(ctx); err != nil {
		t.Error(err)
		return storage
	}

	return storage
}

func TestSpannerPartitionStorage_InitializeRootPartition(t *testing.T) {
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
	ctx := context.Background()
	storage := setupSpannerPartitionStorage(t, ctx)

	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	insert := func(token string, start time.Time, state spream.State) *spanner.Mutation {
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
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
	if err := storage.AddChildPartitions(ctx, parent, record); err != nil {
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
		return spanner.InsertMap(storage.tableName, map[string]interface{}{
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
		if err := storage.UpdateToScheduled(ctx, partitions); err != nil {
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
		if err := storage.UpdateToRunning(ctx, partitions[0]); err != nil {
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
		if err := storage.UpdateToFinished(ctx, partitions[0]); err != nil {
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

		if err := storage.UpdateWatermark(ctx, partitions[0], timestamp); err != nil {
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
