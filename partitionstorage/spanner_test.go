package partitionstorage

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type spannerBackend int

const (
	backendNone     spannerBackend = iota // Spanner is not available
	backendEmulator                       // emulator
	backendReal                           // real Spanner

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
			log.Printf("Spanner emulator not available: %v. Skipping Spanner tests.", err)
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

// ensureInstance reuses an existing instance or creates a new one if none exists.
// On creation, it schedules a Cloud Tasks deletion task before creating the instance.
func ensureInstance(ctx context.Context) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, spannerClientOptions...)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

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
	defer client.Close()

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

func generateUniqueName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, strconv.FormatUint(rand.Uint64(), 36))
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

	if err := createEmulatorInstance(ctx); err != nil {
		log.Fatalf("Could not create instance: %v", err)
	}

	return func() {
		if err := container.Terminate(ctx); err != nil {
			log.Fatalf("Could not terminate emulator on docker: %v", err)
		}
	}
}

// createEmulatorInstance creates an instance on the emulator.
func createEmulatorInstance(ctx context.Context) error {
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

// createTestDatabase creates a unique database with the given DDL statements and
// returns the fully qualified database path.
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

func setupSpannerPartitionStorage(t *testing.T, ctx context.Context) *SpannerPartitionStorage {
	t.Helper()

	tableName := t.Name()
	dbPath := createTestDatabase(ctx, t,
		fmt.Sprintf(`CREATE TABLE %s (
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
	)

	client, err := spanner.NewClient(ctx, dbPath, spannerClientOptions...)
	if err != nil {
		t.Fatalf("Failed to create spanner client: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
	})

	return NewSpanner(client, tableName)
}

func TestSpannerPartitionStorage_InitializeRootPartition(t *testing.T) {
	requireSpanner(t)
	t.Parallel()
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
	requireSpanner(t)
	t.Parallel()
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
	requireSpanner(t)
	t.Parallel()
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
	requireSpanner(t)
	t.Parallel()
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
