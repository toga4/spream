package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

//go:embed schema.sql
var schemaDDLTemplate string

type flags struct {
	database          string
	streamName        string
	startTimestamp    time.Time
	endTimestamp      time.Time
	heartbeatInterval time.Duration
	priority          spannerpb.RequestOptions_Priority
	metadataTableName string
	metadataDatabase  string
	createTable       bool
}

const (
	priorityHigh   = "high"
	priorityMedium = "medium"
	priorityLow    = "low"
)

func parseFlags(cmd string, args []string) (*flags, error) {
	var flags flags

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s [OPTIONS...]

Options:
  -d, --database (required)     Database name of change stream with the form 'projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID'.
  -s, --stream   (required)     Change stream name
  -t, --metadata-table          Table name for partition metadata             (default: store partition metadata on memory, not Cloud Spanner)
  --start                       Start timestamp with RFC3339 format           (default: current timestamp)
  --end                         End timestamp with RFC3339 format             (default: indefinite)
  --heartbeat-interval          Heartbeat interval with time.Duration format  (default: 10s)
  --priority [high|medium|low]  Request priority for Cloud Spanner            (default: high)
  --metadata-database           Database name of partition metadata table     (default: same as database option)
  --create-table                Create partition metadata table if not exists (default: false)
  -h, --help                    Print this message

`, cmd)
	}

	fs.StringVar(&flags.database, "d", "", "")
	fs.StringVar(&flags.streamName, "s", "", "")
	fs.StringVar(&flags.metadataTableName, "t", "", "")

	fs.StringVar(&flags.database, "database", "", "")
	fs.StringVar(&flags.streamName, "stream", "", "")
	fs.StringVar(&flags.metadataTableName, "metadata-table", "", "")
	fs.StringVar(&flags.metadataDatabase, "metadata-database", flags.database, "")
	fs.DurationVar(&flags.heartbeatInterval, "heartbeat-interval", 10*time.Second, "")

	fs.BoolVar(&flags.createTable, "create-table", false, "")

	var start, end, priority string
	fs.StringVar(&start, "start", "", "")
	fs.StringVar(&end, "end", "", "")
	fs.StringVar(&priority, "priority", "", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if flags.database == "" || flags.streamName == "" {
		fs.Usage()
		return nil, errors.New("database and stream is required")
	}
	if flags.metadataDatabase == "" {
		flags.metadataDatabase = flags.database
	}

	if start != "" {
		t, err := time.Parse(time.RFC3339, start)
		if err != nil {
			fs.Usage()
			return nil, fmt.Errorf("invalid start timestamp: %v", err)
		}
		flags.startTimestamp = t
	}
	if end != "" {
		t, err := time.Parse(time.RFC3339, end)
		if err != nil {
			fs.Usage()
			return nil, fmt.Errorf("invalid end timestamp: %v", err)
		}
		flags.endTimestamp = t
	}
	if priority != "" {
		switch priority {
		case priorityHigh:
			flags.priority = spannerpb.RequestOptions_PRIORITY_HIGH
		case priorityMedium:
			flags.priority = spannerpb.RequestOptions_PRIORITY_MEDIUM
		case priorityLow:
			flags.priority = spannerpb.RequestOptions_PRIORITY_LOW
		default:
			fs.Usage()
			return nil, fmt.Errorf("invalid priority: %v", priority)
		}
	}

	return &flags, nil
}

type jsonOutputConsumer struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *jsonOutputConsumer) Consume(_ context.Context, change *spream.DataChangeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.out).Encode(change)
}

func main() {
	flags, err := parseFlags(os.Args[0], os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	spannerClient, err := spanner.NewClientWithConfig(ctx, flags.database, spanner.ClientConfig{
		QueryOptions: spanner.QueryOptions{
			Priority: flags.priority,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer spannerClient.Close()

	var partitionStorage spream.PartitionStorage
	if flags.metadataTableName == "" {
		partitionStorage = partitionstorage.NewInmemory()
	} else {
		metadataSpannerClient, err := spanner.NewClient(ctx, flags.metadataDatabase)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer metadataSpannerClient.Close()

		if flags.createTable {
			if err := createPartitionMetadataTable(ctx, metadataSpannerClient.DatabaseName(), flags.metadataTableName); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}

		partitionStorage = partitionstorage.NewSpanner(metadataSpannerClient, flags.metadataTableName)
	}

	cfg := &spream.Config{
		SpannerClient:     spannerClient,
		StreamName:        flags.streamName,
		PartitionStorage:  partitionStorage,
		Consumer:          &jsonOutputConsumer{out: os.Stdout},
		StartTimestamp:    flags.startTimestamp,
		EndTimestamp:      flags.endTimestamp,
		HeartbeatInterval: flags.heartbeatInterval,
	}

	subscriber, err := spream.NewSubscriber(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "Waiting changes...")

	// Subscribe blocks until completion; run concurrently to allow signal handling below.
	done := make(chan error)
	go func() {
		done <- subscriber.Subscribe()
	}()

	// On interrupt, attempt graceful shutdown with timeout; force close if it exceeds the deadline.
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := subscriber.Shutdown(shutdownCtx); err != nil {
		subscriber.Close()
	}

	if err := <-done; err != nil && !errors.Is(err, spream.ErrShutdown) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func createPartitionMetadataTable(ctx context.Context, databaseName, tableName string) error {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	ddl := fmt.Sprintf(schemaDDLTemplate, tableName)

	var statements []string
	for _, s := range strings.Split(ddl, ";") {
		s = strings.TrimSpace(s)
		if s != "" {
			statements = append(statements, s)
		}
	}

	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   databaseName,
		Statements: statements,
	})
	if err != nil {
		return err
	}

	return op.Wait(ctx)
}
