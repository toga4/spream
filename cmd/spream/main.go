package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

type flags struct {
	database          string
	streamName        string
	startTimestamp    time.Time
	endTimestamp      time.Time
	heartbeatInterval time.Duration
	priority          spannerpb.RequestOptions_Priority
	metadataTableName string
	metadataDatabase  string
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

func (l *jsonOutputConsumer) Consume(change *spream.DataChangeRecord) error {
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

	spannerClient, err := spanner.NewClient(ctx, flags.database)
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
		ps := partitionstorage.NewSpanner(metadataSpannerClient, flags.metadataTableName)
		if err := ps.CreateTableIfNotExists(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		partitionStorage = ps
	}

	options := []spream.Option{}
	if !flags.startTimestamp.IsZero() {
		options = append(options, spream.WithStartTimestamp(flags.startTimestamp))
	}
	if !flags.endTimestamp.IsZero() {
		options = append(options, spream.WithEndTimestamp(flags.endTimestamp))
	}
	if flags.heartbeatInterval != 0 {
		options = append(options, spream.WithHeartbeatInterval(flags.heartbeatInterval))
	}
	if flags.priority != spannerpb.RequestOptions_PRIORITY_UNSPECIFIED {
		options = append(options, spream.WithSpannerRequestPriority(flags.priority))
	}

	subscriber := spream.NewSubscriber(spannerClient, flags.streamName, partitionStorage, options...)
	consumer := &jsonOutputConsumer{out: os.Stdout}

	fmt.Fprintln(os.Stderr, "Waiting changes...")
	if err := subscriber.Subscribe(ctx, consumer); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
