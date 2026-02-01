package spream_test

import (
	"context"
	"encoding/json"
	"errors"
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

func ExampleNewSubscriber_withOptions() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	partitionMetadataTableName := "PartitionMetadata_FooStream"
	partitionStorage := partitionstorage.NewSpanner(spannerClient, partitionMetadataTableName)
	if err := partitionStorage.CreateTableIfNotExists(ctx); err != nil {
		panic(err)
	}

	changeStreamName := "FooStream"
	subscriber := spream.NewSubscriber(
		spannerClient,
		changeStreamName,
		partitionStorage,
		spream.WithStartTimestamp(time.Now().Add(-time.Hour)),  // Start subscribing from 1 hour ago.
		spream.WithEndTimestamp(time.Now().Add(5*time.Minute)), // Stop subscribing after 5 minutes.
		spream.WithHeartbeatInterval(3*time.Second),
		spream.WithSpannerRequestPriority(spannerpb.RequestOptions_PRIORITY_MEDIUM),
	)

	// Start subscribing in a separate goroutine.
	done := make(chan error)
	logger := &Logger{out: os.Stdout}
	go func() {
		done <- subscriber.Subscribe(logger)
	}()

	// Wait for signal and gracefully shutdown.
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := subscriber.Shutdown(shutdownCtx); err != nil {
		subscriber.Close()
	}

	if err := <-done; err != nil && !errors.Is(err, spream.ErrSubscriberClosed) {
		panic(err)
	}
}

type Logger struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *Logger) Consume(_ context.Context, change *spream.DataChangeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.out).Encode(change)
}
