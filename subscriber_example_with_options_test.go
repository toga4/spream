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
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

func ExampleNewSubscriber_withOptions() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	// Create partition metadata table before use. See partitionstorage/schema.sql for DDL.
	partitionMetadataTableName := "PartitionMetadata_FooStream"
	partitionStorage := partitionstorage.NewSpanner(spannerClient, partitionMetadataTableName)

	subscriber, err := spream.NewSubscriber(&spream.Config{
		SpannerClient:     spannerClient,
		StreamName:        "FooStream",
		PartitionStorage:  partitionStorage,
		Consumer:          &Logger{out: os.Stdout},
		StartTimestamp:    time.Now().Add(-time.Hour),      // Start subscribing from 1 hour ago.
		EndTimestamp:      time.Now().Add(5 * time.Minute), // Stop subscribing after 5 minutes.
		HeartbeatInterval: 3 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	// Start subscribing in a separate goroutine.
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Subscribe()
	}()

	// Wait for either a signal or Subscribe to return (e.g., EndTimestamp reached, init error).
	select {
	case <-ctx.Done():
		// On interrupt, attempt graceful shutdown with timeout; force close if it exceeds the deadline.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := subscriber.Shutdown(shutdownCtx); err != nil {
			_ = subscriber.Close()
		}
	case err := <-done:
		if err != nil && !errors.Is(err, spream.ErrShutdown) {
			panic(err)
		}
		return
	}

	if err := <-done; err != nil && !errors.Is(err, spream.ErrShutdown) {
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
