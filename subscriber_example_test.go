package spream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

func ExampleNewSubscriber() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	changeStreamName := "FooStream"
	subscriber := spream.NewSubscriber(spannerClient, changeStreamName, partitionstorage.NewInmemory())

	fmt.Fprintf(os.Stderr, "Reading the stream...\n")

	// Start subscribing in a separate goroutine.
	done := make(chan error)
	var mu sync.Mutex
	go func() {
		done <- subscriber.SubscribeFunc(func(_ context.Context, change *spream.DataChangeRecord) error {
			mu.Lock()
			defer mu.Unlock()
			return json.NewEncoder(os.Stdout).Encode(change)
		})
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
