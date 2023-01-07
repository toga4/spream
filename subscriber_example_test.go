package spream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"

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

	var mu sync.Mutex
	if err := subscriber.SubscribeFunc(ctx, func(change *spream.DataChangeRecord) error {
		mu.Lock()
		defer mu.Unlock()
		return json.NewEncoder(os.Stdout).Encode(change)
	}); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		panic(err)
	}
}
