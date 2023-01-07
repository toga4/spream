# spream

[![Test](https://github.com/toga4/spream/actions/workflows/test.yaml/badge.svg)](https://github.com/toga4/spream/actions/workflows/test.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/toga4/spream.svg)](https://pkg.go.dev/github.com/toga4/spream)

Cloud Spanner Change Streams Subscriber for Go

### Sypnosis

This library is an implementation to subscribe a change stream's records of Google Spanner in Go. 
It is heavily inspired by the SpannerIO connector of the [Apache Beam SDK](https://github.com/apache/beam) and is compatible with the PartitionMetadata data model.

### Motivation

To read a change streams, Google Cloud offers [Dataflow connector](https://cloud.google.com/spanner/docs/change-streams/use-dataflow) as a scalable and reliable solution, but in some cases the abstraction and capabilities of Dataflow pipelines can be too much (or is simply too expensive).
For more flexibility, use the change stream API directly, but it is a bit complex.
This library aims to make reading change streams more flexible and casual to use.

## Example Usage

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
	"github.com/toga4/spream/partitionstorage"
)

func main() {
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
	subscriber := spream.NewSubscriber(spannerClient, changeStreamName, partitionStorage)

	fmt.Fprintf(os.Stderr, "Reading the stream...\n")
	logger := &Logger{out: os.Stdout}
	if err := subscriber.Subscribe(ctx, logger); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		panic(err)
	}
}

type Logger struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *Logger) Consume(change *spream.DataChangeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.out).Encode(change)
}
```
