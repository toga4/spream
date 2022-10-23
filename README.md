# spream [![Go Reference](https://pkg.go.dev/badge/github.com/toga4/spream.svg)](https://pkg.go.dev/github.com/toga4/spream)

Tracking Spanner Change Streams for Go

This library is an implementation of reading change streams of Google Spanner in Go.

## Usage

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
)

func main() {
	ctx := context.Background()

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
	changeStreamName := "FooStream"

	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	c := spream.NewController(
		spannerClient,
		changeStreamName,
		changeSink,
		spream.WithWatermarker(watermarker),
	)

	partition := spream.Partition{
		PartitionToken: spream.RootPartition,
		StartTimestamp: time.Now().Add(-time.Hour),
	}
	if err := c.StartWithPartitions(ctx, partition); err != nil {
		panic(err)
	}
}

func changeSink(ctx context.Context, change *spream.Change) error {
	b, err := json.MarshalIndent(change, "", "  ")
	if err != nil {
		return err
	}
	log.Printf("changed: %s", b)
	return nil
}

func watermarker(ctx context.Context, partitionToken string, timestamp time.Time) error {
	log.Printf("watermark: %v : %s", partitionToken, timestamp)
	return nil
}
```
