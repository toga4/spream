# spream

Tracking Spanner Change Streams for Go

Currently this library is just my practice to reading change streams of Google Spanner.
However this may help you if you are trying to learn about handling Spanner change streams.

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
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	changeStreamName := "FooStream"
	c := spream.NewController(
		spannerClient,
		changeStreamName,
		func(ctx context.Context, change *spream.Change) error {
			b, err := json.MarshalIndent(change, "", "  ")
			if err != nil {
				return err
			}
			log.Printf("changed: %s", b)
			return nil
		},
		spream.WithWatermarker(func(ctx context.Context, partitionToken string, timestamp time.Time) error {
			log.Printf("watermark: %v : %s", partitionToken, timestamp)
			return nil
		}),
	)

	partition := spream.Partition{
		PartitionToken: spream.RootPartition,
		StartTimestamp: time.Now().Add(-time.Hour),
	}
	if err := c.StartWithPartitions(ctx, partition); err != nil {
		panic(err)
	}
}
```