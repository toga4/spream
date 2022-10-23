package spream_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/toga4/spream"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func ExampleNewController() {
	ctx := context.Background()

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "foo-instance", "foo-database")
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	changeStreamName := "FooStream"
	c := spream.NewController(spannerClient, changeStreamName, func(ctx context.Context, change *spream.Change) error {
		log.Printf("changed: %v", change)
		return nil
	})

	if err := c.Start(ctx); err != nil {
		panic(err)
	}
}

func ExampleNewController_withOptions() {
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
			log.Printf("Changed: %v", change)
			return nil
		},
		spream.WithHeartbeatMilliseconds(1000),
		spream.WithSpannerRequestPriority(sppb.RequestOptions_PRIORITY_LOW),
		spream.WithWatermarker(func(ctx context.Context, partitionToken string, timestamp time.Time) error {
			log.Printf("Watermark: %s : %s", partitionToken, timestamp)
			return nil
		}),
	)

	if err := c.Start(ctx); err != nil {
		panic(err)
	}
}
