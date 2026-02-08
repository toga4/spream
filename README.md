# spream

[![Test](https://github.com/toga4/spream/actions/workflows/test.yaml/badge.svg)](https://github.com/toga4/spream/actions/workflows/test.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/toga4/spream.svg)](https://pkg.go.dev/github.com/toga4/spream)

Cloud Spanner Change Streams Subscriber for Go

## Synopsis

A Go library for subscribing to Google Cloud Spanner change streams.
It is heavily inspired by the SpannerIO connector of the [Apache Beam SDK](https://github.com/apache/beam) and uses a compatible PartitionMetadata data model.

## Motivation

For reading change streams, Google Cloud offers the [Dataflow connector](https://cloud.google.com/spanner/docs/change-streams/use-dataflow) as a scalable and reliable solution. However, the abstraction and capabilities of Dataflow pipelines can be overkill for some use cases (or simply too expensive).
Using the change stream API directly offers more flexibility, but it's fairly complex.
This library aims to make reading change streams simpler and more accessible, while maintaining an easy transition to Dataflow when needed.

For design philosophy and key decisions, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Installation

```console
$ go get github.com/toga4/spream
```

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

	// Create partition metadata table before use. See partitionstorage/schema.sql for DDL.
	partitionMetadataTableName := "PartitionMetadata_FooStream"
	partitionStorage := partitionstorage.NewSpanner(spannerClient, partitionMetadataTableName)

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

## CLI

A command-line tool for tailing change streams. Also serves as a reference implementation.

### Installation

```console
$ go install github.com/toga4/spream/cmd/spream@latest
```

### Usage

```
Usage: spream [OPTIONS...]

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
```

### Example

```
$ spream -d projects/my-project/instances/my-instance/databases/my-database -s SingerStream
Waiting changes...
{"commit_timestamp":"2023-01-08T05:47:57.998479Z","record_sequence":"00000000","server_transaction_id":"ODIzNDU0OTc2NzUxOTc0NTU1OQ==","is_last_record_in_transaction_in_partition":true,"table_name":"Singers","column_types":[{"name":"SingerId","type":{"code":"INT64"},"is_primary_key":true,"ordinal_position":1},{"name":"Name","type":{"code":"STRING"},"ordinal_position":2}],"mods":[{"keys":{"SingerId":"1"},"new_values":{"Name":"foo"}}],"mod_type":"INSERT","value_capture_type":"OLD_AND_NEW_VALUES","number_of_records_in_transaction":1,"number_of_partitions_in_transaction":1,"transaction_tag":"","is_system_transaction":false}
{"commit_timestamp":"2023-01-08T05:47:58.766575Z","record_sequence":"00000000","server_transaction_id":"MjQ3ODQzMDcxOTMwNjcyODg4Nw==","is_last_record_in_transaction_in_partition":true,"table_name":"Singers","column_types":[{"name":"SingerId","type":{"code":"INT64"},"is_primary_key":true,"ordinal_position":1},{"name":"Name","type":{"code":"STRING"},"ordinal_position":2}],"mods":[{"keys":{"SingerId":"1"},"new_values":{"Name":"bar"},"old_values":{"Name":"foo"}}],"mod_type":"UPDATE","value_capture_type":"OLD_AND_NEW_VALUES","number_of_records_in_transaction":1,"number_of_partitions_in_transaction":1,"transaction_tag":"","is_system_transaction":false}
{"commit_timestamp":"2023-01-08T05:47:59.117807Z","record_sequence":"00000000","server_transaction_id":"ODkwNDMzNDgxMDU2NzAwMDM2MA==","is_last_record_in_transaction_in_partition":true,"table_name":"Singers","column_types":[{"name":"SingerId","type":{"code":"INT64"},"is_primary_key":true,"ordinal_position":1},{"name":"Name","type":{"code":"STRING"},"ordinal_position":2}],"mods":[{"keys":{"SingerId":"1"},"old_values":{"Name":"bar"}}],"mod_type":"DELETE","value_capture_type":"OLD_AND_NEW_VALUES","number_of_records_in_transaction":1,"number_of_partitions_in_transaction":1,"transaction_tag":"","is_system_transaction":false}
```

## Limitations

- **Single process only**: spream does not support distributed coordination. For scale-out, consider [Dataflow connector](https://cloud.google.com/spanner/docs/change-streams/use-dataflow). The shared PartitionMetadata schema enables migration.
- **At-least-once delivery**: Records may be delivered multiple times on crash recovery or network issues. Exactly-once is not guaranteed; consumers must handle duplicates if needed.
- **Ordering**: Records within a partition are delivered in commit timestamp order. Across partitions, no ordering is guaranteed.

## Credits

Heavily inspired by the following projects:

- The SpannerIO connector of the Apache Beam SDK. (https://github.com/apache/beam)
- spanner-change-streams-tail (https://github.com/cloudspannerecosystem/spanner-change-streams-tail)
