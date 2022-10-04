package spream

import (
	"time"
)

type PartitionToken string

const RootPartition PartitionToken = "Parent0"

func (t PartitionToken) asParameter() *string {
	if t == RootPartition {
		return nil
	}
	s := string(t)
	return &s
}

type Partition struct {
	PartitionToken PartitionToken
	StartTimestamp time.Time
}
