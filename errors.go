package spream

import "errors"

var (
	// errAllPartitionsFinished is returned when all partitions have been processed.
	errAllPartitionsFinished = errors.New("all partitions have been finished")

	// errTrackerClosed is returned when the tracker is closed.
	errTrackerClosed = errors.New("tracker is closed")
)
