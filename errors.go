package spream

import "errors"

var (
	// ErrSubscriberClosed is returned by Subscribe when Shutdown or Close is called.
	ErrSubscriberClosed = errors.New("spream: subscriber closed")

	// errAllPartitionsFinished is returned when all partitions have been processed.
	errAllPartitionsFinished = errors.New("all partitions have been finished")

	// errTrackerClosed is returned when the tracker is closed.
	errTrackerClosed = errors.New("tracker is closed")
)
