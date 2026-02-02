package spream

import "errors"

var (
	// ErrSubscriberClosed is returned by Subscribe when Close is called.
	ErrSubscriberClosed = errors.New("spream: subscriber closed")

	// ErrShutdownAborted is returned when Shutdown is aborted due to context cancellation or timeout.
	ErrShutdownAborted = errors.New("spream: shutdown aborted: in-flight operations did not complete")

	// errAllPartitionsFinished is returned when all partitions have been processed.
	errAllPartitionsFinished = errors.New("all partitions have been finished")

	// errGracefulShutdown is an internal sentinel for normal termination via initiateShutdown or shutdown.
	// This is not exposed because it represents successful completion (Subscribe returns nil).
	errGracefulShutdown = errors.New("graceful shutdown")

	// errTrackerClosed is returned when the tracker is closed.
	errTrackerClosed = errors.New("tracker is closed")
)
