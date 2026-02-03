package spream

import "errors"

var (
	// ErrShutdown is returned by Subscribe when Shutdown is called.
	ErrShutdown = errors.New("spream: shutdown")

	// ErrClosed is returned by Subscribe when Close is called.
	ErrClosed = errors.New("spream: closed")

	// errAllPartitionsFinished is returned when all partitions have been processed.
	errAllPartitionsFinished = errors.New("all partitions have been finished")

	// errGracefulShutdown is an internal sentinel for graceful shutdown.
	// partition_reader uses this to determine whether to drain.
	errGracefulShutdown = errors.New("graceful shutdown")

	// errTrackerClosed is returned when the tracker is closed.
	errTrackerClosed = errors.New("tracker is closed")
)
