package spream

import "errors"

// Public errors returned by Subscriber methods.
var (
	// ErrShutdown is returned by Subscribe when Shutdown is called.
	ErrShutdown = errors.New("spream: shutdown")

	// ErrClosed is returned by Subscribe when Close is called.
	ErrClosed = errors.New("spream: closed")
)

// Internal sentinel errors.
var (
	errAllPartitionsFinished = errors.New("all partitions have been finished")
	errGracefulShutdown      = errors.New("graceful shutdown")
	errTrackerClosed         = errors.New("tracker is closed")
)
