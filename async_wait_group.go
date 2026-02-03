package spream

import "sync"

// asyncWaitGroup wraps sync.WaitGroup to enable waiting via channels.
// This allows using WaitGroup completion in select statements.
// Must be created with newAsyncWaitGroup.
type asyncWaitGroup struct {
	wg       sync.WaitGroup
	waitOnce sync.Once
	done     chan struct{}
}

// newAsyncWaitGroup creates a new asyncWaitGroup.
func newAsyncWaitGroup() *asyncWaitGroup {
	return &asyncWaitGroup{
		done: make(chan struct{}),
	}
}

// Go starts a goroutine and tracks it.
func (a *asyncWaitGroup) Go(f func()) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		f()
	}()
}

// Wait starts waiting for all goroutines to finish and returns the done channel.
// Safe to call multiple times; only the first call starts the wait goroutine.
func (a *asyncWaitGroup) Wait() <-chan struct{} {
	a.waitOnce.Do(func() {
		go func() {
			a.wg.Wait()
			close(a.done)
		}()
	})
	return a.done
}

// WaitDone returns the done channel without starting the wait goroutine.
// The channel blocks until Wait is called and all tracked goroutines complete.
func (a *asyncWaitGroup) WaitDone() <-chan struct{} {
	return a.done
}
