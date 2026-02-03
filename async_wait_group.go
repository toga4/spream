package spream

import "sync"

// asyncWaitGroup wraps sync.WaitGroup to enable waiting via channels.
// This allows using WaitGroup completion in select statements.
type asyncWaitGroup struct {
	wg   sync.WaitGroup
	once sync.Once
	done chan struct{}
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
	a.once.Do(func() {
		a.done = make(chan struct{})
		go func() {
			a.wg.Wait()
			close(a.done)
		}()
	})
	return a.done
}

// WaitDone returns the done channel without starting the wait.
// Returns nil if Wait has not been called yet.
// A nil channel blocks forever in select, which is useful for
// conditional waiting.
func (a *asyncWaitGroup) WaitDone() <-chan struct{} {
	return a.done
}
