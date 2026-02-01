package mutex

import (
	"context"
	"sync"
	"time"
)

type SimpleRWMutex struct {
	mu sync.RWMutex
}

func NewSimpleRWMutex() *SimpleRWMutex {
	return &SimpleRWMutex{}
}

func (rw *SimpleRWMutex) ReadLock(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if rw.mu.TryRLock() {
				return nil
			}

			// Small delay to avoid busy waiting
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (rw *SimpleRWMutex) ReadUnlock() {
	rw.mu.RUnlock()
}

func (rw *SimpleRWMutex) WriteLock(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if rw.mu.TryLock() {
				return nil
			}

			// Small delay to avoid busy waiting
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (rw *SimpleRWMutex) WriteUnlock() {
	rw.mu.Unlock()
}
