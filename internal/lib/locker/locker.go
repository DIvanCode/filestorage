package locker

import (
	"sync"

	errs "github.com/DIvanCode/filestorage/pkg/errors"
)

type Locker struct {
	mu          sync.Mutex
	writeLocked map[any]struct{}
	readLocked  map[any]int
}

func NewLocker() *Locker {
	return &Locker{
		writeLocked: make(map[any]struct{}),
		readLocked:  make(map[any]int),
	}
}

func (l *Locker) ReadLock(key any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.writeLocked[key]; ok {
		return errs.ErrWriteLocked
	}

	l.readLocked[key]++
	return nil
}

func (l *Locker) ReadUnlock(key any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.readLocked[key]--
	if l.readLocked[key] == 0 {
		delete(l.readLocked, key)
	}
}

func (l *Locker) WriteLock(key any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.writeLocked[key]; ok {
		return errs.ErrWriteLocked
	}
	if l.readLocked[key] > 0 {
		return errs.ErrReadLocked
	}

	l.writeLocked[key] = struct{}{}
	return nil
}

func (l *Locker) WriteUnlock(key any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.writeLocked, key)
}
