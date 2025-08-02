package locker

import (
	errs "github.com/DIvanCode/filestorage/pkg/errors"
	"sync"
)

type Locker struct {
	mu          sync.Mutex
	writeLocked map[interface{}]struct{}
	readLocked  map[interface{}]int
}

func NewLocker() *Locker {
	return &Locker{
		writeLocked: make(map[interface{}]struct{}),
		readLocked:  make(map[interface{}]int),
	}
}

func (l *Locker) ReadLock(key interface{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.writeLocked[key]; ok {
		return errs.ErrWriteLocked
	}

	l.readLocked[key]++
	return nil
}

func (l *Locker) ReadUnlock(key interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.readLocked[key]--
	if l.readLocked[key] == 0 {
		delete(l.readLocked, key)
	}
}

func (l *Locker) WriteLock(key interface{}) error {
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

func (l *Locker) WriteUnlock(key interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.writeLocked, key)
}
