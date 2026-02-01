package locker

import (
	"context"
	"sync"

	"github.com/DIvanCode/filestorage/internal/lib/mutex"
)

type Locker struct {
	locks sync.Map // map[any]*SimpleRWMutex
}

func NewLocker() *Locker {
	return &Locker{}
}

func (locker *Locker) ReadLock(ctx context.Context, key any) error {
	value, _ := locker.locks.LoadOrStore(key, mutex.NewSimpleRWMutex())
	mutex := value.(*mutex.SimpleRWMutex)
	return mutex.ReadLock(ctx)
}

func (locker *Locker) ReadUnlock(key any) {
	if value, ok := locker.locks.Load(key); ok {
		mutex := value.(*mutex.SimpleRWMutex)
		mutex.ReadUnlock()
	}
}

func (locker *Locker) WriteLock(ctx context.Context, key any) error {
	value, _ := locker.locks.LoadOrStore(key, mutex.NewSimpleRWMutex())
	mutex := value.(*mutex.SimpleRWMutex)
	return mutex.WriteLock(ctx)
}

func (locker *Locker) WriteUnlock(key any) {
	if value, ok := locker.locks.Load(key); ok {
		mutex := value.(*mutex.SimpleRWMutex)
		mutex.WriteUnlock()
	}
}
