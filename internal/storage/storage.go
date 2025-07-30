package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"filestorage/internal/api/client"
	"filestorage/internal/artifact"
	trasher2 "filestorage/internal/trasher"
	. "filestorage/pkg/config"
	. "filestorage/pkg/errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Storage struct {
	rootDir string
	tmpDir  string

	trasher *trasher2.Trasher

	mu          sync.Mutex
	writeLocked map[artifact.ID]struct{}
	readLocked  map[artifact.ID]int

	log *slog.Logger
}

func NewStorage(log *slog.Logger, root string, cfg Config) (*Storage, error) {
	tmpDir := filepath.Join(root, "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmpDir, 0777); err != nil {
		return nil, err
	}

	rootDir := filepath.Join(root, "storage")
	if err := os.MkdirAll(rootDir, 0777); err != nil {
		return nil, err
	}

	thr, err := trasher2.NewTrasher(log, cfg.Trasher)
	if err != nil {
		return nil, err
	}

	storage := &Storage{
		rootDir: rootDir,
		tmpDir:  tmpDir,

		trasher: thr,

		writeLocked: make(map[artifact.ID]struct{}),
		readLocked:  make(map[artifact.ID]int),

		log: log,
	}

	for i := range 256 {
		shard := hex.EncodeToString([]byte{uint8(i)})
		if err := os.MkdirAll(filepath.Join(rootDir, shard), 0777); err != nil {
			return nil, err
		}
	}

	thr.Start(storage, storage.rootDir)

	return storage, nil
}

func (s *Storage) Shutdown() {
	s.trasher.Stop()
}

// GetArtifact Возвращает абсолютный путь артефакта
// Артефакт блокируется в режиме на чтение. Для разблокировки необходимо вызвать unlock()
func (s *Storage) GetArtifact(artifactID artifact.ID) (path string, unlock func(), err error) {
	if err = s.readLock(artifactID); err != nil {
		return
	}

	path = s.getAbsPath(artifactID)
	if _, err = os.Stat(path); err != nil {
		s.readUnlock(artifactID)

		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return
	}

	unlock = func() {
		s.readUnlock(artifactID)
	}

	return
}

// CreateArtifact Создаёт артефакт с указанным ID
// Артефакт создаётся во временной директории; path - абсолютный путь временной директории
// При вызове функции commit() он перемещается в storage
// При вызове функции abort() он удаляется
func (s *Storage) CreateArtifact(
	artifactID artifact.ID,
	trashTime time.Time,
) (path string, commit, abort func() error, err error) {
	if err = s.writeLock(artifactID, true); err != nil {
		return
	}

	path = filepath.Join(s.tmpDir, artifactID.String())
	create := func() error {
		if err = os.MkdirAll(path, 0777); err != nil {
			return err
		}

		meta := artifact.Meta{
			ID:        artifactID,
			TrashTime: trashTime,
		}

		f, err := os.OpenFile(filepath.Join(path, meta.ID.String()[:]+".meta.json"), os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()

		bytes, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		_, err = f.Write(bytes)
		if err != nil {
			return err
		}

		return nil
	}

	abort = func() error {
		defer s.writeUnlock(artifactID)
		return os.RemoveAll(path)
	}

	commit = func() error {
		defer s.writeUnlock(artifactID)
		return os.Rename(path, s.getAbsPath(artifactID))
	}

	if err = create(); err != nil {
		_ = abort()
		return
	}

	return
}

// DownloadArtifact Скачивает артефакт ID с указанного endpoint
func (s *Storage) DownloadArtifact(
	ctx context.Context,
	endpoint string,
	artifactID artifact.ID,
	trashTime time.Time,
) error {
	_, unlock, err := s.GetArtifact(artifactID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	if err == nil {
		unlock()
		return nil
	}

	path, commit, abort, err := s.CreateArtifact(artifactID, trashTime)
	if err != nil {
		return err
	}

	c := client.NewClient(endpoint)
	if err = c.Download(ctx, artifactID, path); err != nil {
		_ = abort()
		return err
	}

	if err = commit(); err != nil {
		_ = abort()
		return err
	}

	return nil
}

// GetArtifactMeta Возвращает метаинформацию об артефакте
func (s *Storage) GetArtifactMeta(artifactID artifact.ID) (meta artifact.Meta, err error) {
	if err = s.readLock(artifactID); err != nil {
		return
	}
	defer s.readUnlock(artifactID)

	path := s.getAbsPath(artifactID)
	if _, err = os.Stat(path); err != nil {
		s.readUnlock(artifactID)

		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return
	}

	f, err := os.OpenFile(filepath.Join(path, artifactID.String()[:]+".meta.json"), os.O_RDONLY, 0777)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	if err = json.NewDecoder(f).Decode(&meta); err != nil {
		return
	}

	return
}

// RemoveArtifact Удаляет артефакт
func (s *Storage) RemoveArtifact(artifactID artifact.ID) (err error) {
	if err = s.writeLock(artifactID, false); err != nil {
		return
	}
	defer s.writeUnlock(artifactID)

	err = os.RemoveAll(s.getAbsPath(artifactID))
	if err != nil {
		return
	}

	return
}

func (s *Storage) getAbsPath(artifactID artifact.ID) string {
	return filepath.Join(s.rootDir, artifactID.String()[:2], artifactID.String()[:])
}

func (s *Storage) readLock(artifactID artifact.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.writeLocked[artifactID]; ok {
		return ErrWriteLocked
	}

	s.readLocked[artifactID]++
	return nil
}

func (s *Storage) readUnlock(artifactID artifact.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readLocked[artifactID]--
	if s.readLocked[artifactID] == 0 {
		delete(s.readLocked, artifactID)
	}
}

func (s *Storage) writeLock(artifactID artifact.ID, create bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := os.Stat(s.getAbsPath(artifactID))
	if !os.IsNotExist(err) && err != nil {
		return err
	} else if err == nil && create {
		return ErrAlreadyExists
	}

	if _, ok := s.writeLocked[artifactID]; ok {
		return ErrWriteLocked
	}
	if s.readLocked[artifactID] > 0 {
		return ErrReadLocked
	}

	s.writeLocked[artifactID] = struct{}{}
	return nil
}

func (s *Storage) writeUnlock(artifactID artifact.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.writeLocked, artifactID)
}
