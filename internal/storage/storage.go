package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/DIvanCode/filestorage/internal/api/client"
	"github.com/DIvanCode/filestorage/internal/artifact"
	lock "github.com/DIvanCode/filestorage/internal/locker"
	trash "github.com/DIvanCode/filestorage/internal/trasher"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"github.com/DIvanCode/filestorage/pkg/config"
	errs "github.com/DIvanCode/filestorage/pkg/errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

type Storage struct {
	rootDir string
	tmpDir  string

	trasher *trash.Trasher
	locker  *lock.Locker

	log *slog.Logger
}

func NewStorage(log *slog.Logger, cfg config.Config) (*Storage, error) {
	tmpDir := filepath.Join(cfg.RootDir, "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmpDir, 0777); err != nil {
		return nil, err
	}

	rootDir := filepath.Join(cfg.RootDir, "storage")
	if err := os.MkdirAll(rootDir, 0777); err != nil {
		return nil, err
	}

	trasher, err := trash.NewTrasher(log, cfg.Trasher)
	if err != nil {
		return nil, err
	}

	locker := lock.NewLocker()

	storage := &Storage{
		rootDir: rootDir,
		tmpDir:  tmpDir,

		trasher: trasher,
		locker:  locker,

		log: log,
	}

	for i := range 256 {
		shard := hex.EncodeToString([]byte{uint8(i)})
		if err := os.MkdirAll(filepath.Join(rootDir, shard), 0777); err != nil {
			return nil, err
		}
	}

	trasher.Start(storage, storage.rootDir)

	return storage, nil
}

func (s *Storage) Shutdown() {
	s.trasher.Stop()
}

// GetArtifact Возвращает абсолютный путь артефакта artifactID
// Артефакт блокируется в режиме на чтение. Для разблокировки необходимо вызвать unlock()
func (s *Storage) GetArtifact(artifactID id.ID) (path string, unlock func(), err error) {
	if err = s.locker.ReadLock(artifactID); err != nil {
		return
	}

	path = s.getAbsPath(artifactID)
	if _, err = os.Stat(path); err != nil {
		s.locker.ReadUnlock(artifactID)

		if os.IsNotExist(err) {
			err = errs.ErrNotFound
		}
		return
	}

	unlock = func() {
		s.locker.ReadUnlock(artifactID)
	}

	return
}

// CreateArtifact Создаёт артефакт artifactID
// Артефакт создаётся во временной директории; path - абсолютный путь временной директории
// При вызове функции commit() он перемещается в storage
// При вызове функции abort() он удаляется
func (s *Storage) CreateArtifact(
	artifactID id.ID,
	trashTime time.Time,
) (path string, commit, abort func() error, err error) {
	if err = s.locker.WriteLock(artifactID); err != nil {
		return
	}

	if s.existsArtifact(artifactID) {
		err = errs.ErrAlreadyExists
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

		var f *os.File
		f, err = os.OpenFile(filepath.Join(path, meta.ID.String()[:]+".meta.json"), os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()

		var bytes []byte
		bytes, err = json.Marshal(meta)
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
		defer s.locker.WriteUnlock(artifactID)
		return os.RemoveAll(path)
	}

	commit = func() error {
		defer s.locker.WriteUnlock(artifactID)
		return os.Rename(path, s.getAbsPath(artifactID))
	}

	if err = create(); err != nil {
		_ = abort()
		return
	}

	return
}

// CreateFile Создаёт файл file в существующем артефакте artifactID
// Файл создаётся во временной директории; path - абсолютный путь до временной директории
// При вызове функции commit() он перемещается в storage
// При вызове функции abort() он удаляется
func (s *Storage) CreateFile(artifactID id.ID, file string) (path string, commit, abort func() error, err error) {
	if err = s.locker.WriteLock(artifactID); err != nil {
		return
	}

	if !s.existsArtifact(artifactID) {
		err = errs.ErrNotFound
		return
	}

	if s.existsFile(artifactID, file) {
		err = errs.ErrAlreadyExists
		return
	}

	path = filepath.Join(s.tmpDir, artifactID.String())
	create := func() error {
		if err = os.MkdirAll(path, 0777); err != nil {
			return err
		}

		var f *os.File
		f, err = os.Create(filepath.Join(path, file))
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()

		return nil
	}

	abort = func() error {
		defer s.locker.WriteUnlock(artifactID)
		return os.RemoveAll(path)
	}

	commit = func() error {
		defer s.locker.WriteUnlock(artifactID)
		return os.Rename(filepath.Join(path, file), filepath.Join(s.getAbsPath(artifactID), file))
	}

	if err = create(); err != nil {
		_ = abort()
		return
	}

	return
}

// DownloadArtifact Скачивает артефакт artifactID с указанного endpoint
func (s *Storage) DownloadArtifact(
	ctx context.Context,
	endpoint string,
	artifactID id.ID,
	trashTime time.Time,
) error {
	path, commit, abort, err := s.CreateArtifact(artifactID, trashTime)
	if err != nil && errors.Is(err, errs.ErrAlreadyExists) {
		return nil
	}
	if err != nil {
		return err
	}

	c := client.NewClient(endpoint)
	if err = c.DownloadArtifact(ctx, artifactID, path); err != nil {
		_ = abort()
		return err
	}

	if err = commit(); err != nil {
		_ = abort()
		return err
	}

	return nil
}

// DownloadFile Скачивает файл file в существующем артефакте artifactID с указанного endpoint
func (s *Storage) DownloadFile(
	ctx context.Context,
	endpoint string,
	artifactID id.ID,
	file string,
) error {
	path, commit, abort, err := s.CreateFile(artifactID, file)
	if err != nil && errors.Is(err, errs.ErrAlreadyExists) {
		return nil
	}
	if err != nil {
		return err
	}

	c := client.NewClient(endpoint)
	if err := c.DownloadFile(ctx, artifactID, path, file); err != nil {
		_ = abort()
		return err
	}

	if err = commit(); err != nil {
		_ = abort()
		return err
	}
	return nil
}

// GetArtifactMeta Возвращает метаинформацию об артефакте artifactID
func (s *Storage) GetArtifactMeta(artifactID id.ID) (meta artifact.Meta, err error) {
	if err = s.locker.ReadLock(artifactID); err != nil {
		return
	}
	defer s.locker.ReadUnlock(artifactID)

	path := s.getAbsPath(artifactID)
	if _, err = os.Stat(path); err != nil {
		s.locker.ReadUnlock(artifactID)

		if os.IsNotExist(err) {
			err = errs.ErrNotFound
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

// RemoveArtifact Удаляет артефакт artifactID
func (s *Storage) RemoveArtifact(artifactID id.ID) (err error) {
	if err = s.locker.WriteLock(artifactID); err != nil {
		return
	}
	defer s.locker.WriteUnlock(artifactID)

	err = os.RemoveAll(s.getAbsPath(artifactID))
	if err != nil {
		return
	}

	return
}

func (s *Storage) getAbsPath(artifactID id.ID) string {
	return filepath.Join(s.rootDir, artifactID.String()[:2], artifactID.String()[:])
}

func (s *Storage) existsArtifact(artifactID id.ID) bool {
	path := s.getAbsPath(artifactID)
	_, err := os.Stat(path)
	return err == nil
}

func (s *Storage) existsFile(artifactID id.ID, file string) bool {
	path := filepath.Join(s.getAbsPath(artifactID), file)
	_, err := os.Stat(path)
	return err == nil
}
