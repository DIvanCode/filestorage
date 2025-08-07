package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/DIvanCode/filestorage/internal/api/client"
	. "github.com/DIvanCode/filestorage/internal/bucket/meta"
	lock "github.com/DIvanCode/filestorage/internal/lib/locker"
	trash "github.com/DIvanCode/filestorage/internal/trasher"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
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

// GetBucket Возвращает абсолютный путь бакета id
// Бакет блокируется в режиме на чтение. Для разблокировки необходимо вызвать unlock()
func (s *Storage) GetBucket(id bucket.ID) (path string, unlock func(), err error) {
	if err = s.locker.ReadLock(id); err != nil {
		return
	}

	path = s.getAbsPath(id)
	if _, err = os.Stat(path); err != nil {
		s.locker.ReadUnlock(id)

		if os.IsNotExist(err) {
			err = ErrBucketNotFound
		}
		return
	}

	unlock = func() {
		s.locker.ReadUnlock(id)
	}

	return
}

// CreateBucket Создаёт бакет id
// Бакет создаётся во временной директории; path - абсолютный путь временной директории
// При вызове функции commit() он перемещается в storage
// При вызове функции abort() он удаляется
func (s *Storage) CreateBucket(
	id bucket.ID,
	trashTime time.Time,
) (path string, commit, abort func() error, err error) {
	if err = s.locker.WriteLock(id); err != nil {
		return
	}

	if s.existsBucket(id) {
		err = ErrBucketAlreadyExists
		return
	}

	path = filepath.Join(s.tmpDir, id.String())
	create := func() error {
		if err = os.MkdirAll(path, 0777); err != nil {
			return err
		}

		meta := BucketMeta{
			BucketID:  id,
			TrashTime: trashTime,
		}

		var f *os.File
		f, err = os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_CREATE|os.O_WRONLY, 0777)
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
		defer s.locker.WriteUnlock(id)
		return os.RemoveAll(path)
	}

	commit = func() error {
		defer s.locker.WriteUnlock(id)
		return os.Rename(path, s.getAbsPath(id))
	}

	if err = create(); err != nil {
		_ = abort()
		return
	}

	return
}

// CreateFile Создаёт файл file в существующем бакете bucketID
// Файл создаётся во временной директории; path - абсолютный путь до временной директории
// При вызове функции commit() он перемещается в storage
// При вызове функции abort() он удаляется
func (s *Storage) CreateFile(bucketID bucket.ID, file string) (path string, commit, abort func() error, err error) {
	if err = s.locker.WriteLock(bucketID); err != nil {
		return
	}

	if !s.existsBucket(bucketID) {
		err = ErrBucketNotFound
		return
	}

	if s.existsFile(bucketID, file) {
		err = ErrFileAlreadyExists
		return
	}

	path = filepath.Join(s.tmpDir, bucketID.String())
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
		defer s.locker.WriteUnlock(bucketID)
		return os.RemoveAll(path)
	}

	commit = func() error {
		defer s.locker.WriteUnlock(bucketID)
		return os.Rename(filepath.Join(path, file), filepath.Join(s.getAbsPath(bucketID), file))
	}

	if err = create(); err != nil {
		_ = abort()
		return
	}

	return
}

// DownloadBucket Скачивает бакет id с указанного endpoint
func (s *Storage) DownloadBucket(
	ctx context.Context,
	endpoint string,
	id bucket.ID,
	trashTime time.Time,
) error {
	path, commit, abort, err := s.CreateBucket(id, trashTime)
	if err != nil && errors.Is(err, ErrBucketAlreadyExists) {
		return nil
	}
	if err != nil {
		return err
	}

	c := client.NewClient(endpoint)
	if err = c.DownloadBucket(ctx, id, path); err != nil {
		_ = abort()
		return err
	}

	if err = commit(); err != nil {
		_ = abort()
		return err
	}

	return nil
}

// DownloadFile Скачивает файл file в существующий бакет bucketID с указанного endpoint
func (s *Storage) DownloadFile(
	ctx context.Context,
	endpoint string,
	bucketID bucket.ID,
	file string,
) error {
	path, commit, abort, err := s.CreateFile(bucketID, file)
	if err != nil && errors.Is(err, ErrFileAlreadyExists) {
		return nil
	}
	if err != nil {
		return err
	}

	c := client.NewClient(endpoint)
	if err := c.DownloadFile(ctx, bucketID, file, path); err != nil {
		_ = abort()
		return err
	}

	if err = commit(); err != nil {
		_ = abort()
		return err
	}
	return nil
}

func (s *Storage) DeleteFile(bucketID bucket.ID, file string) error {
	if err := s.locker.WriteLock(bucketID); err != nil {
		return err
	}
	defer s.locker.WriteUnlock(bucketID)

	if !s.existsBucket(bucketID) {
		return ErrBucketNotFound
	}
	if !s.existsFile(bucketID, file) {
		return ErrFileNotFound
	}

	return os.RemoveAll(filepath.Join(s.getAbsPath(bucketID), file))
}

// GetBucketMeta Возвращает метаинформацию о бакете id
func (s *Storage) GetBucketMeta(id bucket.ID) (meta BucketMeta, err error) {
	if err = s.locker.ReadLock(id); err != nil {
		return
	}
	defer s.locker.ReadUnlock(id)

	path := s.getAbsPath(id)
	if _, err = os.Stat(path); err != nil {
		s.locker.ReadUnlock(id)

		if os.IsNotExist(err) {
			err = ErrBucketNotFound
		}
		return
	}

	f, err := os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_RDONLY, 0777)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	if err = json.NewDecoder(f).Decode(&meta); err != nil {
		return
	}

	return
}

// RemoveBucket Удаляет бакет id
func (s *Storage) RemoveBucket(id bucket.ID) (err error) {
	if err = s.locker.WriteLock(id); err != nil {
		return
	}
	defer s.locker.WriteUnlock(id)

	err = os.RemoveAll(s.getAbsPath(id))
	if err != nil {
		return
	}

	return
}

func (s *Storage) getAbsPath(id bucket.ID) string {
	return filepath.Join(s.rootDir, id.String()[:2], id.String()[:])
}

func (s *Storage) getMetaFile(id bucket.ID) string {
	return id.String()[:] + ".meta.json"
}

func (s *Storage) existsBucket(id bucket.ID) bool {
	path := s.getAbsPath(id)
	_, err := os.Stat(path)
	return err == nil
}

func (s *Storage) existsFile(bucketID bucket.ID, file string) bool {
	path := filepath.Join(s.getAbsPath(bucketID), file)
	_, err := os.Stat(path)
	return err == nil
}
