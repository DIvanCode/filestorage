package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/DIvanCode/filestorage/internal/api/client"
	. "github.com/DIvanCode/filestorage/internal/bucket/meta"
	lock "github.com/DIvanCode/filestorage/internal/lib/locker"
	trash "github.com/DIvanCode/filestorage/internal/trasher"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/google/uuid"
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
// addTTL - длительность продления жизни бакета (без надобности оставьте nil)
// Бакет блокируется в режиме на чтение. Для разблокировки необходимо вызвать unlock()
// НЕ гарантируется консистентность данных при модификации данных
func (s *Storage) GetBucket(
	ctx context.Context,
	id bucket.ID,
	addTTL *time.Duration,
) (path string, unlock func(), err error) {
	if addTTL != nil {
		if err = s.addTTL(ctx, id, *addTTL); err != nil {
			err = fmt.Errorf("failed to add bucket ttl: %w", err)
			return
		}
	}

	unlockBucket := func() {
		s.locker.ReadUnlock(id)
	}
	if err = s.locker.ReadLock(ctx, id); err != nil {
		err = fmt.Errorf("failed to read lock bucket: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockBucket()
		}
	}()

	unlock = func() {
		unlockBucket()
	}

	path = s.getAbsPath(id)
	if _, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			err = ErrBucketNotFound
		} else {
			err = fmt.Errorf("failed to get bucket info: %w", err)
		}
		return
	}

	return
}

// GetFile Возвращает абсолютный путь бакета bucketID, в котором лежит файл file
// Бакет и файл блокируются в режиме на чтение. Для разблокировки необходимо вызвать unlock()
// НЕ гарантируется консистентность данных при модификации данных
func (s *Storage) GetFile(
	ctx context.Context,
	bucketID bucket.ID,
	file string,
) (path string, unlock func(), err error) {
	// read lock bucket
	unlockBucket := func() {
		s.locker.ReadUnlock(bucketID)
	}
	if err = s.locker.ReadLock(ctx, bucketID); err != nil {
		err = fmt.Errorf("failed to read lock bucket: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockBucket()
		}
	}()

	// read lock file in bucket
	unlockFile := func() {
		s.locker.ReadUnlock(bucketID.String() + file)
	}
	if err = s.locker.ReadLock(ctx, bucketID.String()+file); err != nil {
		err = fmt.Errorf("failed to read lock file in bucket: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockFile()
		}
	}()

	unlock = func() {
		unlockBucket()
		unlockFile()
	}

	path = s.getAbsPath(bucketID)
	if _, err = os.Stat(filepath.Join(s.getAbsPath(bucketID), file)); err != nil {
		if os.IsNotExist(err) {
			err = ErrFileNotFound
		} else {
			err = fmt.Errorf("failed to get bucket info: %w", err)
		}
		return
	}

	return
}

// ReserveBucket Разервирует бакет id
// ttl - время жизни бакета
// Бакет изначально создаётся во временной директории; path - абсолютный путь временной директории
// Бакет блокируется в режиме на запись. Для разблокировки необходимо вызвать commit() или abort()
// При вызове функции commit() бакет перемещается в storage
// При вызове функции abort() бакет удаляется
func (s *Storage) ReserveBucket(
	ctx context.Context,
	id bucket.ID,
	ttl time.Duration,
) (path string, commit, abort func() error, err error) {
	unlockBucket := func() {
		s.locker.WriteUnlock(id)
	}
	if err = s.locker.WriteLock(ctx, id); err != nil {
		err = fmt.Errorf("failed to write lock bucket: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockBucket()
		}
	}()

	if s.existsBucket(id) {
		err = ErrBucketAlreadyExists
		return
	}

	path = filepath.Join(s.tmpDir, id.String())
	create := func() error {
		if err = os.MkdirAll(path, 0777); err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}

		bucketMeta := BucketMeta{
			BucketID:  id,
			TrashTime: time.Now().Add(ttl),
		}

		var f *os.File
		f, err = os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return fmt.Errorf("failed to create bucket meta: %w", err)
		}
		defer func() { _ = f.Close() }()

		var bytes []byte
		bytes, err = json.Marshal(bucketMeta)
		if err != nil {
			return fmt.Errorf("failed to marshal bucket meta: %w", err)
		}

		if _, err = f.Write(bytes); err != nil {
			return fmt.Errorf("failed to write bucket meta: %w", err)
		}

		return nil
	}
	remove := func() error {
		if err = os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove temp directory: %w", err)
		}
		return nil
	}

	abort = func() error {
		defer unlockBucket()
		return remove()
	}

	commit = func() error {
		defer unlockBucket()
		if err = os.Rename(path, s.getAbsPath(id)); err != nil {
			return fmt.Errorf("failed to move bucket to storage: %w", err)
		}
		return nil
	}

	if err = create(); err != nil {
		_ = remove()
		err = fmt.Errorf("failed to create bucket: %w", err)
		return
	}

	return
}

// ReserveFile Резервирует файл file в существующем бакете bucketID
// Бакет блокируется в режиме на запись. Для разблокировки необходимо вызвать commit() или abort()
// Файл резервируется во временной директории; path - абсолютный путь до временной директории
// При вызове функции commit() файл перемещается в storage
// При вызове функции abort() файл удаляется
func (s *Storage) ReserveFile(
	ctx context.Context,
	bucketID bucket.ID,
	file string,
) (path string, commit, abort func() error, err error) {
	// read lock bucket
	unlockBucket := func() {
		s.locker.ReadUnlock(bucketID)
	}
	if err = s.locker.ReadLock(ctx, bucketID); err != nil {
		err = fmt.Errorf("failed to read lock bucket: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockBucket()
		}
	}()

	if !s.existsBucket(bucketID) {
		err = ErrBucketNotFound
		return
	}

	// write lock file
	unlockFile := func() {
		s.locker.WriteUnlock(bucketID.String() + file)
	}
	if err = s.locker.WriteLock(ctx, bucketID.String()+file); err != nil {
		err = fmt.Errorf("failed to write lock file: %w", err)
		return
	}
	defer func() {
		if err != nil {
			unlockFile()
		}
	}()

	if s.existsFile(bucketID, file) {
		err = ErrFileAlreadyExists
		return
	}

	path = filepath.Join(s.tmpDir, bucketID.String()+"_"+uuid.New().String())
	create := func() error {
		if err = os.MkdirAll(path, 0777); err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		if err = os.MkdirAll(filepath.Dir(filepath.Join(path, file)), 0777); err != nil {
			return fmt.Errorf("failed to create temp subdirectories: %w", err)
		}
		return nil
	}
	remove := func() error {
		return os.RemoveAll(path)
	}

	abort = func() error {
		defer unlockBucket()
		defer unlockFile()
		return remove()
	}

	commit = func() error {
		defer unlockBucket()
		defer unlockFile()

		dstPath := filepath.Join(s.getAbsPath(bucketID), file)
		if err = os.MkdirAll(filepath.Dir(dstPath), 0777); err != nil {
			return fmt.Errorf("failed to create subdirectories in storage: %w", err)
		}
		if err = os.Rename(filepath.Join(path, file), dstPath); err != nil {
			return fmt.Errorf("failed to move file to storage: %w", err)
		}

		return nil
	}

	if err = create(); err != nil {
		_ = remove()
		err = fmt.Errorf("failed to create file: %w", err)
		return
	}

	return
}

// DownloadBucket Скачивает бакет id с указанного endpoint
// ttl - время жизни бакета
// Если бакет существует, то его время жизни продлевается на ttl
func (s *Storage) DownloadBucket(
	ctx context.Context,
	endpoint string,
	id bucket.ID,
	ttl time.Duration,
) error {
	path, commit, abort, err := s.ReserveBucket(ctx, id, ttl)
	if err != nil && errors.Is(err, ErrBucketAlreadyExists) {
		if err = s.addTTL(ctx, id, ttl); err != nil {
			return fmt.Errorf("failed to add bucket ttl: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to reserve bucket: %w", err)
	}

	c := client.NewClient(endpoint)
	if err = c.DownloadBucket(ctx, id, path); err != nil {
		_ = abort()
		return fmt.Errorf("failed to download bucket: %w", err)
	}

	if err = commit(); err != nil {
		_ = abort()
		return fmt.Errorf("failed to commit bucket creation: %w", err)
	}

	return nil
}

// DownloadFile Скачивает файл file в существующий бакет bucketID с указанного endpoint
// Если файл уже существует, то ничего не происходит
func (s *Storage) DownloadFile(
	ctx context.Context,
	endpoint string,
	bucketID bucket.ID,
	file string,
) error {
	path, commit, abort, err := s.ReserveFile(ctx, bucketID, file)
	if err != nil && errors.Is(err, ErrFileAlreadyExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to reserve file: %w", err)
	}

	c := client.NewClient(endpoint)
	if err := c.DownloadFile(ctx, bucketID, file, path); err != nil {
		_ = abort()
		return fmt.Errorf("failed to download file: %w", err)
	}

	if err = commit(); err != nil {
		_ = abort()
		return fmt.Errorf("failed to commit file creation: %w", err)
	}

	return nil
}

// GetBucketMeta Возвращает метаинформацию о бакете id
func (s *Storage) GetBucketMeta(
	ctx context.Context,
	id bucket.ID) (meta BucketMeta, err error) {
	unlockBucket := func() {
		s.locker.ReadUnlock(id)
	}
	if err = s.locker.ReadLock(ctx, id); err != nil {
		err = fmt.Errorf("failed to read lock bucket: %w", err)
		return
	}
	defer unlockBucket()

	path := s.getAbsPath(id)
	if _, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			err = ErrBucketNotFound
		} else {
			err = fmt.Errorf("failed to get bucket info: %w", err)
		}
		return
	}

	f, err := os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_RDONLY, 0777)
	if err != nil {
		err = fmt.Errorf("failed to open bucket meta file: %w", err)
		return
	}
	defer func() { _ = f.Close() }()

	if err = json.NewDecoder(f).Decode(&meta); err != nil {
		err = fmt.Errorf("failed to decode bucket meta: %w", err)
		return
	}

	return
}

// RemoveBucket Удаляет бакет id
func (s *Storage) RemoveBucket(
	ctx context.Context,
	id bucket.ID,
) (err error) {
	unlockBucket := func() {
		s.locker.WriteUnlock(id)
	}
	if err = s.locker.WriteLock(ctx, id); err != nil {
		err = fmt.Errorf("failed to write lock bucket: %w", err)
		return
	}
	defer unlockBucket()

	if err = os.RemoveAll(s.getAbsPath(id)); err != nil {
		err = fmt.Errorf("failed to remove directory: %w", err)
		return
	}

	return
}

func (s *Storage) addTTL(
	ctx context.Context,
	id bucket.ID,
	addTTL time.Duration,
) error {
	if err := s.locker.WriteLock(ctx, id); err != nil {
		return fmt.Errorf("failed to write lock bucket: %w", err)
	}
	defer s.locker.WriteUnlock(id)

	var bucketMeta BucketMeta

	path := s.getAbsPath(id)
	f, err := os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_RDWR, 0777)
	if err != nil {
		return fmt.Errorf("failed to read bucket meta: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err = json.NewDecoder(f).Decode(&bucketMeta); err != nil {
		return fmt.Errorf("failed to unmarshal bucket meta: %w", err)
	}

	bucketMeta.TrashTime = time.Now().Add(addTTL)

	var bytes []byte
	bytes, err = json.Marshal(bucketMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket meta: %w", err)
	}

	if _, err = f.Write(bytes); err != nil {
		return fmt.Errorf("failed to write bucket meta: %w", err)
	}

	return nil
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
