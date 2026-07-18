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
	"sort"
	"time"

	. "github.com/DIvanCode/filestorage/internal/bucket/meta"

	"github.com/DIvanCode/filestorage/internal/api/client"
	lock "github.com/DIvanCode/filestorage/internal/lib/locker"
	"github.com/DIvanCode/filestorage/internal/lib/safepath"
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
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}

	rootDir := filepath.Join(cfg.RootDir, "storage")
	if err := os.MkdirAll(rootDir, 0755); err != nil {
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
		if err := os.MkdirAll(filepath.Join(rootDir, shard), 0755); err != nil {
			return nil, err
		}
	}

	trasher.Start(storage, storage.rootDir)

	return storage, nil
}

func (s *Storage) Shutdown() {
	s.trasher.Stop()
	_ = os.RemoveAll(s.tmpDir)
}

func (s *Storage) ListBuckets(ctx context.Context) ([]bucket.ID, error) {
	shards, err := os.ReadDir(s.rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage root: %w", err)
	}

	buckets := make([]bucket.ID, 0)
	for _, shard := range shards {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if !shard.IsDir() {
			continue
		}

		shardPath := filepath.Join(s.rootDir, shard.Name())
		entries, err := os.ReadDir(shardPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read storage shard %s: %w", shard.Name(), err)
		}

		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			if !entry.IsDir() {
				continue
			}

			var id bucket.ID
			if err := id.FromString(entry.Name()); err != nil {
				continue
			}

			buckets = append(buckets, id)
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].String() < buckets[j].String()
	})

	return buckets, nil
}

// GetBucket Возвращает абсолютный путь бакета id
// extendTTL - длительность продления жизни бакета (без надобности оставьте nil)
// Бакет блокируется в режиме на чтение. Для разблокировки необходимо вызвать unlock()
// НЕ гарантируется консистентность данных при модификации
func (s *Storage) GetBucket(
	ctx context.Context,
	id bucket.ID,
	extendTTL *time.Duration,
) (path string, unlock func(), err error) {
	if err = s.extendTTL(ctx, id, extendTTL); err != nil {
		err = fmt.Errorf("failed to extend bucket ttl: %w", err)
		return
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

	path, err = s.getSafeBucketPath(id)

	return
}

func (s *Storage) GetBucketTrashTime(ctx context.Context, id bucket.ID) (trashTime *time.Time, err error) {
	meta, err := s.GetBucketMeta(ctx, id)
	if err != nil {
		err = fmt.Errorf("failed to get bucket meta: %w", err)
		return
	}

	return meta.TrashTime, nil
}

// GetFile Возвращает абсолютный путь бакета bucketID, в котором лежит файл file
// Бакет и файл блокируются в режиме на чтение. Для разблокировки необходимо вызвать unlock()
// НЕ гарантируется консистентность данных при модификации
func (s *Storage) GetFile(
	ctx context.Context,
	bucketID bucket.ID,
	file string,
	extendTTL *time.Duration,
) (path string, unlock func(), err error) {
	file, _, err = safepath.Resolve(s.getAbsPath(bucketID), file)
	if err != nil {
		err = fmt.Errorf("failed to validate file path: %w", err)
		return
	}
	if err = s.extendTTL(ctx, bucketID, extendTTL); err != nil {
		err = fmt.Errorf("failed to extend bucket ttl: %w", err)
		return
	}

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
		s.locker.ReadUnlock(s.fileLockKey(bucketID, file))
	}
	if err = s.locker.ReadLock(ctx, s.fileLockKey(bucketID, file)); err != nil {
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
	info, _, statErr := safepath.Lstat(path, file)
	if statErr != nil {
		err = statErr
		if os.IsNotExist(err) {
			err = ErrFileNotFound
		} else if errors.Is(err, ErrInvalidPath) {
			err = fmt.Errorf("failed to validate file path: %w", err)
		} else {
			err = fmt.Errorf("failed to get bucket info: %w", err)
		}
		return
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		err = fmt.Errorf("failed to validate file path: %w", ErrInvalidPath)
		return
	}

	return
}

// ReserveBucket Резервирует бакет id
// ttl - время жизни бакета (оставьте nil, если бакет должен жить бессрочно)
// Бакет изначально создаётся во временной директории; path - абсолютный путь временной директории
// Бакет блокируется в режиме на запись. Для разблокировки необходимо вызвать commit() или abort()
// При вызове функции commit() бакет перемещается в storage
// При вызове функции abort() бакет удаляется
func (s *Storage) ReserveBucket(
	ctx context.Context,
	id bucket.ID,
	ttl *time.Duration,
) (path string, commit, abort func() error, err error) {
	bucketUnlocked := false
	unlockBucket := func() {
		if !bucketUnlocked {
			bucketUnlocked = true
			s.locker.WriteUnlock(id)
		}
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
		if err = os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}

		var bucketMeta BucketMeta
		if ttl != nil {
			trashTime := time.Now().Add(*ttl)
			bucketMeta = BucketMeta{
				BucketID:  id,
				TrashTime: &trashTime,
			}
		} else {
			bucketMeta = BucketMeta{
				BucketID: id,
			}
		}

		var f *os.File
		f, err = os.OpenFile(filepath.Join(path, s.getMetaFile(id)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return fmt.Errorf("failed to inspect reserved bucket: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("failed to inspect reserved bucket: %w", ErrInvalidPath)
		}
		metaInfo, _, metaErr := safepath.Lstat(path, s.getMetaFile(id))
		if metaErr != nil {
			return fmt.Errorf("failed to inspect reserved bucket metadata: %w", metaErr)
		}
		if !metaInfo.Mode().IsRegular() {
			return fmt.Errorf("failed to inspect reserved bucket metadata: %w", ErrInvalidPath)
		}
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
	file, _, err = safepath.Resolve(s.getAbsPath(bucketID), file)
	if err != nil {
		err = fmt.Errorf("failed to validate file path: %w", err)
		return
	}

	// read lock bucket
	bucketUnlocked := false
	unlockBucket := func() {
		if !bucketUnlocked {
			bucketUnlocked = true
			s.locker.ReadUnlock(bucketID)
		}
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
	fileUnlocked := false
	unlockFile := func() {
		if !fileUnlocked {
			fileUnlocked = true
			s.locker.WriteUnlock(s.fileLockKey(bucketID, file))
		}
	}
	if err = s.locker.WriteLock(ctx, s.fileLockKey(bucketID, file)); err != nil {
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
		if err = os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}
		if err = safepath.MkdirAll(path, filepath.Dir(file), 0755); err != nil {
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

		info, srcPath, statErr := safepath.Lstat(path, file)
		if statErr != nil {
			return fmt.Errorf("failed to inspect reserved file: %w", statErr)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("failed to inspect reserved file: %w", ErrInvalidPath)
		}

		bucketPath := s.getAbsPath(bucketID)
		if err = safepath.MkdirAll(bucketPath, filepath.Dir(file), 0755); err != nil {
			return fmt.Errorf("failed to create subdirectories in storage: %w", err)
		}
		if _, _, statErr = safepath.Lstat(bucketPath, file); statErr == nil {
			return ErrFileAlreadyExists
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("failed to inspect destination file: %w", statErr)
		}
		_, dstPath, resolveErr := safepath.Resolve(bucketPath, file)
		if resolveErr != nil {
			return fmt.Errorf("failed to resolve destination file: %w", resolveErr)
		}
		if err = os.Rename(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to move file to storage: %w", err)
		}
		if err = os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove temp directory: %w", err)
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
// ttl - время жизни бакета (оставьте nil, если бакет должен жить бессрочно)
// Если бакет существует, то его время жизни продлевается на ttl
func (s *Storage) DownloadBucket(
	ctx context.Context,
	endpoint string,
	id bucket.ID,
	ttl *time.Duration,
) error {
	path, commit, abort, err := s.ReserveBucket(ctx, id, ttl)
	if err != nil && errors.Is(err, ErrBucketAlreadyExists) {
		if err = s.extendTTL(ctx, id, ttl); err != nil {
			return fmt.Errorf("failed to extend bucket ttl: %w", err)
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

	path, err := s.getSafeBucketPath(id)
	if err != nil {
		return
	}

	metaInfo, metaPath, err := safepath.Lstat(path, s.getMetaFile(id))
	if err != nil {
		err = fmt.Errorf("failed to inspect bucket meta file: %w", err)
		return
	}
	if !metaInfo.Mode().IsRegular() {
		err = fmt.Errorf("failed to inspect bucket meta file: %w", ErrInvalidPath)
		return
	}
	f, err := os.Open(metaPath)
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

func (s *Storage) extendTTL(
	ctx context.Context,
	id bucket.ID,
	extendTTL *time.Duration,
) error {
	if extendTTL == nil {
		return nil
	}

	if err := s.locker.WriteLock(ctx, id); err != nil {
		return fmt.Errorf("failed to write lock bucket: %w", err)
	}
	defer s.locker.WriteUnlock(id)

	var bucketMeta BucketMeta

	path, err := s.getSafeBucketPath(id)
	if err != nil {
		return err
	}
	metaInfo, metaPath, err := safepath.Lstat(path, s.getMetaFile(id))
	if err != nil {
		return fmt.Errorf("failed to inspect bucket meta: %w", err)
	}
	if !metaInfo.Mode().IsRegular() {
		return fmt.Errorf("failed to inspect bucket meta: %w", ErrInvalidPath)
	}
	f, err := os.OpenFile(metaPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to read bucket meta: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err = json.NewDecoder(f).Decode(&bucketMeta); err != nil {
		return fmt.Errorf("failed to unmarshal bucket meta: %w", err)
	}

	trashTime := time.Now().Add(*extendTTL)
	bucketMeta.TrashTime = &trashTime

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
	_, err := s.getSafeBucketPath(id)
	return err == nil
}

func (s *Storage) existsFile(bucketID bucket.ID, file string) bool {
	_, _, err := safepath.Lstat(s.getAbsPath(bucketID), file)
	return err == nil

}

func (s *Storage) fileLockKey(bucketID bucket.ID, file string) string {
	return bucketID.String() + "/" + file
}

func (s *Storage) getSafeBucketPath(id bucket.ID) (string, error) {
	path := s.getAbsPath(id)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return path, ErrBucketNotFound
	}
	if err != nil {
		return path, fmt.Errorf("failed to get bucket info: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return path, fmt.Errorf("failed to get bucket info: %w", ErrInvalidPath)
	}
	return path, nil
}
