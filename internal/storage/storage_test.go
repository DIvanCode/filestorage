package storage

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	*Storage
	tmpDir string
}

func (s *testStorage) cleanup() {
	s.Shutdown()
	_ = os.RemoveAll(s.tmpDir)
}

func newTestStorage(t *testing.T) *testStorage {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		RootDir: tmpDir,
		Trasher: config.TrasherConfig{
			Workers:                  1,
			CollectorIterationsDelay: 60,
			WorkerIterationsDelay:    60,
		},
	}

	storage, err := NewStorage(log, cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
	}
	require.NoError(t, err)

	s := &testStorage{Storage: storage, tmpDir: tmpDir}
	t.Cleanup(s.cleanup)
	return s
}

func newBucketID(t *testing.T, idNum int) bucket.ID {
	var id bucket.ID

	idStr := strconv.Itoa(idNum)
	for len(idStr) < len(id) {
		idStr = "0" + idStr
	}

	require.NoError(t, id.FromString(idStr))
	return id
}

func reserveBucket(t *testing.T, s *testStorage, id bucket.ID, ttl time.Duration) {
	_, commit, _, err := s.ReserveBucket(context.Background(), id, &ttl)
	require.NoError(t, err)
	require.NoError(t, commit())
}

func Test_Shutdown_RemovesTmpDir(t *testing.T) {
	rootDir := t.TempDir()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		RootDir: rootDir,
		Trasher: config.TrasherConfig{
			Workers:                  1,
			CollectorIterationsDelay: 60,
			WorkerIterationsDelay:    60,
		},
	}

	storage, err := NewStorage(log, cfg)
	require.NoError(t, err)

	tmpDir := filepath.Join(rootDir, "tmp")
	require.DirExists(t, tmpDir)

	storage.Shutdown()

	require.NoDirExists(t, tmpDir)
}

func Test_ListBuckets(t *testing.T) {
	s := newTestStorage(t)

	firstID := newBucketID(t, 2)
	secondID := newBucketID(t, 1)
	reserveBucket(t, s, firstID, time.Minute)
	reserveBucket(t, s, secondID, time.Minute)

	buckets, err := s.ListBuckets(context.Background())
	require.NoError(t, err)

	require.Equal(t, []bucket.ID{secondID, firstID}, buckets)
}

func Test_GetBucket_NotFound(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	path, _, err := s.GetBucket(context.Background(), bucketID, nil)
	require.ErrorIs(t, err, ErrBucketNotFound)
	assert.NotNil(t, path)
}

func Test_GetBucket_ParallelRead(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	reserveBucket(t, s, bucketID, time.Minute)

	path, unlock1, err := s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock1)
	defer unlock1()

	path, unlock2, err := s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock2)
	defer unlock2()
}

func Test_GetBucket_WriteLock(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	path, unlock, err := s.GetBucket(ctx, bucketID, nil)
	require.ErrorIs(t, err, ctx.Err())
	assert.Nil(t, unlock)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err = s.ReserveBucket(ctx, bucketID, &ttl)
	require.ErrorIs(t, err, ctx.Err())

	require.NoError(t, commit())

	path, unlock, err = s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)
	defer unlock()
}

func Test_ReserveBucket_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	reserveBucket(t, s, bucketID, ttl)

	_, _, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.ErrorIs(t, err, ErrBucketAlreadyExists)
}

func Test_ReserveBucket_ReserveFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetBucket(context.Background(), bucketID, nil)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	path, unlock, err = s.GetFile(context.Background(), bucketID, "a.txt", nil)
	defer unlock()
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_ReserveFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)
	require.NoError(t, commit())

	path, commit, _, err := s.ReserveFile(context.Background(), bucketID, "a.txt")
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	_, err = f.WriteString("aaa")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	bytes, err := os.ReadFile(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "aaa", string(bytes))
}

func Test_ReserveFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	_, _, _, err = s.ReserveFile(context.Background(), bucketID, "a.txt")
	require.ErrorIs(t, err, ErrFileAlreadyExists)
}

func Test_ReserveFile_BucketNotFound(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	_, _, _, err := s.ReserveFile(context.Background(), bucketID, "a.txt")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func Test_RemoveBucket_NotExisting(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	err := s.RemoveBucket(context.Background(), bucketID)
	require.NoError(t, err)
}

func Test_RemoveBucket_Removed(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	reserveBucket(t, s, bucketID, time.Minute)

	path, unlock, err := s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	unlock()

	err = s.RemoveBucket(context.Background(), bucketID)
	require.NoError(t, err)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_ReserveBucket_Abort(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	path, _, abort, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)
	require.NoError(t, abort())

	_, _, err = s.GetBucket(context.Background(), bucketID, nil)
	require.ErrorIs(t, err, ErrBucketNotFound)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_GetBucketMeta(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	expectedTrashTime := time.Now().Add(time.Minute)
	ttl := time.Minute

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)
	require.NoError(t, commit())

	meta, err := s.GetBucketMeta(context.Background(), bucketID)
	require.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, bucketID, meta.BucketID)
	assert.NotNil(t, meta.TrashTime)
	assert.True(t, expectedTrashTime.Before(*meta.TrashTime))

	_, _, err = s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
}

func Test_DownloadBucket_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DownloadBucket(context.Background(), "some-endpoint", bucketID, &ttl)
	require.NoError(t, err)
}

func Test_DownloadFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	ttl := time.Minute

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DownloadFile(context.Background(), "some-endpoint", bucketID, "a.txt")
	require.NoError(t, err)
}

func Test_FileOperationsRejectUnsafePaths(t *testing.T) {
	s := newTestStorage(t)
	bucketID := newBucketID(t, 1)
	reserveBucket(t, s, bucketID, time.Minute)

	unsafePaths := []string{
		"../outside.txt",
		"nested/../../outside.txt",
		`..\outside.txt`,
		filepath.Join(s.tmpDir, "absolute.txt"),
		"nul\x00.txt",
	}

	for _, file := range unsafePaths {
		t.Run(file, func(t *testing.T) {
			_, _, err := s.GetFile(context.Background(), bucketID, file, nil)
			require.ErrorIs(t, err, ErrInvalidPath)

			_, _, _, err = s.ReserveFile(context.Background(), bucketID, file)
			require.ErrorIs(t, err, ErrInvalidPath)

			err = s.DownloadFile(context.Background(), "http://localhost:1", bucketID, file)
			require.ErrorIs(t, err, ErrInvalidPath)
		})
	}
}

func Test_GetAndCommitFileRejectSymlinkParent(t *testing.T) {
	s := newTestStorage(t)
	bucketID := newBucketID(t, 1)
	reserveBucket(t, s, bucketID, time.Minute)

	bucketPath := s.getAbsPath(bucketID)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0600))
	if err := os.Symlink(outside, filepath.Join(bucketPath, "link")); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	_, _, err := s.GetFile(context.Background(), bucketID, "link/secret.txt", nil)
	require.ErrorIs(t, err, ErrInvalidPath)

	path, commit, abort, err := s.ReserveFile(context.Background(), bucketID, "link/new.txt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = abort() })
	require.NoError(t, os.WriteFile(filepath.Join(path, "link", "new.txt"), []byte("new"), 0600))
	require.ErrorIs(t, commit(), ErrInvalidPath)
	require.NoFileExists(t, filepath.Join(outside, "new.txt"))
}

func Test_ReserveFileRejectsSymlinkContent(t *testing.T) {
	s := newTestStorage(t)
	bucketID := newBucketID(t, 1)
	reserveBucket(t, s, bucketID, time.Minute)

	path, commit, abort, err := s.ReserveFile(context.Background(), bucketID, "link.txt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = abort() })
	outside := filepath.Join(t.TempDir(), "outside.txt")
	require.NoError(t, os.WriteFile(outside, []byte("outside"), 0600))
	if err := os.Symlink(outside, filepath.Join(path, "link.txt")); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	require.ErrorIs(t, commit(), ErrInvalidPath)
	content, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, []byte("outside"), content)
}

func Test_ReserveBucketUsesRestrictedMetadataPermissions(t *testing.T) {
	s := newTestStorage(t)
	bucketID := newBucketID(t, 1)

	path, _, abort, err := s.ReserveBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, abort()) }()

	info, err := os.Stat(filepath.Join(path, s.getMetaFile(bucketID)))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func Test_ReserveBucketRejectsSymlinkMetadata(t *testing.T) {
	s := newTestStorage(t)
	bucketID := newBucketID(t, 1)

	path, commit, abort, err := s.ReserveBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = abort() })
	metaPath := filepath.Join(path, s.getMetaFile(bucketID))
	require.NoError(t, os.Remove(metaPath))
	outside := filepath.Join(t.TempDir(), "outside.json")
	require.NoError(t, os.WriteFile(outside, []byte(`{"outside":true}`), 0600))
	if err := os.Symlink(outside, metaPath); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	require.ErrorIs(t, commit(), ErrInvalidPath)
	content, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"outside":true}`), content)
}
