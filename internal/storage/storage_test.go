package storage

import (
	"context"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
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
	idStr := strconv.Itoa(idNum)
	for len(idStr) < 20 {
		idStr = "0" + idStr
	}

	var id bucket.ID
	require.NoError(t, id.FromString(idStr))
	return id
}

func createBucket(t *testing.T, s *testStorage, id bucket.ID, trashTime time.Time) {
	_, commit, _, err := s.CreateBucket(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())
}

func Test_GetBucket_NotFound(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	path, unlock, err := s.GetBucket(bucketID)
	require.ErrorIs(t, err, ErrBucketNotFound)
	assert.NotNil(t, path)
	assert.Nil(t, unlock)
}

func Test_GetBucket_ParallelRead(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createBucket(t, s, bucketID, trashTime)

	path, unlock1, err := s.GetBucket(bucketID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock1)
	defer unlock1()

	path, unlock2, err := s.GetBucket(bucketID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock2)
	defer unlock2()
}

func Test_GetBucket_WriteLock(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)

	path, unlock, err := s.GetBucket(bucketID)
	require.ErrorIs(t, err, ErrWriteLocked)
	assert.NotNil(t, path)
	assert.Nil(t, unlock)

	_, _, _, err = s.CreateBucket(bucketID, trashTime)
	require.ErrorIs(t, err, ErrWriteLocked)

	require.NoError(t, commit())

	path, unlock, err = s.GetBucket(bucketID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)
	defer unlock()
}

func Test_CreateBucket_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createBucket(t, s, bucketID, trashTime)

	_, _, _, err := s.CreateBucket(bucketID, trashTime)
	require.ErrorIs(t, err, ErrBucketAlreadyExists)
}

func Test_CreateBucket_CreateFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetBucket(bucketID)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_CreateFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	path, commit, _, err := s.CreateFile(bucketID, "a.txt")
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	_, err = f.WriteString("aaa")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetBucket(bucketID)
	require.NoError(t, err)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	bytes, err := os.ReadFile(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "aaa", string(bytes))
}

func Test_CreateFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	_, _, _, err = s.CreateFile(bucketID, "a.txt")
	require.ErrorIs(t, err, ErrFileAlreadyExists)
}

func Test_CreateFile_BucketNotFound(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	_, _, _, err := s.CreateFile(bucketID, "a.txt")
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func Test_RemoveBucket_NotExisting(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	err := s.RemoveBucket(bucketID)
	require.NoError(t, err)
}

func Test_RemoveBucket_Removed(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createBucket(t, s, bucketID, trashTime)

	path, unlock, err := s.GetBucket(bucketID)
	require.NoError(t, err)
	unlock()

	err = s.RemoveBucket(bucketID)
	require.NoError(t, err)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_CreateBucket_Abort(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, _, abort, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)
	require.NoError(t, abort())

	_, _, err = s.GetBucket(bucketID)
	require.ErrorIs(t, err, ErrBucketNotFound)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_GetBucketMeta(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	meta, err := s.GetBucketMeta(bucketID)
	require.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, bucketID, meta.BucketID)
	assert.True(t, trashTime.Equal(meta.TrashTime))

	_, _, err = s.GetBucket(bucketID)
	require.NoError(t, err)
}

func Test_DownloadBucket_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DownloadBucket(context.Background(), "some-endpoint", bucketID, trashTime)
	require.NoError(t, err)
}

func Test_DownloadFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DownloadFile(context.Background(), "some-endpoint", bucketID, "a.txt")
	require.NoError(t, err)
}

func Test_DeleteFile_Deleted(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	path, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DeleteFile(bucketID, "a.txt")
	require.NoError(t, err)

	path, unlock, err := s.GetBucket(bucketID)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	unlock()
}

func Test_DeleteFile_NotExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	_, commit, _, err := s.CreateBucket(bucketID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DeleteFile(bucketID, "a.txt")
	require.ErrorIs(t, err, ErrFileNotFound)
}
