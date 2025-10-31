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
	_, commit, _, err := s.ReserveBucket(context.Background(), id, ttl)
	require.NoError(t, err)
	require.NoError(t, commit())
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

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	path, unlock, err := s.GetBucket(ctx, bucketID, nil)
	require.ErrorIs(t, err, ctx.Err())
	assert.Nil(t, unlock)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err = s.ReserveBucket(ctx, bucketID, time.Minute)
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
	reserveBucket(t, s, bucketID, time.Minute)

	_, _, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.ErrorIs(t, err, ErrBucketAlreadyExists)
}

func Test_ReserveBucket_ReserveFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetBucket(context.Background(), bucketID, nil)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	path, unlock, err = s.GetFile(context.Background(), bucketID, "a.txt")
	defer unlock()
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_ReserveFile(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
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

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
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

	path, _, abort, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
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

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.NoError(t, err)
	require.NoError(t, commit())

	meta, err := s.GetBucketMeta(context.Background(), bucketID)
	require.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, bucketID, meta.BucketID)
	assert.True(t, expectedTrashTime.Before(meta.TrashTime))

	_, _, err = s.GetBucket(context.Background(), bucketID, nil)
	require.NoError(t, err)
}

func Test_DownloadBucket_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	_, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DownloadBucket(context.Background(), "some-endpoint", bucketID, time.Minute)
	require.NoError(t, err)
}

func Test_DownloadFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	bucketID := newBucketID(t, 1)

	path, commit, _, err := s.ReserveBucket(context.Background(), bucketID, time.Minute)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DownloadFile(context.Background(), "some-endpoint", bucketID, "a.txt")
	require.NoError(t, err)
}
