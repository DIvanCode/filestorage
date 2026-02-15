package filestorage

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/DIvanCode/filestorage/pkg/filestorage"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type testStorage struct {
	filestorage.FileStorage
	tmpDir string
	srv    *http.Server
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func (s *testStorage) cleanup() {
	s.Shutdown()
	shutdown(s.srv)
	_ = os.RemoveAll(s.tmpDir)
}

func newTestStorage(t *testing.T, rootDir, endpoint string) *testStorage {
	tmpDir, err := os.MkdirTemp("", rootDir)
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{
		RootDir: tmpDir,
		Trasher: config.TrasherConfig{
			Workers:                  1,
			CollectorIterationsDelay: 1,
			WorkerIterationsDelay:    1,
		},
	}
	mux := chi.NewRouter()

	srv := &http.Server{
		Addr:    endpoint,
		Handler: mux,
	}

	storage, err := filestorage.New(log, cfg, mux)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
	}
	require.NoError(t, err)

	go func() {
		_ = srv.ListenAndServe()
	}()

	s := &testStorage{FileStorage: storage, tmpDir: tmpDir, srv: srv}
	t.Cleanup(s.cleanup)
	return s
}

func newBucketID(t *testing.T, idStr string) bucket.ID {
	var id bucket.ID
	require.NoError(t, id.FromString(idStr))
	return id
}

func Test_TransferBucket(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := time.Minute

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadBucket(ctx, "http://0.0.0.0:5252", ID, &ttl)
	require.NoError(t, err)

	path, unlock, err := dst.GetBucket(context.Background(), ID, nil)
	defer unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_BucketExists_TransferFile(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := time.Minute

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = os.Create(filepath.Join(path, "b.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, commit, _, err = dst.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err = os.Create(filepath.Join(path, "c.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadFile(ctx, "http://localhost:5252", ID, "a.txt")
	require.NoError(t, err)

	path, unlock, err := dst.GetBucket(context.Background(), ID, nil)
	defer unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(path, "b.txt"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(path, "c.txt"))
	assert.NoError(t, err)
}

func Test_BucketNotExists_TransferFile(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := time.Minute

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Join(path, "a"), 0777))
	f, err := os.Create(filepath.Join(path, "a/a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = os.Create(filepath.Join(path, "b.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, commit, _, err = dst.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)
	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadFile(ctx, "http://localhost:5252", ID, "a/a.txt")
	require.NoError(t, err)

	path, unlock, err := dst.GetBucket(context.Background(), ID, nil)
	defer unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a/a.txt"))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(path, "b.txt"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func Test_DownloadFailed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := time.Minute

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	shutdown(src.srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadBucket(ctx, "http://localhost:5252", ID, &ttl)
	require.Error(t, err)
}

func Test_DoNotRepeatDownload(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := time.Minute

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadBucket(ctx, "http://localhost:5252", ID, &ttl)
	require.NoError(t, err)

	path, unlock, err := dst.GetBucket(context.Background(), ID, nil)
	unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	shutdown(src.srv)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadBucket(ctx, "http://localhost:5252", ID, &ttl)
	require.NoError(t, err)
}

func Test_BucketTrashedAfterTrashTime(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	defer src.Shutdown()
	dst := newTestStorage(t, "dst", "localhost:5253")
	defer dst.Shutdown()

	ID := newBucketID(t, "0000000000000000000000000000000000000001")
	ttl := -time.Second

	path, commit, _, err := src.ReserveBucket(context.Background(), ID, &ttl)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	time.Sleep(2 * time.Second)

	_, _, err = src.GetBucket(context.Background(), ID, nil)
	if err == nil {
		time.Sleep(time.Second)
		_, _, err = src.GetBucket(context.Background(), ID, nil)
	}
	require.ErrorIs(t, err, ErrBucketNotFound)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ttl = time.Minute
	err = dst.DownloadBucket(ctx, "http://localhost:5252", ID, &ttl)
	require.Error(t, err)
}
