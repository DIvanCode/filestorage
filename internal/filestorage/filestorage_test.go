package filestorage

import (
	"context"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"github.com/DIvanCode/filestorage/pkg/config"
	errs "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/DIvanCode/filestorage/pkg/filestorage"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func newArtifactID(t *testing.T, idStr string) id.ID {
	var artifactID id.ID
	require.NoError(t, artifactID.FromString(idStr))
	return artifactID
}

func Test_TransferArtifact(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	dst := newTestStorage(t, "dst", "localhost:5253")

	ID := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := src.CreateArtifact(ID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadArtifact(ctx, "http://localhost:5252", ID, trashTime)
	require.NoError(t, err)

	path, unlock, err := dst.GetArtifact(ID)
	defer unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_DownloadFailed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	dst := newTestStorage(t, "dst", "localhost:5253")

	ID := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := src.CreateArtifact(ID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	shutdown(src.srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadArtifact(ctx, "http://localhost:5252", ID, trashTime)
	require.Error(t, err)
}

func Test_DoNotRepeatDownload(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	dst := newTestStorage(t, "dst", "localhost:5253")

	ID := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := src.CreateArtifact(ID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadArtifact(ctx, "http://localhost:5252", ID, trashTime)
	require.NoError(t, err)

	path, unlock, err := dst.GetArtifact(ID)
	defer unlock()
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	shutdown(src.srv)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadArtifact(ctx, "http://localhost:5252", ID, trashTime)
	require.NoError(t, err)
}

func Test_ArtifactTrashedAfterTrashTime(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	src := newTestStorage(t, "src", "localhost:5252")
	dst := newTestStorage(t, "dst", "localhost:5253")

	ID := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(-time.Second)

	path, commit, _, err := src.CreateArtifact(ID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	time.Sleep(3 * time.Second)

	_, _, err = src.GetArtifact(ID)
	require.ErrorIs(t, err, errs.ErrNotFound)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = dst.DownloadArtifact(ctx, "http://localhost:5252", ID, trashTime)
	require.Error(t, err)
}
