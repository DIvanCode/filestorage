package storage

import (
	"context"
	"github.com/DIvanCode/filestorage/pkg/artifact"
	. "github.com/DIvanCode/filestorage/pkg/config"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/stretchr/testify/require"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	cfg := Config{
		Trasher: TrasherConfig{
			Workers:                  1,
			CollectorIterationsDelay: 60,
			WorkerIterationsDelay:    60,
		},
	}

	storage, err := NewStorage(log, tmpDir, cfg)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
	}
	require.NoError(t, err)

	s := &testStorage{Storage: storage, tmpDir: tmpDir}
	t.Cleanup(s.cleanup)
	return s
}

func newArtifactID(t *testing.T, id string) artifact.ID {
	var artifactID artifact.ID
	require.NoError(t, artifactID.FromString(id))
	return artifactID
}

func createArtifact(t *testing.T, s *testStorage, id artifact.ID, trashTime time.Time) {
	_, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())
}

func Test_GetArtifact_NotFound(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")

	path, unlock, err := s.GetArtifact(id)
	require.ErrorIs(t, err, ErrNotFound)
	require.NotNil(t, path)
	require.Nil(t, unlock)
}

func Test_GetArtifact_ParallelRead(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, id, trashTime)

	path, unlock1, err := s.GetArtifact(id)
	require.NoError(t, err)
	require.NotNil(t, path)
	require.NotNil(t, unlock1)
	defer unlock1()

	path, unlock2, err := s.GetArtifact(id)
	require.NoError(t, err)
	require.NotNil(t, path)
	require.NotNil(t, unlock2)
	defer unlock2()
}

func Test_GetArtifact_WriteLock(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)

	path, unlock, err := s.GetArtifact(id)
	require.ErrorIs(t, err, ErrWriteLocked)
	require.NotNil(t, path)
	require.Nil(t, unlock)

	_, _, _, err = s.CreateArtifact(id, trashTime)
	require.ErrorIs(t, err, ErrWriteLocked)

	require.NoError(t, commit())

	path, unlock, err = s.GetArtifact(id)
	require.NoError(t, err)
	require.NotNil(t, path)
	require.NotNil(t, unlock)
	defer unlock()
}

func Test_CreateArtifact_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, id, trashTime)

	_, _, _, err := s.CreateArtifact(id, trashTime)
	require.ErrorIs(t, err, ErrAlreadyExists)
}

func Test_CreateArtifact_CreateFile(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetArtifact(id)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_RemoveArtifact_NotExisting(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	err := s.RemoveArtifact(id)
	require.NoError(t, err)
}

func Test_RemoveArtifact_Removed(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, id, trashTime)

	path, unlock, err := s.GetArtifact(id)
	require.NoError(t, err)
	unlock()

	err = s.RemoveArtifact(id)
	require.NoError(t, err)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_CreateArtifact_Abort(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	path, _, abort, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, abort())

	_, _, err = s.GetArtifact(id)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_GetArtifactMeta(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	meta, err := s.GetArtifactMeta(id)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, id, meta.ID)
	require.True(t, trashTime.Equal(meta.TrashTime))

	_, _, err = s.GetArtifact(id)
	require.NoError(t, err)
}

func Test_DownloadArtifact_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	id := newArtifactID(t, "00000000000000000001")
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DownloadArtifact(context.Background(), "some-endpoint", id, trashTime)
	require.NoError(t, err)
}
