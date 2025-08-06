package storage

import (
	"context"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"github.com/DIvanCode/filestorage/pkg/config"
	errs "github.com/DIvanCode/filestorage/pkg/errors"
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

func newArtifactID(t *testing.T, idNum int) id.ID {
	idStr := strconv.Itoa(idNum)
	for len(idStr) < 20 {
		idStr = "0" + idStr
	}

	var artifactID id.ID
	require.NoError(t, artifactID.FromString(idStr))
	return artifactID
}

func createArtifact(t *testing.T, s *testStorage, id id.ID, trashTime time.Time) {
	_, commit, _, err := s.CreateArtifact(id, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())
}

func Test_GetArtifact_NotFound(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)

	path, unlock, err := s.GetArtifact(artifactID)
	require.ErrorIs(t, err, errs.ErrNotFound)
	assert.NotNil(t, path)
	assert.Nil(t, unlock)
}

func Test_GetArtifact_ParallelRead(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, artifactID, trashTime)

	path, unlock1, err := s.GetArtifact(artifactID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock1)
	defer unlock1()

	path, unlock2, err := s.GetArtifact(artifactID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock2)
	defer unlock2()
}

func Test_GetArtifact_WriteLock(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)

	path, unlock, err := s.GetArtifact(artifactID)
	require.ErrorIs(t, err, errs.ErrWriteLocked)
	assert.NotNil(t, path)
	assert.Nil(t, unlock)

	_, _, _, err = s.CreateArtifact(artifactID, trashTime)
	require.ErrorIs(t, err, errs.ErrWriteLocked)

	require.NoError(t, commit())

	path, unlock, err = s.GetArtifact(artifactID)
	require.NoError(t, err)
	assert.NotNil(t, path)
	assert.NotNil(t, unlock)
	defer unlock()
}

func Test_CreateArtifact_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, artifactID, trashTime)

	_, _, _, err := s.CreateArtifact(artifactID, trashTime)
	require.ErrorIs(t, err, errs.ErrAlreadyExists)
}

func Test_CreateArtifact_CreateFile(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetArtifact(artifactID)
	defer unlock()

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
}

func Test_CreateFile(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	path, commit, _, err := s.CreateFile(artifactID, "a.txt")
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)

	_, err = f.WriteString("aaa")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	path, unlock, err := s.GetArtifact(artifactID)
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

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	_, _, _, err = s.CreateFile(artifactID, "a.txt")
	require.ErrorIs(t, err, errs.ErrAlreadyExists)
}

func Test_CreateFile_ArtifactNotExists(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)

	_, _, _, err := s.CreateFile(artifactID, "a.txt")
	require.ErrorIs(t, err, errs.ErrNotFound)
}

func Test_RemoveArtifact_NotExisting(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	err := s.RemoveArtifact(artifactID)
	require.NoError(t, err)
}

func Test_RemoveArtifact_Removed(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	createArtifact(t, s, artifactID, trashTime)

	path, unlock, err := s.GetArtifact(artifactID)
	require.NoError(t, err)
	unlock()

	err = s.RemoveArtifact(artifactID)
	require.NoError(t, err)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_CreateArtifact_Abort(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, _, abort, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)
	require.NoError(t, abort())

	_, _, err = s.GetArtifact(artifactID)
	require.ErrorIs(t, err, errs.ErrNotFound)

	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func Test_GetArtifactMeta(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	meta, err := s.GetArtifactMeta(artifactID)
	require.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, artifactID, meta.ID)
	assert.True(t, trashTime.Equal(meta.TrashTime))

	_, _, err = s.GetArtifact(artifactID)
	require.NoError(t, err)
}

func Test_DownloadArtifact_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	_, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DownloadArtifact(context.Background(), "some-endpoint", artifactID, trashTime)
	require.NoError(t, err)
}

func Test_DownloadFile_AlreadyExists(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)

	path, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DownloadFile(context.Background(), "some-endpoint", artifactID, "a.txt")
	require.NoError(t, err)
}

func Test_DeleteFile_Deleted(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	path, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)

	f, err := os.Create(filepath.Join(path, "a.txt"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, commit())

	err = s.DeleteFile(artifactID, "a.txt")
	require.NoError(t, err)

	path, unlock, err := s.GetArtifact(artifactID)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(path, "a.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	unlock()
}

func Test_DeleteFile_NotExists(t *testing.T) {
	s := newTestStorage(t)

	artifactID := newArtifactID(t, 1)
	trashTime := time.Now().Add(time.Minute)
	_, commit, _, err := s.CreateArtifact(artifactID, trashTime)
	require.NoError(t, err)
	require.NoError(t, commit())

	err = s.DeleteFile(artifactID, "a.txt")
	require.ErrorIs(t, err, errs.ErrNotFound)
}
