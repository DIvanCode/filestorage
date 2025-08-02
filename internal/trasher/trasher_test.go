package trasher_test

import (
	"github.com/DIvanCode/filestorage/internal/trasher"
	artifact "github.com/DIvanCode/filestorage/pkg/artifact"
	"github.com/DIvanCode/filestorage/pkg/config"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newArtifactID(t *testing.T, id string) artifact.ID {
	var artifactID artifact.ID
	require.NoError(t, artifactID.FromString(id))
	return artifactID
}

type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) GetArtifactMeta(id artifact.ID) (models.Meta, error) {
	args := m.Called(id)
	return args.Get(0).(models.Meta), args.Error(1)
}

func (m *MockStorage) RemoveArtifact(id artifact.ID) error {
	args := m.Called(id)
	return args.Error(0)
}

func TestTrasherCollectAndRemove(t *testing.T) {
	tmpRoot := t.TempDir()
	shardDir := filepath.Join(tmpRoot, "00")
	require.NoError(t, os.MkdirAll(shardDir, 0777))

	id := newArtifactID(t, "00000000000000000001")
	require.NoError(t, os.Mkdir(filepath.Join(shardDir, id.String()), 0777))

	meta := models.Meta{ID: id, TrashTime: time.Now().Add(-time.Hour)}

	mockStorage := new(MockStorage)
	mockStorage.On("GetArtifactMeta", id).Return(meta, nil)
	mockStorage.On("RemoveArtifact", id).Return(nil)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	thr, err := trasher.NewTrasher(
		logger,
		config.TrasherConfig{
			CollectorIterationsDelay: 1,
			WorkerIterationsDelay:    1,
			Workers:                  1,
		},
	)
	require.NoError(t, err)

	thr.Start(mockStorage, tmpRoot)
	time.Sleep(3 * time.Second)
	thr.Stop()

	mockStorage.AssertCalled(t, "GetArtifactMeta", id)
	mockStorage.AssertCalled(t, "RemoveArtifact", id)
}
