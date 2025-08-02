package trasher_test

import (
	"github.com/DIvanCode/filestorage/internal/artifact"
	trash "github.com/DIvanCode/filestorage/internal/trasher"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"github.com/DIvanCode/filestorage/pkg/config"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newArtifactID(t *testing.T, idNum int) id.ID {
	idStr := strconv.Itoa(idNum)
	for len(idStr) < 20 {
		idStr = "0" + idStr
	}

	var artifactID id.ID
	require.NoError(t, artifactID.FromString(idStr))
	return artifactID
}

type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) GetArtifactMeta(artifactID id.ID) (artifact.Meta, error) {
	args := m.Called(artifactID)
	return args.Get(0).(artifact.Meta), args.Error(1)
}

func (m *MockStorage) RemoveArtifact(artifactID id.ID) error {
	args := m.Called(artifactID)
	return args.Error(0)
}

func TestTrasherCollectAndRemove(t *testing.T) {
	tmpRoot := t.TempDir()
	shardDir := filepath.Join(tmpRoot, "00")
	require.NoError(t, os.MkdirAll(shardDir, 0777))

	artifactID := newArtifactID(t, 1)
	require.NoError(t, os.Mkdir(filepath.Join(shardDir, artifactID.String()), 0777))

	meta := artifact.Meta{ID: artifactID, TrashTime: time.Now().Add(-time.Hour)}

	mockStorage := new(MockStorage)
	mockStorage.On("GetArtifactMeta", artifactID).Return(meta, nil)
	mockStorage.On("RemoveArtifact", artifactID).Return(nil)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	trasher, err := trash.NewTrasher(
		logger,
		config.TrasherConfig{
			CollectorIterationsDelay: 1,
			WorkerIterationsDelay:    1,
			Workers:                  1,
		},
	)
	require.NoError(t, err)

	trasher.Start(mockStorage, tmpRoot)
	time.Sleep(3 * time.Second)
	trasher.Stop()

	mockStorage.AssertCalled(t, "GetArtifactMeta", artifactID)
	mockStorage.AssertCalled(t, "RemoveArtifact", artifactID)
}
