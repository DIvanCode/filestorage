package trasher_test

import (
	. "github.com/DIvanCode/filestorage/internal/bucket/meta"
	trash "github.com/DIvanCode/filestorage/internal/trasher"
	"github.com/DIvanCode/filestorage/pkg/bucket"
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

func newBucketID(t *testing.T, idNum int) bucket.ID {
	idStr := strconv.Itoa(idNum)
	for len(idStr) < 20 {
		idStr = "0" + idStr
	}

	var id bucket.ID
	require.NoError(t, id.FromString(idStr))
	return id
}

type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) GetBucketMeta(id bucket.ID) (BucketMeta, error) {
	args := m.Called(id)
	return args.Get(0).(BucketMeta), args.Error(1)
}

func (m *MockStorage) RemoveBucket(id bucket.ID) error {
	args := m.Called(id)
	return args.Error(0)
}

func TestTrasherCollectAndRemove(t *testing.T) {
	tmpRoot := t.TempDir()
	shardDir := filepath.Join(tmpRoot, "00")
	require.NoError(t, os.MkdirAll(shardDir, 0777))

	bucketID := newBucketID(t, 1)
	require.NoError(t, os.Mkdir(filepath.Join(shardDir, bucketID.String()), 0777))

	meta := BucketMeta{BucketID: bucketID, TrashTime: time.Now().Add(-time.Hour)}

	mockStorage := new(MockStorage)
	mockStorage.On("GetBucketMeta", bucketID).Return(meta, nil)
	mockStorage.On("RemoveBucket", bucketID).Return(nil)

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

	mockStorage.AssertCalled(t, "GetBucketMeta", bucketID)
	mockStorage.AssertCalled(t, "RemoveBucket", bucketID)
}
