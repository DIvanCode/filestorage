package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIvanCode/filestorage/internal/lib/tarstream"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

type stubStorage struct {
	getFilePath string
	getFileErr  error
}

func (s stubStorage) GetBucket(context.Context, bucket.ID, *time.Duration) (string, func(), error) {
	return "", func() {}, nil
}

func (s stubStorage) GetFile(context.Context, bucket.ID, string, *time.Duration) (string, func(), error) {
	return s.getFilePath, func() {}, s.getFileErr
}

func TestHandleDownloadFileClassifiesInvalidPathAsBadRequest(t *testing.T) {
	mux := chi.NewRouter()
	NewHandler(stubStorage{getFileErr: fserrors.ErrInvalidPath}).Register(mux)
	req := httptest.NewRequest(
		http.MethodGet,
		"/file?bucket-id=0000000000000000000000000000000000000001",
		bytes.NewBufferString(`{"file":"../secret.txt"}`),
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, req)

	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestHandleDownloadFileSupportsRelativeStorageRoot(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	require.NoError(t, os.Mkdir("storage", 0755))
	require.NoError(t, os.WriteFile(filepath.Join("storage", "checker.cpp"), []byte("checker"), 0644))

	mux := chi.NewRouter()
	NewHandler(stubStorage{getFilePath: "storage"}).Register(mux)
	req := httptest.NewRequest(
		http.MethodGet,
		"/file?bucket-id=0000000000000000000000000000000000000001",
		bytes.NewBufferString(`{"file":"checker.cpp"}`),
	)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, req)

	require.Equal(t, http.StatusOK, response.Code)
	destination := t.TempDir()
	require.NoError(t, tarstream.Receive(destination, response.Body))
	require.FileExists(t, filepath.Join(destination, "checker.cpp"))
}
