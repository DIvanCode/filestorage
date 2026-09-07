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
	NewHandler(stubStorage{getFileErr: fserrors.ErrInvalidPath}, "test-key").Register(mux)
	req := httptest.NewRequest(
		http.MethodGet,
		"/file?bucket-id=0000000000000000000000000000000000000001",
		bytes.NewBufferString(`{"file":"../secret.txt"}`),
	)
	req.Header.Set("internal-auth", "test-key")
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
	NewHandler(stubStorage{getFilePath: "storage"}, "test-key").Register(mux)
	req := httptest.NewRequest(
		http.MethodGet,
		"/file?bucket-id=0000000000000000000000000000000000000001",
		bytes.NewBufferString(`{"file":"checker.cpp"}`),
	)
	req.Header.Set("internal-auth", "test-key")
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, req)

	require.Equal(t, http.StatusOK, response.Code)
	destination := t.TempDir()
	require.NoError(t, tarstream.Receive(destination, response.Body))
	require.FileExists(t, filepath.Join(destination, "checker.cpp"))
}

func TestHandlerRejectsUnauthorizedRequests(t *testing.T) {
	for _, path := range []string{"/bucket", "/file"} {
		for _, tc := range []struct {
			name          string
			configuredKey string
			headers       []string
		}{
			{name: "missing", configuredKey: "test-key"},
			{name: "empty", configuredKey: "test-key", headers: []string{""}},
			{name: "wrong", configuredKey: "test-key", headers: []string{"wrong-key"}},
			{name: "duplicate", configuredKey: "test-key", headers: []string{"test-key", "wrong-key"}},
			{name: "unconfigured"},
			{name: "unconfigured with header", headers: []string{"test-key"}},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				mux := chi.NewRouter()
				// A nil storage also proves authentication happens before storage access.
				NewHandler(nil, tc.configuredKey).Register(mux)
				req := httptest.NewRequest(http.MethodGet, path, nil)
				for _, key := range tc.headers {
					req.Header.Add("internal-auth", key)
				}
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, req)
				require.Equal(t, http.StatusUnauthorized, response.Code)
				require.Equal(t, "Unauthorized\n", response.Body.String())
			})
		}
	}
}

func TestAuthenticationOnlyAppliesToFilestorageRoutes(t *testing.T) {
	mux := chi.NewRouter()
	NewHandler(nil, "test-key").Register(mux)
	mux.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusNoContent, response.Code)
}
