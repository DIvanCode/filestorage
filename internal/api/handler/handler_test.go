package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DIvanCode/filestorage/pkg/bucket"
	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

type stubStorage struct {
	getFileErr error
}

func (s stubStorage) GetBucket(context.Context, bucket.ID, *time.Duration) (string, func(), error) {
	return "", func() {}, nil
}

func (s stubStorage) GetFile(context.Context, bucket.ID, string, *time.Duration) (string, func(), error) {
	return "", nil, s.getFileErr
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
