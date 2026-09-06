package handler

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/DIvanCode/filestorage/internal/api"
	"github.com/DIvanCode/filestorage/internal/lib/tarstream"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
)

type (
	Handler struct {
		storage         fileStorage
		internalAuthKey string
	}

	fileStorage interface {
		GetBucket(ctx context.Context, id bucket.ID, addTTL *time.Duration) (path string, unlock func(), err error)
		GetFile(ctx context.Context, bucketID bucket.ID, file string, addTTL *time.Duration) (path string, unlock func(), err error)
	}
)

func NewHandler(storage fileStorage, internalAuthKey string) *Handler {
	return &Handler{
		storage:         storage,
		internalAuthKey: internalAuthKey,
	}
}

func (h *Handler) Register(mux *chi.Mux) {
	mux.With(h.authenticate).HandleFunc("/bucket", h.handleDownloadBucket)
	mux.With(h.authenticate).HandleFunc("/file", h.handleDownloadFile)
}

func (h *Handler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys := r.Header.Values("internal-auth")
		if h.internalAuthKey == "" || len(keys) != 1 ||
			subtle.ConstantTimeCompare([]byte(keys[0]), []byte(h.internalAuthKey)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) handleDownloadBucket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	var id bucket.ID
	if err := id.FromString(query.Get("id")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.storage.GetBucket(r.Context(), id, nil)
	if err != nil {
		if errors.Is(err, ErrBucketNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer unlock()

	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarstream.Send(path, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	var id bucket.ID
	if err := id.FromString(query.Get("bucket-id")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req api.DownloadFileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.storage.GetFile(r.Context(), id, req.File, nil)
	if err != nil {
		if errors.Is(err, ErrInvalidPath) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if errors.Is(err, ErrBucketNotFound) || errors.Is(err, ErrFileNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer unlock()

	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarstream.SendFile(req.File, path, w); err != nil {
		if errors.Is(err, ErrInvalidPath) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if errors.Is(err, os.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}
