package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/DIvanCode/filestorage/internal/api"
	"github.com/DIvanCode/filestorage/internal/lib/tarstream"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	. "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
)

type (
	Handler struct {
		storage fileStorage
	}

	fileStorage interface {
		GetBucket(ctx context.Context, id bucket.ID, addTTL *time.Duration) (path string, unlock func(), err error)
	}
)

func NewHandler(storage fileStorage) *Handler {
	return &Handler{
		storage: storage,
	}
}

func (h *Handler) Register(mux *chi.Mux) {
	mux.HandleFunc("/bucket", h.handleDownloadBucket)
	mux.HandleFunc("/file", h.handleDownloadFile)
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
	if err := tarstream.SendFile(req.File, path, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
