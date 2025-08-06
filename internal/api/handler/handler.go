package handler

import (
	"encoding/json"
	"errors"
	"github.com/DIvanCode/filestorage/internal/api"
	"github.com/DIvanCode/filestorage/internal/api/tarstream"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	errs "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
	"net/http"
)

type Handler struct {
	storage FileStorage
}

type FileStorage interface {
	GetArtifact(artifactID id.ID) (path string, unlock func(), err error)
}

func NewHandler(storage FileStorage) *Handler {
	return &Handler{
		storage: storage,
	}
}

func (h *Handler) Register(mux *chi.Mux) {
	mux.HandleFunc("/artifact", h.handleDownloadArtifact)
	mux.HandleFunc("/artifact-file", h.handleDownloadArtifactFile)
}

func (h *Handler) handleDownloadArtifact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	var artifactID id.ID
	if err := artifactID.FromString(query.Get("id")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.storage.GetArtifact(artifactID)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
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

func (h *Handler) handleDownloadArtifactFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	var artifactID id.ID
	if err := artifactID.FromString(query.Get("id")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req api.DownloadFileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.storage.GetArtifact(artifactID)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer unlock()

	w.Header().Set("Content-Type", "application/x-tar")
	if err := tarstream.SendFile(path, req.File, w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
