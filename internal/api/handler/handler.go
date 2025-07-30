package handler

import (
	"errors"
	"filestorage/internal/api/tarstream"
	"filestorage/internal/artifact"
	. "filestorage/internal/storage"
	. "filestorage/pkg/errors"
	"github.com/go-chi/chi/v5"
	"net/http"
)

type Handler struct {
	storage *Storage
}

func NewHandler(storage *Storage) *Handler {
	return &Handler{
		storage: storage,
	}
}

func (h *Handler) Register(mux *chi.Mux) {
	mux.HandleFunc("/artifact", h.handleDownloadArtifact)
}

func (h *Handler) handleDownloadArtifact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()

	var artifactID artifact.ID
	if err := artifactID.FromString(query.Get("id")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.storage.GetArtifact(artifactID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
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
