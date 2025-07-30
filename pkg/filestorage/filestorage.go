package filestorage

import (
	"context"
	"filestorage/internal/api/handler"
	"filestorage/internal/artifact"
	"filestorage/internal/storage"
	"filestorage/pkg/config"
	"github.com/go-chi/chi/v5"
	"log/slog"
	"time"
)

type FileStorage interface {
	GetArtifact(artifactID artifact.ID) (path string, unlock func(), err error)
	CreateArtifact(artifactID artifact.ID, trashTime time.Time) (path string, commit, abort func() error, err error)
	DownloadArtifact(ctx context.Context, endpoint string, artifactID artifact.ID, trashTime time.Time) error
	Shutdown()
}

func New(log *slog.Logger, root string, mux *chi.Mux, cfg config.Config) (FileStorage, error) {
	s, err := storage.NewStorage(log, root, cfg)
	if err != nil {
		return nil, err
	}

	handler.NewHandler(s).Register(mux)

	return s, nil
}
