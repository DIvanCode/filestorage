package filestorage

import (
	"context"
	"github.com/DIvanCode/filestorage/internal/api/handler"
	"github.com/DIvanCode/filestorage/internal/storage"
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"github.com/DIvanCode/filestorage/pkg/config"
	"github.com/go-chi/chi/v5"
	"log/slog"
	"time"
)

type FileStorage interface {
	GetArtifact(artifactID id.ID) (path string, unlock func(), err error)
	CreateArtifact(artifactID id.ID, trashTime time.Time) (path string, commit, abort func() error, err error)
	DownloadArtifact(ctx context.Context, endpoint string, artifactID id.ID, trashTime time.Time) error
	DownloadFile(ctx context.Context, endpoint string, artifactID id.ID, file string, trashTime time.Time) error
	Shutdown()
}

func New(log *slog.Logger, cfg config.Config, mux *chi.Mux) (FileStorage, error) {
	s, err := storage.NewStorage(log, cfg)
	if err != nil {
		return nil, err
	}
	handler.NewHandler(s).Register(mux)
	return s, nil
}
