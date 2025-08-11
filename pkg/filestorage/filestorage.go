package filestorage

import (
	"context"
	"github.com/DIvanCode/filestorage/internal/api/handler"
	"github.com/DIvanCode/filestorage/internal/storage"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	"github.com/go-chi/chi/v5"
	"log/slog"
	"time"
)

type FileStorage interface {
	GetBucket(id bucket.ID) (path string, unlock func(), err error)
	GetFile(bucketID bucket.ID, file string) (path string, unlock func(), err error)
	CreateBucket(id bucket.ID, trashTime time.Time) (path string, commit, abort func() error, err error)
	CreateFile(bucketID bucket.ID, file string) (path string, commit, abort func() error, err error)
	DownloadBucket(ctx context.Context, endpoint string, id bucket.ID, trashTime time.Time) error
	DownloadFile(ctx context.Context, endpoint string, bucketID bucket.ID, file string) error
	DeleteFile(bucketID bucket.ID, file string) error
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
