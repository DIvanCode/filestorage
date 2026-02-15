package filestorage

import (
	"context"
	"log/slog"
	"time"

	"github.com/DIvanCode/filestorage/internal/api/handler"
	"github.com/DIvanCode/filestorage/internal/storage"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
	"github.com/go-chi/chi/v5"
)

type FileStorage interface {
	GetBucket(ctx context.Context, id bucket.ID, extendTTL *time.Duration) (path string, unlock func(), err error)
	GetFile(ctx context.Context, bucketID bucket.ID, file string, extendTTL *time.Duration) (path string, unlock func(), err error)
	ReserveBucket(ctx context.Context, id bucket.ID, ttl *time.Duration) (path string, commit, abort func() error, err error)
	ReserveFile(ctx context.Context, bucketID bucket.ID, file string) (path string, commit, abort func() error, err error)
	DownloadBucket(ctx context.Context, endpoint string, id bucket.ID, ttl *time.Duration) error
	DownloadFile(ctx context.Context, endpoint string, bucketID bucket.ID, file string) error
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
