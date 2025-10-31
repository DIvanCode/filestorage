package trasher

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	. "github.com/DIvanCode/filestorage/internal/bucket/meta"
	"github.com/DIvanCode/filestorage/internal/lib/queue"
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/DIvanCode/filestorage/pkg/config"
)

type Trasher struct {
	cfg config.TrasherConfig

	collectedBucketsQueue *queue.Queue[bucket.ID]

	cancelFunc context.CancelFunc

	log *slog.Logger
}

type FileStorage interface {
	GetBucketMeta(ctx context.Context, id bucket.ID) (BucketMeta, error)
	RemoveBucket(ctx context.Context, id bucket.ID) error
}

func NewTrasher(log *slog.Logger, cfg config.TrasherConfig) (*Trasher, error) {
	trasher := &Trasher{
		cfg: cfg,

		collectedBucketsQueue: queue.NewQueue[bucket.ID](),

		log: log,
	}

	return trasher, nil
}

func (t *Trasher) Start(storage FileStorage, rootDir string) {
	ctx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel

	t.startCollector(ctx, storage, rootDir)
	for range t.cfg.Workers {
		t.startWorker(ctx, storage)
	}
}

func (t *Trasher) Stop() {
	t.cancelFunc()
}

func (t *Trasher) startCollector(ctx context.Context, storage FileStorage, rootDir string) {
	go func() {
		for {
			delay := time.NewTicker(time.Duration(t.cfg.CollectorIterationsDelay) * time.Second)

			select {
			case <-ctx.Done():
				return
			case <-delay.C:
				break
			}

			if err := t.collect(ctx, storage, rootDir); err != nil {
				t.log.Error(fmt.Sprintf("error collecting: %v", err))
			}
		}
	}()
}

func (t *Trasher) collect(ctx context.Context, storage FileStorage, rootDir string) error {
	shards, err := os.ReadDir(rootDir)
	if err != nil {
		return err
	}

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}

		shardName := filepath.Join(rootDir, shard.Name())

		if err := t.collectShard(ctx, storage, shardName); err != nil {
			t.log.Error(fmt.Sprintf("error collecting shard %s: %v", shardName, err))
		}
	}

	return nil
}

func (t *Trasher) collectShard(ctx context.Context, storage FileStorage, shardDir string) error {
	buckets, err := os.ReadDir(shardDir)
	if err != nil {
		return fmt.Errorf("error reading shard %s: %v", shardDir, err)
	}

	for _, bucketDir := range buckets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := t.collectBucket(storage, bucketDir.Name()); err != nil {
			t.log.Error(fmt.Sprintf("error collecting bucket %s: %v", bucketDir.Name(), err))
		}
	}

	return nil
}

func (t *Trasher) collectBucket(storage FileStorage, bucketDir string) error {
	var bucketID bucket.ID
	if err := bucketID.FromString(bucketDir); err != nil {
		return fmt.Errorf("error reading bucket dir %s: %v", bucketDir, err)
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	meta, err := storage.GetBucketMeta(ctx, bucketID)
	if err != nil {
		return fmt.Errorf("error getting bucket %s: %v", bucketID, err)
	}

	if !meta.TrashTime.Before(time.Now()) {
		return nil
	}

	t.collectedBucketsQueue.Enqueue(bucketID)

	return nil
}

func (t *Trasher) startWorker(ctx context.Context, storage FileStorage) {
	go func() {
		for {
			delay := time.NewTicker(time.Duration(t.cfg.WorkerIterationsDelay) * time.Second)

			select {
			case <-ctx.Done():
				return
			case <-delay.C:
				break
			}

			bucketID := t.collectedBucketsQueue.Dequeue()
			if bucketID == nil {
				continue
			}

			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			if err := storage.RemoveBucket(ctx, *bucketID); err != nil {
				t.log.Error(fmt.Sprintf("error removing bucket %s: %v", bucketID.String(), err))
				continue
			}
		}
	}()
}
