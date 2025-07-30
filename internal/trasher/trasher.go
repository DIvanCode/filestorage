package trasher

import (
	"context"
	"filestorage/internal/artifact"
	"filestorage/pkg/config"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Trasher struct {
	cfg config.TrasherConfig

	collectedArtifactsQueue queue

	cancelFunc context.CancelFunc

	log *slog.Logger
}

type TrashableStorage interface {
	GetArtifactMeta(artifactID artifact.ID) (artifact.Meta, error)
	RemoveArtifact(artifactID artifact.ID) error
}

func NewTrasher(log *slog.Logger, cfg config.TrasherConfig) (*Trasher, error) {
	trasher := &Trasher{
		cfg: cfg,

		collectedArtifactsQueue: queue{},

		log: log,
	}

	return trasher, nil
}

func (t *Trasher) Start(storage TrashableStorage, rootDir string) {
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

func (t *Trasher) startCollector(ctx context.Context, storage TrashableStorage, rootDir string) {
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

func (t *Trasher) collect(ctx context.Context, storage TrashableStorage, rootDir string) error {
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

func (t *Trasher) collectShard(ctx context.Context, storage TrashableStorage, shardDir string) error {
	artifacts, err := os.ReadDir(shardDir)
	if err != nil {
		return fmt.Errorf("error reading shard %s: %v", shardDir, err)
	}

	for _, artifactDir := range artifacts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := t.collectArtifact(storage, artifactDir.Name()); err != nil {
			t.log.Error(fmt.Sprintf("error collecting artifact %s: %v", artifactDir.Name(), err))
		}
	}

	return nil
}

func (t *Trasher) collectArtifact(storage TrashableStorage, artifactDir string) error {
	var artifactID artifact.ID
	if err := artifactID.FromString(artifactDir); err != nil {
		return fmt.Errorf("error reading artifact dir %s: %v", artifactDir, err)
	}

	meta, err := storage.GetArtifactMeta(artifactID)
	if err != nil {
		return fmt.Errorf("error getting artifact %s: %v", artifactID, err)
	}

	if !meta.TrashTime.Before(time.Now()) {
		return nil
	}

	t.collectedArtifactsQueue.enqueue(artifactID)

	return nil
}

func (t *Trasher) startWorker(ctx context.Context, storage TrashableStorage) {
	go func() {
		for {
			delay := time.NewTicker(time.Duration(t.cfg.WorkerIterationsDelay) * time.Second)

			select {
			case <-ctx.Done():
				return
			case <-delay.C:
				break
			}

			artifactID := t.collectedArtifactsQueue.dequeue()
			if artifactID == nil {
				continue
			}

			if err := storage.RemoveArtifact(*artifactID); err != nil {
				t.log.Error(fmt.Sprintf("error removing artifact %s: %v", *artifactID, err))
				continue
			}
		}
	}()
}

type node struct {
	artifactID artifact.ID
	next       *node
}

type queue struct {
	mu   sync.Mutex
	head *node
	tail *node
}

func (q *queue) enqueue(artifactID artifact.ID) {
	node := &node{artifactID: artifactID, next: nil}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		q.head = node
		q.tail = node
	} else {
		q.tail.next = node
		q.tail = node
	}
}

func (q *queue) dequeue() *artifact.ID {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return nil
	}

	id := q.head.artifactID
	q.head = q.head.next
	return &id
}
