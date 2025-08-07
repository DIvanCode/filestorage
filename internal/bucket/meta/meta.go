package meta

import (
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"time"
)

type BucketMeta struct {
	BucketID  bucket.ID `json:"id"`
	TrashTime time.Time `json:"trash_time"`
}
