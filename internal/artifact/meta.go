package artifact

import (
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
	"time"
)

type Meta struct {
	ID        id.ID     `json:"id"`
	TrashTime time.Time `json:"trash_time"`
}
