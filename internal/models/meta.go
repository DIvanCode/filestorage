package models

import (
	"github.com/DIvanCode/filestorage/pkg/artifact"
	"time"
)

type Meta struct {
	ID        artifact.ID `json:"id"`
	TrashTime time.Time   `json:"trashTime"`
}
