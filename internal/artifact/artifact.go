package artifact

import (
	"fmt"
	"time"
)

type ID [20]byte

func (artifactID *ID) String() string {
	return string(artifactID[:])
}

func (artifactID *ID) FromString(id string) error {
	if len(id) != len(artifactID) {
		return fmt.Errorf("invalid id size: %d", len(id))
	}

	for _, x := range id {
		if ('0' <= x && x <= '9') || ('A' <= x && x <= 'F') {
			continue
		}
		return fmt.Errorf("id contains restricted symbol: %c", x)
	}

	copy(artifactID[:], id)
	return nil
}

type Meta struct {
	ID        ID        `json:"id"`
	TrashTime time.Time `json:"trashTime"`
}
