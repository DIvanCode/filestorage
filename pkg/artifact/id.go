package artifact

import (
	"encoding/hex"
	"fmt"
	"strings"
)

type ID [10]byte

func (id *ID) FromString(s string) error {
	if len(s) != 20 {
		return fmt.Errorf("invalid string length")
	}
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid hex string: %w", err)
	}
	copy(id[:], bytes)
	return nil
}

func (id *ID) String() string {
	return strings.ToUpper(hex.EncodeToString(id[:]))
}
