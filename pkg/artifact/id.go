package artifact

import (
	"encoding/hex"
	"encoding/json"
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

func (id *ID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("artifact.ID should be a string, got %s", data)
	}
	return id.FromString(s)
}

func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}
