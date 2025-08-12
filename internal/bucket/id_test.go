package bucket

import (
	"github.com/DIvanCode/filestorage/pkg/bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type testCase struct {
	id string
}

func TestIDFromStringOK(t *testing.T) {
	for _, input := range []testCase{
		{id: "00000000000000000000"},
		{id: "01e23fd9315cab124096"},
		{id: "ffffffffffffffffffff"},
		{id: "a095617ef0fac83eabcd"},
	} {
		t.Run(input.id, func(t *testing.T) {
			var id bucket.ID
			err := id.FromString(input.id)
			require.NoError(t, err)
			assert.Equal(t, input.id, id.String())
		})
	}
}

func TestIDFromStringError(t *testing.T) {
	for _, input := range []testCase{
		{id: "0000000000000000F000"},
		{id: "0000000000000000g000"},
		{id: "=-=-=-=-=-=-=-=-=-=-"},
		{id: "fffffffffffffffgffff"},
		{id: "a095617ef0fac83eabc"},
		{id: "a095617ef0fac83eabcff"},
		{id: ""},
	} {
		t.Run(input.id, func(t *testing.T) {
			var id bucket.ID
			err := id.FromString(input.id)
			require.Error(t, err)
		})
	}
}
