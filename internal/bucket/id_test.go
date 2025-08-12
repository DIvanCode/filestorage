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
		{id: "0000000000000000000000000000000000000000"},
		{id: "01E23FD9315CAB12409601E23FD9315CAB124096"},
		{id: "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"},
		{id: "A09568817359813EABCDA09568817359813EABCD"},
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
		{id: "0000000000000000P0000000000000000000P000"},
		{id: "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"},
		{id: "FFFGFFFFFFFFFFFFFFFFFFFGFFFFFFFFFFFFFFFF"},
		{id: "A09568817359813EABA09568817359813EAB"},
		{id: ""},
	} {
		t.Run(input.id, func(t *testing.T) {
			var id bucket.ID
			err := id.FromString(input.id)
			require.Error(t, err)
		})
	}
}
