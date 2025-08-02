package id

import (
	"github.com/DIvanCode/filestorage/pkg/artifact/id"
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
		{id: "01E23FD9315CAB124096"},
		{id: "FFFFFFFFFFFFFFFFFFFF"},
		{id: "A09568817359813EABCD"},
	} {
		t.Run(input.id, func(t *testing.T) {
			var artifactID id.ID
			err := artifactID.FromString(input.id)
			require.NoError(t, err)
			assert.Equal(t, input.id, artifactID.String())
		})
	}
}

func TestIDFromStringError(t *testing.T) {
	for _, input := range []testCase{
		{id: "0000000000000000P000"},
		{id: "=-=-=-=-=-=-=-=-=-=-"},
		{id: "FFFGFFFFFFFFFFFFFFFF"},
		{id: "A09568817359813EAB"},
		{id: ""},
	} {
		t.Run(input.id, func(t *testing.T) {
			var artifactID id.ID
			err := artifactID.FromString(input.id)
			require.Error(t, err)
		})
	}
}
