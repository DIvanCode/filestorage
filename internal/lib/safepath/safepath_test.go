package safepath

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestClean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
		valid    bool
	}{
		{name: "simple", input: "file.txt", expected: "file.txt", valid: true},
		{name: "nested", input: "dir/child.txt", expected: filepath.Join("dir", "child.txt"), valid: true},
		{name: "normalized", input: "dir/../file.txt", expected: "file.txt", valid: true},
		{name: "parent traversal", input: "../file.txt"},
		{name: "nested traversal", input: "dir/../../file.txt"},
		{name: "backslash traversal", input: `..\file.txt`},
		{name: "absolute", input: "/tmp/file.txt"},
		{name: "windows absolute", input: `C:\tmp\file.txt`},
		{name: "nul", input: "file\x00.txt"},
		{name: "empty", input: ""},
		{name: "root", input: "."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := Clean(tt.input)
			if !tt.valid {
				require.ErrorIs(t, err, fserrors.ErrInvalidPath)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestLstatRejectsSymlinkComponent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0600))
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	_, _, err := Lstat(root, filepath.Join("link", "secret.txt"))
	require.True(t, errors.Is(err, fserrors.ErrInvalidPath))
}
