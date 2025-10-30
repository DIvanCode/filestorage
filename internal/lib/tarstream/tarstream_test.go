package tarstream

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTarStreamSendReceive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tarstream")
	require.NoError(t, err)

	from := filepath.Join(tmpDir, "from")
	to := filepath.Join(tmpDir, "to")

	require.NoError(t, os.Mkdir(from, 0777))
	require.NoError(t, os.Mkdir(to, 0777))

	var buf bytes.Buffer

	require.NoError(t, os.MkdirAll(filepath.Join(from, "a"), 0777))
	require.NoError(t, os.MkdirAll(filepath.Join(from, "b", "c", "d"), 0777))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "x.bin"), []byte("xxx"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "b", "c", "y.txt"), []byte("yyy"), 0666))

	require.NoError(t, Send(from, &buf))
	require.NoError(t, Receive(to, &buf))

	checkDir := func(path string) {
		t.Helper()

		st, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, st.IsDir())
	}

	checkDir(filepath.Join(to, "a"))
	checkDir(filepath.Join(to, "b", "c", "d"))

	checkFile := func(path string, content []byte, mode os.FileMode) {
		t.Helper()

		st, err := os.Stat(path)
		require.NoError(t, err)

		require.Equal(t, mode.String(), st.Mode().String())

		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, content, b)
	}

	checkFile(filepath.Join(to, "a", "x.bin"), []byte("xxx"), 0666)
	checkFile(filepath.Join(to, "b", "c", "y.txt"), []byte("yyy"), 0666)
}

func TestTarStreamSendFileReceive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tarstream")
	require.NoError(t, err)

	from := filepath.Join(tmpDir, "from")
	to := filepath.Join(tmpDir, "to")

	require.NoError(t, os.Mkdir(from, 0777))
	require.NoError(t, os.Mkdir(to, 0777))

	var buf bytes.Buffer

	require.NoError(t, os.MkdirAll(filepath.Join(from, "a"), 0777))
	require.NoError(t, os.MkdirAll(filepath.Join(from, "b", "c", "d"), 0777))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "x.bin"), []byte("xxx"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "b", "c", "y.txt"), []byte("yyy"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "z.bin"), []byte("zzz"), 0666))

	require.NoError(t, SendFile(filepath.Join("a", "x.bin"), from, &buf))
	require.NoError(t, Receive(to, &buf))

	checkDirExist := func(path string) {
		t.Helper()

		st, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, st.IsDir())
	}
	checkDirNotExist := func(path string) {
		t.Helper()

		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	checkDirExist(filepath.Join(to, "a"))
	checkDirNotExist(filepath.Join(to, "b", "c", "d"))

	checkFileExist := func(path string, content []byte, mode os.FileMode) {
		t.Helper()

		st, err := os.Stat(path)
		require.NoError(t, err)

		require.Equal(t, mode.String(), st.Mode().String())

		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, content, b)
	}
	checkFileNotExist := func(path string) {
		t.Helper()

		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	checkFileExist(filepath.Join(to, "a", "x.bin"), []byte("xxx"), 0666)
	checkFileNotExist(filepath.Join(to, "b", "c", "y.txt"))
	checkFileNotExist(filepath.Join(to, "a", "z.bin"))
}

func TestTarStreamExecutablePermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tarstream-exec")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	from := filepath.Join(tmpDir, "from")
	to := filepath.Join(tmpDir, "to")

	require.NoError(t, os.Mkdir(from, 0777))
	require.NoError(t, os.Mkdir(to, 0777))

	var buf bytes.Buffer

	require.NoError(t, os.WriteFile(filepath.Join(from, "normal.txt"), []byte("normal file"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(from, "executable.sh"), []byte("#!/bin/bash\necho hello"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(from, "readonly.txt"), []byte("read only"), 0444))
	require.NoError(t, os.WriteFile(filepath.Join(from, "user_exec.go"), []byte("package main"), 0744))

	require.NoError(t, Send(from, &buf))
	require.NoError(t, Receive(to, &buf))

	checkPermissions := func(path string, expectedMode os.FileMode) {
		t.Helper()

		st, err := os.Stat(path)
		require.NoError(t, err)

		actualPerm := st.Mode().Perm()
		expectedPerm := expectedMode.Perm()

		require.Equal(t, expectedPerm, actualPerm,
			"File %s: expected permissions %o, got %o",
			path, expectedPerm, actualPerm)
	}

	checkPermissions(filepath.Join(to, "normal.txt"), 0644)
	checkPermissions(filepath.Join(to, "executable.sh"), 0755)
	checkPermissions(filepath.Join(to, "readonly.txt"), 0444)
	checkPermissions(filepath.Join(to, "user_exec.go"), 0744)

	content, err := os.ReadFile(filepath.Join(to, "executable.sh"))
	require.NoError(t, err)
	require.Equal(t, "#!/bin/bash\necho hello", string(content))
}
