package tarstream

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
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
