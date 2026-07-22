package tarstream

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
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

		_, err := os.Stat(path)
		require.NoError(t, err)

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
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "x.bin"), []byte("xxx"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(from, "b", "c", "y.txt"), []byte("yyy"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "z.bin"), []byte("zzz"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "x"), []byte("prefix collision"), 0666))
	require.NoError(t, os.WriteFile(filepath.Join(from, "a", "x.bin.backup"), []byte("backup"), 0666))

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

		require.Equal(t, mode.Perm(), st.Mode().Perm())

		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, content, b)
	}
	checkFileNotExist := func(path string) {
		t.Helper()

		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	checkFileExist(filepath.Join(to, "a", "x.bin"), []byte("xxx"), 0755)
	checkFileNotExist(filepath.Join(to, "b", "c", "y.txt"))
	checkFileNotExist(filepath.Join(to, "a", "z.bin"))
	checkFileNotExist(filepath.Join(to, "a", "x"))
	checkFileNotExist(filepath.Join(to, "a", "x.bin.backup"))
}

func TestTarStreamSendFileReceiveStreaming(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	from := "from"
	to := "to"
	require.NoError(t, os.Mkdir(from, 0755))
	require.NoError(t, os.Mkdir(to, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(from, "checker.cpp"), []byte("checker"), 0755))

	reader, writer := io.Pipe()
	sendErr := make(chan error, 1)
	go func() {
		err := SendFile("checker.cpp", from, writer)
		_ = writer.CloseWithError(err)
		sendErr <- err
	}()

	require.NoError(t, Receive(to, reader))
	require.NoError(t, <-sendErr)
	require.FileExists(t, filepath.Join(to, "checker.cpp"))
}

func TestSendErrorBeforeFirstHeaderDoesNotWriteEmptyArchive(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	require.NoError(t, os.Mkdir("from", 0755))
	require.NoError(t, os.WriteFile(filepath.Join("from", "checker.cpp"), []byte("checker"), 0644))
	absoluteFile, err := filepath.Abs(filepath.Join("from", "checker.cpp"))
	require.NoError(t, err)

	var buf bytes.Buffer
	err = send("from", absoluteFile, true, &buf)
	require.Error(t, err)
	require.Empty(t, buf.Bytes())
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

type testTarEntry struct {
	header tar.Header
	data   []byte
}

func makeTar(t *testing.T, entries ...testTarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		header := entry.header
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			header.Size = int64(len(entry.data))
		}
		require.NoError(t, tw.WriteHeader(&header))
		if len(entry.data) > 0 {
			_, err := tw.Write(entry.data)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func TestReceiveRejectsUnsafeEntries(t *testing.T) {
	tests := []struct {
		name       string
		header     tar.Header
		data       []byte
		setAbsName bool
	}{
		{
			name:   "parent traversal",
			header: tar.Header{Name: "../outside.txt", Typeflag: tar.TypeReg, Mode: 0644},
			data:   []byte("escaped"),
		},
		{
			name:       "absolute path",
			header:     tar.Header{Typeflag: tar.TypeReg, Mode: 0644},
			data:       []byte("escaped"),
			setAbsName: true,
		},
		{
			name:   "symlink",
			header: tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../outside.txt"},
		},
		{
			name:   "hardlink",
			header: tar.Header{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "../outside.txt"},
		},
		{
			name:   "device",
			header: tar.Header{Name: "device", Typeflag: tar.TypeChar},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			destination := filepath.Join(base, "destination")
			require.NoError(t, os.Mkdir(destination, 0755))
			outside := filepath.Join(base, "outside.txt")
			header := tt.header
			if tt.setAbsName {
				header.Name = filepath.ToSlash(outside)
			}
			archive := makeTar(t, testTarEntry{header: header, data: tt.data})

			err := Receive(destination, bytes.NewReader(archive))
			require.True(t, errors.Is(err, fserrors.ErrInvalidArchive), "unexpected error: %v", err)
			require.NoFileExists(t, outside)
		})
	}
}

func TestReceiveRejectsArchiveOverLimits(t *testing.T) {
	tests := []struct {
		name    string
		limits  Limits
		entries []testTarEntry
	}{
		{
			name:   "single file size",
			limits: Limits{MaxEntries: 10, MaxFiles: 10, MaxFileSize: 3, MaxTotalSize: 10},
			entries: []testTarEntry{
				{header: tar.Header{Name: "large.txt", Typeflag: tar.TypeReg}, data: []byte("1234")},
			},
		},
		{
			name:   "total size",
			limits: Limits{MaxEntries: 10, MaxFiles: 10, MaxFileSize: 10, MaxTotalSize: 5},
			entries: []testTarEntry{
				{header: tar.Header{Name: "first.txt", Typeflag: tar.TypeReg}, data: []byte("123")},
				{header: tar.Header{Name: "second.txt", Typeflag: tar.TypeReg}, data: []byte("456")},
			},
		},
		{
			name:   "file count",
			limits: Limits{MaxEntries: 10, MaxFiles: 1, MaxFileSize: 10, MaxTotalSize: 10},
			entries: []testTarEntry{
				{header: tar.Header{Name: "first.txt", Typeflag: tar.TypeReg}},
				{header: tar.Header{Name: "second.txt", Typeflag: tar.TypeReg}},
			},
		},
		{
			name:   "entry count",
			limits: Limits{MaxEntries: 1, MaxFiles: 10, MaxFileSize: 10, MaxTotalSize: 10},
			entries: []testTarEntry{
				{header: tar.Header{Name: "first", Typeflag: tar.TypeDir}},
				{header: tar.Header{Name: "second", Typeflag: tar.TypeDir}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archive := makeTar(t, tt.entries...)
			err := ReceiveWithLimits(t.TempDir(), bytes.NewReader(archive), tt.limits)
			require.ErrorIs(t, err, fserrors.ErrArchiveTooLarge)
		})
	}
}

func TestReceiveRestrictsWritablePermissions(t *testing.T) {
	archive := makeTar(t, testTarEntry{
		header: tar.Header{Name: "executable", Typeflag: tar.TypeReg, Mode: 0777},
		data:   []byte("content"),
	})
	destination := t.TempDir()

	require.NoError(t, Receive(destination, bytes.NewReader(archive)))
	info, err := os.Stat(filepath.Join(destination, "executable"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

func TestSendRejectsSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0600))
	if err := os.Symlink(outside, filepath.Join(root, "leak")); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	var buf bytes.Buffer
	require.ErrorIs(t, Send(root, &buf), fserrors.ErrInvalidPath)
	buf.Reset()
	require.ErrorIs(t, SendFile("leak", root, &buf), fserrors.ErrInvalidPath)
}

func TestReceiveRejectsPreexistingSymlinkParent(t *testing.T) {
	base := t.TempDir()
	destination := filepath.Join(base, "destination")
	outside := filepath.Join(base, "outside")
	require.NoError(t, os.Mkdir(destination, 0755))
	require.NoError(t, os.Mkdir(outside, 0755))
	if err := os.Symlink(outside, filepath.Join(destination, "link")); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
	archive := makeTar(t, testTarEntry{
		header: tar.Header{Name: "link/escaped.txt", Typeflag: tar.TypeReg, Mode: 0644},
		data:   []byte("escaped"),
	})

	err := Receive(destination, bytes.NewReader(archive))
	require.ErrorIs(t, err, fserrors.ErrInvalidArchive)
	require.NoFileExists(t, filepath.Join(outside, "escaped.txt"))
}

func TestSendFileDirectoryRoundTrip(t *testing.T) {
	from := t.TempDir()
	to := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(from, "selected"), 0755))
	require.NoError(t, os.Mkdir(filepath.Join(from, "selected-prefix"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(from, "selected", "file.txt"), []byte("selected"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(from, "selected-prefix", "leak.txt"), []byte("leak"), 0644))

	var buf bytes.Buffer
	require.NoError(t, SendFile("selected", from, &buf))
	require.NoError(t, Receive(to, &buf))
	require.FileExists(t, filepath.Join(to, "selected", "file.txt"))
	require.NoFileExists(t, filepath.Join(to, "selected-prefix", "leak.txt"))
}
