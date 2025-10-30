package tarstream

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Send рекурсивно обходит директорию и сериализует её содержимое в поток w.
func Send(dir string, w io.Writer) error {
	tw := tar.NewWriter(w)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk filepath: %w", err)
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path of %s: %w", path, err)
		}

		if rel == "." {
			return nil
		}

		if info.IsDir() {
			if err := tw.WriteHeader(&tar.Header{
				Name:     rel,
				Typeflag: tar.TypeDir,
			}); err != nil {
				return fmt.Errorf("failed to write dir (%s) header: %w", path, err)
			}
			return nil
		}

		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     rel,
			Size:     info.Size(),
			Mode:     int64(info.Mode()),
		}); err != nil {
			return fmt.Errorf("failed to write file (%s) header: %w", path, err)
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer func() { _ = f.Close() }()

		if _, err = io.Copy(tw, f); err != nil {
			return fmt.Errorf("failed to write file %s: %w", path, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	if err = tw.Close(); err != nil {
		return fmt.Errorf("failed to close tarstream: %w", err)
	}

	return nil
}

// SendFile берём файл вместе с путём и сериализует его содержимое в поток w.
func SendFile(file, dir string, w io.Writer) error {
	tw := tar.NewWriter(w)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk filepath: %w", err)
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path of %s: %w", path, err)
		}

		if rel == "." || !strings.HasPrefix(normalize(file), normalize(rel)) {
			return nil
		}

		if info.IsDir() {
			if err := tw.WriteHeader(&tar.Header{
				Name:     rel,
				Typeflag: tar.TypeDir,
			}); err != nil {
				return fmt.Errorf("failed to write dir (%s) header: %w", path, err)
			}
			return nil
		}

		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     rel,
			Size:     info.Size(),
			Mode:     int64(info.Mode()),
		}); err != nil {
			return fmt.Errorf("failed to write file (%s) header: %w", path, err)
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer func() { _ = f.Close() }()

		if _, err = io.Copy(tw, f); err != nil {
			return fmt.Errorf("failed to write file %s: %w", path, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	if err = tw.Close(); err != nil {
		return fmt.Errorf("failed to close tarstream: %w", err)
	}

	return nil
}

// Receive читает поток r и материализует содержимое потока внутри dir.
func Receive(dir string, r io.Reader) error {
	tr := tar.NewReader(r)

	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from tarstream: %w", err)
		}

		absPath := filepath.Join(dir, h.Name)

		if h.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(absPath, 0777); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", h.Name, err)
			}
			continue
		}

		receiveFile := func() error {
			f, err := os.OpenFile(absPath, os.O_CREATE|os.O_WRONLY, os.FileMode(h.Mode)&0777)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", h.Name, err)
			}
			defer func() { _ = f.Close() }()

			if _, err = io.Copy(f, tr); err != nil {
				return fmt.Errorf("failed to write file (%s) data: %w", h.Name, err)
			}

			return nil
		}

		if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
			return fmt.Errorf("failed to create subdirectories of file %s: %w", h.Name, err)
		}

		if err := receiveFile(); err != nil {
			return fmt.Errorf("failed to receive file %s: %w", h.Name, err)
		}
	}

	return nil
}

func normalize(path string) string {
	return filepath.Clean(filepath.FromSlash(path))
}
