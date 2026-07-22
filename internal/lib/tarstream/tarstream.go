package tarstream

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/DIvanCode/filestorage/internal/lib/safepath"
	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
)

const (
	DefaultMaxEntries   = 100_000
	DefaultMaxFiles     = 50_000
	DefaultMaxFileSize  = int64(1 << 30) // 1 GiB
	DefaultMaxTotalSize = int64(4 << 30) // 4 GiB
)

type Limits struct {
	MaxEntries   int
	MaxFiles     int
	MaxFileSize  int64
	MaxTotalSize int64
}

var defaultLimits = Limits{
	MaxEntries:   DefaultMaxEntries,
	MaxFiles:     DefaultMaxFiles,
	MaxFileSize:  DefaultMaxFileSize,
	MaxTotalSize: DefaultMaxTotalSize,
}

// Send recursively serializes a directory without following symlinks.
func Send(dir string, w io.Writer) error {
	root, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("failed to resolve send root: %w", err)
	}
	if err := validateSendRoot(root); err != nil {
		return err
	}
	return send(root, root, false, w)
}

// SendFile serializes exactly one selected regular file or directory tree.
func SendFile(file, dir string, w io.Writer) error {
	root, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("failed to resolve send root: %w", err)
	}
	clean, _, err := safepath.Resolve(root, file)
	if err != nil {
		return fmt.Errorf("failed to validate selected file: %w", err)
	}
	info, target, err := safepath.Lstat(root, clean)
	if err != nil {
		return fmt.Errorf("failed to locate selected file: %w", err)
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return fmt.Errorf("%w: selected path has unsupported type", fserrors.ErrInvalidPath)
	}

	return send(root, target, true, w)
}

func send(root, start string, includeStart bool, w io.Writer) error {
	tw := tar.NewWriter(w)
	err := filepath.Walk(start, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("failed to walk filepath: %w", walkErr)
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path of %s: %w", path, err)
		}
		if rel == "." && !includeStart {
			return nil
		}

		clean, _, err := safepath.Resolve(root, rel)
		if err != nil {
			return fmt.Errorf("failed to validate path %s: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: refusing to send symlink %s", fserrors.ErrInvalidPath, clean)
		}

		header := &tar.Header{Name: filepath.ToSlash(clean)}
		switch {
		case info.IsDir():
			header.Typeflag = tar.TypeDir
			header.Mode = 0755
			if err := tw.WriteHeader(header); err != nil {
				return fmt.Errorf("failed to write directory %s header: %w", path, err)
			}
			return nil
		case info.Mode().IsRegular():
			header.Typeflag = tar.TypeReg
			header.Size = info.Size()
			header.Mode = int64(info.Mode().Perm() & 0755)
		default:
			return fmt.Errorf("%w: refusing to send unsupported file type %s", fserrors.ErrInvalidPath, clean)
		}

		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write file %s header: %w", path, err)
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return fmt.Errorf("failed to write file %s: %w", path, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("failed to close file %s: %w", path, closeErr)
		}

		return nil
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("failed to close tarstream: %w", err)
	}
	return nil
}

// Receive materializes a tar stream inside dir using bounded, safe defaults.
func Receive(dir string, r io.Reader) error {
	return ReceiveWithLimits(dir, r, defaultLimits)
}

func ReceiveWithLimits(dir string, r io.Reader, limits Limits) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if err := validateSendRoot(dir); err != nil {
		return fmt.Errorf("failed to validate destination: %w", err)
	}

	tr := tar.NewReader(r)
	seen := make(map[string]struct{})
	entries := 0
	files := 0
	var totalSize int64

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: failed to read tar stream: %v", fserrors.ErrInvalidArchive, err)
		}

		entries++
		if entries > limits.MaxEntries {
			return fmt.Errorf("%w: too many entries", fserrors.ErrArchiveTooLarge)
		}

		clean, target, err := safepath.Resolve(dir, header.Name)
		if err != nil {
			return fmt.Errorf("%w: unsafe entry %q: %v", fserrors.ErrInvalidArchive, header.Name, err)
		}
		if _, duplicate := seen[clean]; duplicate {
			return fmt.Errorf("%w: duplicate entry %q", fserrors.ErrInvalidArchive, header.Name)
		}
		seen[clean] = struct{}{}

		switch header.Typeflag {
		case tar.TypeDir:
			if header.Size != 0 {
				return fmt.Errorf("%w: directory %q has data", fserrors.ErrInvalidArchive, header.Name)
			}
			if err := safepath.MkdirAll(dir, clean, 0755); err != nil {
				return fmt.Errorf("%w: failed to create directory %q: %v", fserrors.ErrInvalidArchive, header.Name, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			files++
			if files > limits.MaxFiles {
				return fmt.Errorf("%w: too many files", fserrors.ErrArchiveTooLarge)
			}
			if header.Size < 0 || header.Size > limits.MaxFileSize {
				return fmt.Errorf("%w: file %q is too large", fserrors.ErrArchiveTooLarge, header.Name)
			}
			if header.Size > limits.MaxTotalSize-totalSize {
				return fmt.Errorf("%w: total file size is too large", fserrors.ErrArchiveTooLarge)
			}
			totalSize += header.Size

			parent := filepath.Dir(clean)
			if parent != "." {
				if err := safepath.MkdirAll(dir, parent, 0755); err != nil {
					return fmt.Errorf("%w: failed to create parent of %q: %v", fserrors.ErrInvalidArchive, header.Name, err)
				}
			}
			if info, _, statErr := safepath.Lstat(dir, clean); statErr == nil {
				if !info.Mode().IsRegular() {
					return fmt.Errorf("%w: entry %q would replace a non-regular file", fserrors.ErrInvalidArchive, header.Name)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return fmt.Errorf("%w: failed to inspect entry %q: %v", fserrors.ErrInvalidArchive, header.Name, statErr)
			}

			mode := os.FileMode(header.Mode) & 0755
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return fmt.Errorf("failed to create file %q: %w", header.Name, err)
			}
			written, copyErr := io.CopyN(f, tr, header.Size)
			chmodErr := f.Chmod(mode)
			closeErr := f.Close()
			if copyErr != nil || written != header.Size {
				return fmt.Errorf("%w: incomplete data for %q", fserrors.ErrInvalidArchive, header.Name)
			}
			if chmodErr != nil {
				return fmt.Errorf("failed to set permissions on %q: %w", header.Name, chmodErr)
			}
			if closeErr != nil {
				return fmt.Errorf("failed to close file %q: %w", header.Name, closeErr)
			}
		default:
			return fmt.Errorf("%w: unsupported type %d for %q", fserrors.ErrInvalidArchive, header.Typeflag, header.Name)
		}
	}

	return nil
}

func validateSendRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: unsafe root directory", fserrors.ErrInvalidPath)
	}
	return nil
}

func validateLimits(limits Limits) error {
	if limits.MaxEntries <= 0 || limits.MaxFiles <= 0 || limits.MaxFileSize < 0 || limits.MaxTotalSize < 0 {
		return fmt.Errorf("invalid tar limits")
	}
	return nil
}
