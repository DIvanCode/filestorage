package safepath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	fserrors "github.com/DIvanCode/filestorage/pkg/errors"
)

// Clean validates an untrusted relative path and returns its canonical,
// platform-native representation. Both slash styles are treated as separators
// so a path cannot become unsafe when a transfer crosses operating systems.
func Clean(name string) (string, error) {
	if name == "" || strings.IndexByte(name, 0) >= 0 {
		return "", fserrors.ErrInvalidPath
	}

	portable := strings.ReplaceAll(name, `\`, "/")
	if strings.HasPrefix(portable, "/") || hasWindowsVolume(portable) {
		return "", fserrors.ErrInvalidPath
	}

	clean := filepath.Clean(filepath.FromSlash(portable))
	if clean == "." || filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
		return "", fserrors.ErrInvalidPath
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fserrors.ErrInvalidPath
	}

	return clean, nil
}

// Resolve validates name and returns a target proven to be contained in root.
func Resolve(root, name string) (clean, target string, err error) {
	clean, err = Clean(name)
	if err != nil {
		return "", "", err
	}

	root, err = filepath.Abs(root)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve root: %w", err)
	}
	target = filepath.Join(root, clean)
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return "", "", fmt.Errorf("failed to check path containment: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", "", fserrors.ErrInvalidPath
	}

	return clean, target, nil
}

// Lstat resolves name inside root and rejects symlinks in every existing path
// component. It returns os.ErrNotExist when the target itself is missing.
func Lstat(root, name string) (os.FileInfo, string, error) {
	clean, target, err := Resolve(root, name)
	if err != nil {
		return nil, "", err
	}

	if err := validateRoot(root); err != nil {
		return nil, "", err
	}

	current, err := filepath.Abs(root)
	if err != nil {
		return nil, "", fmt.Errorf("failed to resolve root: %w", err)
	}
	parts := strings.Split(clean, string(filepath.Separator))
	var info os.FileInfo
	for i, part := range parts {
		current = filepath.Join(current, part)
		var statErr error
		info, statErr = os.Lstat(current)
		if statErr != nil {
			return nil, target, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, target, fmt.Errorf("%w: symlink component %q", fserrors.ErrInvalidPath, part)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return nil, target, fmt.Errorf("%w: non-directory component %q", fserrors.ErrInvalidPath, part)
		}
	}

	return info, target, nil
}

// MkdirAll creates a directory path within root without following symlinks.
func MkdirAll(root, name string, perm os.FileMode) error {
	if name == "" || name == "." {
		return validateRoot(root)
	}

	clean, _, err := Resolve(root, name)
	if err != nil {
		return err
	}
	if err := validateRoot(root); err != nil {
		return err
	}

	current, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("failed to resolve root: %w", err)
	}
	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		switch {
		case statErr == nil:
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("%w: unsafe directory component %q", fserrors.ErrInvalidPath, part)
			}
		case errors.Is(statErr, os.ErrNotExist):
			if err := os.Mkdir(current, perm); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err = os.Lstat(current)
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("%w: unsafe directory component %q", fserrors.ErrInvalidPath, part)
			}
		default:
			return statErr
		}
	}

	return nil
}

func validateRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: unsafe root directory", fserrors.ErrInvalidPath)
	}
	return nil
}

func hasWindowsVolume(name string) bool {
	return len(name) >= 2 && unicode.IsLetter(rune(name[0])) && name[1] == ':'
}
