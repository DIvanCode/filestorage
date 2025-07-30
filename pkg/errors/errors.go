package errors

import "errors"

var (
	ErrNotFound      = errors.New("artifact not found")
	ErrAlreadyExists = errors.New("artifact already exists")
	ErrWriteLocked   = errors.New("artifact is locked for write")
	ErrReadLocked    = errors.New("artifact is locked for read")
)
