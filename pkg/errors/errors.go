package errors

import "errors"

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrFileNotFound        = errors.New("file not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrFileAlreadyExists   = errors.New("file already exists")
	ErrInvalidPath         = errors.New("invalid relative path")
	ErrInvalidArchive      = errors.New("invalid tar archive")
	ErrArchiveTooLarge     = errors.New("tar archive exceeds limits")
	ErrWriteLocked         = errors.New("bucket is locked for write")
	ErrReadLocked          = errors.New("bucket is locked for read")
)
