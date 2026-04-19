package lsm

import "errors"

var (
	ErrInvalidMagicNumber = errors.New("lsm: invalid checkpoint magic number")
	ErrCheckpointNotFound = errors.New("lsm: checkpoint not found")
)
