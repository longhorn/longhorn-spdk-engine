package spdk

import "github.com/cockroachdb/errors"

var (
	// ErrExpansionInProgress indicates expansion cannot proceed because another
	// expansion operation is already running.
	ErrExpansionInProgress = errors.New("expansion is in progress")
	// ErrRestoringInProgress indicates expansion cannot proceed while restoring.
	ErrRestoringInProgress = errors.New("restoring is in progress")
	// ErrExpansionInvalidSize indicates an invalid target size for expansion.
	ErrExpansionInvalidSize = errors.New("invalid expansion size")
)
