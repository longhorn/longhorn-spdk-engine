package spdk

import "github.com/cockroachdb/errors"

var (
	// ErrRecoverDeviceNotFound indicates the NVMe device was not found on the
	// host during recovery. The persisted record should be removed.
	ErrRecoverDeviceNotFound = errors.New("device not found on host during recovery")
)
