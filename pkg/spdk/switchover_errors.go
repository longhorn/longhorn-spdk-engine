package spdk

import "github.com/cockroachdb/errors"

var (
	// ErrSwitchOverTargetInvalidInput indicates invalid user input for a target switchover request.
	ErrSwitchOverTargetInvalidInput = errors.New("invalid switchover target request")
	// ErrSwitchOverTargetPrecondition indicates the current frontend state cannot satisfy switchover preconditions.
	ErrSwitchOverTargetPrecondition = errors.New("switchover target precondition failed")
	// ErrSwitchOverTargetEngineNotFound indicates no engine can be resolved from the target side.
	ErrSwitchOverTargetEngineNotFound = errors.New("cannot find target engine for switchover")
	// ErrSwitchOverTargetInternal indicates switchover execution failed due to runtime/internal reasons.
	ErrSwitchOverTargetInternal = errors.New("failed to switch over target")
)
