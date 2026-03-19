package spdk

import "github.com/cockroachdb/errors"

var (
	// ErrEngineFrontendCreateInvalidArgument indicates the create request carries
	// invalid input, such as an unparsable target address.
	ErrEngineFrontendCreateInvalidArgument = errors.New("engine frontend create invalid argument")
	// ErrEngineFrontendCreatePrecondition indicates the frontend is not in a
	// state that can satisfy create preconditions.
	ErrEngineFrontendCreatePrecondition = errors.New("engine frontend create precondition failed")
	// ErrEngineFrontendLifecyclePrecondition indicates suspend/resume/delete
	// cannot proceed because the frontend is in an incompatible state.
	ErrEngineFrontendLifecyclePrecondition = errors.New("engine frontend lifecycle precondition failed")
	// ErrEngineFrontendLifecycleUnimplemented indicates the requested lifecycle
	// operation is not implemented for the current frontend type.
	ErrEngineFrontendLifecycleUnimplemented = errors.New("engine frontend lifecycle unimplemented")
)
