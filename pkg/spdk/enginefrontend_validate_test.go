package spdk

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func newTransientStateTestEngineFrontend(deviceInfoErr error) *EngineFrontend {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.initiator = &initiator.Initiator{
		Endpoint:    ef.Endpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn},
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return deviceInfoErr
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error { return nil }
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	return ef
}

func (s *TestSuite) TestValidateNvmeTcpFrontendToleratesTransientDeviceState(c *C) {
	fmt.Println("Testing EngineFrontend.validateAndUpdateNvmeTcpFrontend tolerates a transient device state for a while")

	ef := newTransientStateTestEngineFrontend(errors.New("subsystem NQN nqn.a path nvme0 address traddr=10.0.0.1,trsvcid=2000 is in connecting state"))

	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, false)

	firstSeen := ef.transientDeviceStateSince
	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	c.Assert(ef.transientDeviceStateSince, Equals, firstSeen)
}

func (s *TestSuite) TestValidateNvmeTcpFrontendFailsAfterTransientDeviceStateLasts(c *C) {
	fmt.Println("Testing EngineFrontend.validateAndUpdateNvmeTcpFrontend fails once a transient device state outlasts the tolerance")

	ef := newTransientStateTestEngineFrontend(errors.New("subsystem NQN nqn.a path nvme0 address traddr=10.0.0.1,trsvcid=2000 is in connecting state"))

	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	ef.transientDeviceStateSince = time.Now().Add(-maxTransientDeviceStateDuration - time.Second)

	err := ef.validateAndUpdateNvmeTcpFrontend()
	c.Assert(err, ErrorMatches, ".*stayed in a transient state for over.*")
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, true)
}

func (s *TestSuite) TestValidateNvmeTcpFrontendForgetsTransientDeviceStateOnceRead(c *C) {
	fmt.Println("Testing EngineFrontend.validateAndUpdateNvmeTcpFrontend restarts the tolerance after the device reads cleanly for a while")

	deviceInfoErr := errors.New("subsystem NQN nqn.a path nvme0 address traddr=10.0.0.1,trsvcid=2000 is in resetting state")
	ef := newTransientStateTestEngineFrontend(nil)
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return deviceInfoErr
	}

	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, false)

	deviceInfoErr = nil
	ef.lastTransientDeviceStateAt = time.Now().Add(-transientDeviceStateClearInterval)
	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, true)
}

// A frontend whose target has moved reads the device on one round and fails on it
// the next, so a good read must not put the deadline out of reach.
func (s *TestSuite) TestValidateNvmeTcpFrontendKeepsDeadlineWhileTransientDeviceStateFlaps(c *C) {
	fmt.Println("Testing EngineFrontend.validateAndUpdateNvmeTcpFrontend keeps the deadline when the device alternates between readable and transient")

	deviceInfoErr := errors.New("subsystem NQN nqn.a path nvme0 address traddr=10.0.0.1,trsvcid=2000 is in connecting state")
	ef := newTransientStateTestEngineFrontend(nil)
	fail := false
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		fail = !fail
		if fail {
			return deviceInfoErr
		}
		return nil
	}

	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, false)

	firstSeen := time.Now().Add(-time.Minute)
	ef.transientDeviceStateSince = firstSeen
	for range 8 {
		c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), IsNil)
	}
	c.Assert(ef.transientDeviceStateSince, Equals, firstSeen)

	ef.transientDeviceStateSince = time.Now().Add(-maxTransientDeviceStateDuration - time.Second)
	fail = false
	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), ErrorMatches, ".*stayed in a transient state for over.*")
}

func (s *TestSuite) TestValidateNvmeTcpFrontendReportsNonTransientDeviceError(c *C) {
	fmt.Println("Testing EngineFrontend.validateAndUpdateNvmeTcpFrontend reports an error that is not a transient device state")

	ef := newTransientStateTestEngineFrontend(errors.New("failed to get devices: no such device"))

	c.Assert(ef.validateAndUpdateNvmeTcpFrontend(), ErrorMatches, ".*no such device.*")
	c.Assert(ef.transientDeviceStateSince.IsZero(), Equals, true)
}
