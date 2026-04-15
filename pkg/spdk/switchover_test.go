package spdk

import (
	"context"
	"fmt"
	"strings"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func stubSwitchoverANASync(ef *EngineFrontend, err error) {
	ef.syncRemoteEngineTargetANAStatesFn = func(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
		return err
	}
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		return err
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetNvmfSuccess(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP NVMe-oF frontend with successful switchover")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)

	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = "nqn.2014-08.org.nvmexpress:uuid:test-a"
	ef.NvmeTcpFrontend.Nguid = "old-nguid"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
	ef.syncCurrentNVMeTCPPathLocked()
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))

	expectedNQN := getStableVolumeNQN("vol-a")
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, expectedNQN)

	expectedEndpoint := GetNvmfEndpoint(expectedNQN, "10.0.0.2", 3000)
	c.Assert(ef.Endpoint, Equals, expectedEndpoint)
	c.Assert(ef.ActivePath, Equals, "10.0.0.2:3000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 2)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.2:3000"].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateInaccessible)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after target switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevRunningUsesMultipathConnect(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend uses multipath connect while running")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		c.Assert(subsystemNQN, Equals, getStableVolumeNQN("vol-a"))
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		return nil
	}
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	called := false
	stubSwitchoverANASync(ef, nil)

	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		called = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.Endpoint, Equals, "/dev/longhorn/vol-a")

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev switchover")
	}
}

func (s *TestSuite) TestEngineFrontendGetExportsMultipathState(c *C) {
	fmt.Println("Testing EngineFrontend.Get exports multipath path state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.ActivePath = "10.0.0.1:2000"
	ef.PreferredPath = "10.0.0.2:3000"
	ef.NvmeTCPPathMap = map[string]*NvmeTCPPath{
		"10.0.0.1:2000": {
			TargetIP:   "10.0.0.1",
			TargetPort: 2000,
			EngineName: "engine-a",
			Nqn:        getStableVolumeNQN("vol-a"),
			Nguid:      getStableVolumeNGUID("vol-a"),
			ANAState:   NvmeTCPANAStateOptimized,
		},
		"10.0.0.2:3000": {
			TargetIP:   "10.0.0.2",
			TargetPort: 3000,
			EngineName: "engine-b",
			Nqn:        getStableVolumeNQN("vol-a"),
			Nguid:      getStableVolumeNGUID("vol-a"),
			ANAState:   NvmeTCPANAStateNonOptimized,
		},
	}

	got := ef.Get()
	c.Assert(got.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(got.PreferredPath, Equals, "10.0.0.2:3000")
	c.Assert(len(got.Paths), Equals, 2)
	c.Assert(got.Paths[0].TargetIp, Equals, "10.0.0.1")
	c.Assert(got.Paths[0].AnaState, Equals, string(NvmeTCPANAStateOptimized))
	c.Assert(got.Paths[1].TargetIp, Equals, "10.0.0.2")
	c.Assert(got.Paths[1].AnaState, Equals, string(NvmeTCPANAStateNonOptimized))
}

func (s *TestSuite) TestEngineFrontendSuspendIdempotent(c *C) {
	fmt.Println("Testing EngineFrontend.Suspend is idempotent when already suspended")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateSuspended

	err := ef.Suspend(nil)
	c.Assert(err, IsNil)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateSuspended))
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverLookupByEngineName(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver lookup by engine name")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = "nqn.2014-08.org.nvmexpress:uuid:test-a"
	stubSwitchoverANASync(ef, nil)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.EngineName,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, IsNil)

	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.EngineName, Equals, "engine-b")
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverAmbiguousEngineName(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with ambiguous engine name")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			"ef-a": NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1)),
			"ef-b": NewEngineFrontend("ef-b", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1)),
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          "engine-a",
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverInvalidAddress(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with invalid target address")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          "ef-a",
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.InvalidArgument)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverBlockdevRunning(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver for blockdev frontend while running")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error { return nil }
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error { return nil }
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error { return nil }
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error { return nil }
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverRejectedDuringRestore(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver is rejected during restore")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverRejectedDuringExpand(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver is rejected during expansion")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.isExpanding = true

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		EngineName:    "engine-b",
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetResolveEngineNameFallback(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP NVMe-oF frontend with engine name fallback")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		return "engine-c", nil
	}
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-c")
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetUsesPathMetadataForRemoteANASync(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget resolves remote ANA sync metadata from path records")

	ef := NewEngineFrontend("ef-a", "", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)
	ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.Nguid, NvmeTCPANAStateOptimized)
	ef.ActivePath = "10.0.0.1:2000"
	ef.PreferredPath = "10.0.0.1:2000"

	called := false
	ef.syncRemoteEngineTargetANAStatesFn = func(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
		called = true
		c.Assert(oldTargetIP, Equals, "10.0.0.1")
		c.Assert(oldEngineName, Equals, "engine-a")
		c.Assert(newTargetIP, Equals, "10.0.0.2")
		c.Assert(newEngineName, Equals, "engine-b")
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(called, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevNoOpWithoutSuspend(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend with no-op switchover without suspend")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"

	resolveCalled := false
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		resolveCalled = true
		return "", fmt.Errorf("should not resolve engine name for no-op switchover")
	}

	err := ef.SwitchOverTarget(nil, "", "10.0.0.1:2000", "")
	c.Assert(err, IsNil)
	c.Assert(resolveCalled, Equals, false)

	select {
	case <-updateCh:
		c.Fatal("did not expect update notification for no-op switchover")
	default:
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevConnectFailurePreservesOriginalState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend preserves original state on connect failure")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning

	oldEngineName := "engine-a"
	oldTargetIP := "10.0.0.1"
	oldTargetPort := int32(2000)
	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")
	oldEndpoint := "/dev/longhorn/vol-a"

	ef.EngineName = oldEngineName
	ef.NvmeTcpFrontend.TargetIP = oldTargetIP
	ef.NvmeTcpFrontend.TargetPort = oldTargetPort
	ef.NvmeTcpFrontend.Nqn = oldNQN
	ef.NvmeTcpFrontend.Nguid = oldNGUID
	ef.syncCurrentNVMeTCPPathLocked()
	ef.Endpoint = oldEndpoint
	ef.dmDeviceIsBusy = true
	ef.initiator = &initiator.Initiator{
		Endpoint:    oldEndpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: oldNQN},
	}
	stubSwitchoverANASync(ef, nil)

	var callTargets []string
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		callTargets = append(callTargets, transportAddress+":"+transportServiceID)
		return fmt.Errorf("connect failed")
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "connect failed"), Equals, true)

	c.Assert(len(callTargets), Equals, 1)
	c.Assert(callTargets[0], Equals, "10.0.0.2:3000")

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.EngineName, Equals, oldEngineName)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, oldTargetPort)
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, oldNQN)
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, oldNGUID)
	c.Assert(ef.Endpoint, Equals, oldEndpoint)
	c.Assert(ef.initiator.NVMeTCPInfo, NotNil)
	c.Assert(ef.initiator.NVMeTCPInfo.SubsystemNQN, Equals, oldNQN)
	c.Assert(ef.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateOptimized)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after connect failure")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevANASyncFailurePreservesOriginalState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend preserves original state on ANA sync failure")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning

	oldEngineName := "engine-a"
	oldTargetIP := "10.0.0.1"
	oldTargetPort := int32(2000)
	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")
	oldEndpoint := "/dev/longhorn/vol-a"

	ef.EngineName = oldEngineName
	ef.NvmeTcpFrontend.TargetIP = oldTargetIP
	ef.NvmeTcpFrontend.TargetPort = oldTargetPort
	ef.NvmeTcpFrontend.Nqn = oldNQN
	ef.NvmeTcpFrontend.Nguid = oldNGUID
	ef.syncCurrentNVMeTCPPathLocked()
	ef.Endpoint = oldEndpoint
	ef.dmDeviceIsBusy = true
	ef.initiator = &initiator.Initiator{
		Endpoint:    oldEndpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: oldNQN},
	}
	ef.getInitiatorEndpointFn = func() string { return oldEndpoint }
	// Stub: the pre-connect ANA state setting (setRemoteEngineTargetANAStateFn)
	// must succeed so that the multipath connect proceeds, but the post-connect
	// ANA sync (syncRemoteEngineTargetANAStatesFn) will fail.
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		return nil
	}
	ef.syncRemoteEngineTargetANAStatesFn = func(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
		return fmt.Errorf("ana sync failed")
	}

	var callTargets []string
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		callTargets = append(callTargets, transportAddress+":"+transportServiceID)
		return nil
	}
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "ana sync failed"), Equals, true)
	c.Assert(len(callTargets), Equals, 1)
	c.Assert(callTargets[0], Equals, "10.0.0.2:3000")

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.EngineName, Equals, oldEngineName)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, oldTargetPort)
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, oldNQN)
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, oldNGUID)
	c.Assert(ef.Endpoint, Equals, oldEndpoint)
	c.Assert(ef.initiator.NVMeTCPInfo, NotNil)
	c.Assert(ef.initiator.NVMeTCPInfo.SubsystemNQN, Equals, oldNQN)
	c.Assert(ef.ActivePath, Equals, "10.0.0.1:2000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateOptimized)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after ANA sync failure")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevCreatesInitiatorForMultipathConnect(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend creates initiator before multipath connect")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	stubSwitchoverANASync(ef, nil)

	connected := false
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		connected = true
		if ef.initiator == nil {
			return fmt.Errorf("initiator was not created")
		}
		return nil
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		return nil
	}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(connected, Equals, true)
	c.Assert(ef.initiator, NotNil)
	c.Assert(ef.EngineName, Equals, "engine-b")

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevAlreadyConnectedReloadsInitiatorState(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend reloads initiator state when target path is already connected")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.dmDeviceIsBusy = true
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}

	connectCalled := false
	deviceReloaded := false
	endpointReloaded := false
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		connectCalled = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		return fmt.Errorf("nvme connect target failed: already connected")
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		deviceReloaded = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		c.Assert(subsystemNQN, Equals, getStableVolumeNQN("vol-a"))
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		endpointReloaded = true
		c.Assert(dmDeviceIsBusy, Equals, true)
		return nil
	}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(connectCalled, Equals, true)
	c.Assert(deviceReloaded, Equals, true)
	c.Assert(endpointReloaded, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))
	c.Assert(ef.Endpoint, Equals, "/dev/longhorn/vol-a")
	c.Assert(ef.ActivePath, Equals, "10.0.0.2:3000")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 2)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.2:3000"].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap["10.0.0.1:2000"].ANAState, Equals, NvmeTCPANAStateInaccessible)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev already-connected switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevReloadsInitiatorStateAfterANASync(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend reloads initiator state after ANA sync")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.dmDeviceIsBusy = true
	ef.syncCurrentNVMeTCPPathLocked()
	ef.initiator = &initiator.Initiator{Endpoint: ef.Endpoint, NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	connectCalled := false
	anaSyncCalled := false
	deviceReloaded := false
	endpointReloaded := false
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		connectCalled = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		return nil
	}
	ef.syncRemoteEngineTargetANAStatesFn = func(oldTargetIP, oldEngineName, newTargetIP, newEngineName string) error {
		anaSyncCalled = true
		c.Assert(oldTargetIP, Equals, "10.0.0.1")
		c.Assert(newTargetIP, Equals, "10.0.0.2")
		return nil
	}
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		return nil
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		deviceReloaded = true
		c.Assert(transportAddress, Equals, "10.0.0.2")
		c.Assert(transportServiceID, Equals, "3000")
		c.Assert(subsystemNQN, Equals, getStableVolumeNQN("vol-a"))
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		endpointReloaded = true
		c.Assert(dmDeviceIsBusy, Equals, true)
		return nil
	}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, IsNil)
	c.Assert(connectCalled, Equals, true)
	c.Assert(anaSyncCalled, Equals, true)
	c.Assert(deviceReloaded, Equals, true)
	c.Assert(endpointReloaded, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after blockdev switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevInProgressGuard(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget in-progress guard for SPDK TCP Blockdev frontend")

	updateCh := make(chan interface{}, 2)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateSuspended
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		return nil
	}
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		enteredCh <- struct{}{}
		<-releaseCh
		return nil
	}

	firstErrCh := make(chan error, 1)
	go func() {
		firstErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	}()

	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for first switchover to enter phase-2")
	}

	// While first switchover is in phase-2, read operations should remain responsive.
	getDone := make(chan struct{}, 1)
	go func() {
		_ = ef.Get()
		getDone <- struct{}{}
	}()
	select {
	case <-getDone:
	case <-time.After(1 * time.Second):
		c.Fatal("Get() blocked while switchover is in progress")
	}

	// Concurrent switchover should be rejected by the in-progress guard.
	err := ef.SwitchOverTarget(nil, "engine-c", "10.0.0.3:3000", "")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "already in progress"), Equals, true)

	close(releaseCh)
	select {
	case err := <-firstErrCh:
		c.Assert(err, IsNil)
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for first switchover to complete")
	}
}

func (s *TestSuite) TestEngineFrontendDeleteRejectedDuringSwitchOver(c *C) {
	fmt.Println("Testing EngineFrontend.Delete is rejected while switch over is in progress")

	updateCh := make(chan interface{}, 2)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
	stubSwitchoverANASync(ef, nil)
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return nil
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		return nil
	}
	ef.waitForNvmeTCPControllerLiveFn = func(transportAddress string, transportPort int32) error {
		return nil
	}

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.connectNvmeTCPPathFn = func(transportAddress, transportServiceID string) error {
		enteredCh <- struct{}{}
		<-releaseCh
		return nil
	}

	switchErrCh := make(chan error, 1)
	go func() {
		switchErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	}()

	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for switchover to enter phase-2")
	}

	deleteErrCh := make(chan error, 1)
	go func() {
		deleteErrCh <- ef.Delete(nil)
	}()

	select {
	case err := <-deleteErrCh:
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "switching over target"), Equals, true)
	case <-time.After(1 * time.Second):
		c.Fatal("Delete() blocked while switchover is in progress")
	}

	close(releaseCh)
	select {
	case err := <-switchErrCh:
		c.Assert(err, IsNil)
	case <-time.After(2 * time.Second):
		c.Fatal("timeout waiting for switchover to complete")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetRejectedDuringExpand(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget is rejected while expansion is in progress")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.isExpanding = true

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "expansion is in progress"), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetRejectedDuringRestore(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget is rejected while restore is in progress")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "restore is in progress"), Equals, true)
}

func (s *TestSuite) TestServerEngineFrontendSwitchOverEngineNotFound(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver with target engine not found")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		return "", ErrSwitchOverTargetEngineNotFound
	}

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			ef.Name: ef,
		},
	}

	_, err := srv.EngineFrontendSwitchOver(context.Background(), &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:          ef.Name,
		TargetAddress: "10.0.0.2:3000",
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.NotFound)
}

func (s *TestSuite) TestCreateUblkFrontendNilReturnsCorrectErrorField(c *C) {
	fmt.Println("Testing createUblkFrontend with nil UblkFrontend returns error referencing UblkFrontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendUBLK, 1024, 0, 0, make(chan interface{}, 1))
	ef.UblkFrontend = nil // force nil

	err := ef.createUblkFrontend(nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "UblkFrontend"), Equals, true)
	// Ensure it does NOT reference the wrong field
	c.Assert(strings.Contains(err.Error(), "NvmeTcpFrontend"), Equals, false)
}

func (s *TestSuite) TestPromoteNVMeTCPPathLockedDemotesOldActivePath(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))

	oldAddress := ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateOptimized)
	newAddress := ef.upsertNVMeTCPPathLocked("10.0.0.2", 3000, "engine-b", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateNonOptimized)
	ef.ActivePath = oldAddress
	ef.PreferredPath = oldAddress

	changed := ef.promoteNVMeTCPPathLocked(newAddress)
	c.Assert(changed, Equals, true)
	c.Assert(ef.ActivePath, Equals, newAddress)
	c.Assert(ef.PreferredPath, Equals, oldAddress)
	c.Assert(ef.NvmeTCPPathMap[newAddress].ANAState, Equals, NvmeTCPANAStateOptimized)
	c.Assert(ef.NvmeTCPPathMap[oldAddress].ANAState, Equals, NvmeTCPANAStateInaccessible)
}

func (s *TestSuite) TestRemoveNVMeTCPPathLockedUpdatesSelectors(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))

	firstAddress := ef.upsertNVMeTCPPathLocked("10.0.0.1", 2000, "engine-a", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateNonOptimized)
	secondAddress := ef.upsertNVMeTCPPathLocked("10.0.0.2", 3000, "engine-b", getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), NvmeTCPANAStateOptimized)
	ef.ActivePath = secondAddress
	ef.PreferredPath = secondAddress

	ef.removeNVMeTCPPathLocked(secondAddress)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, firstAddress)
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 1)

	ef.removeNVMeTCPPathLocked(firstAddress)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, "")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 0)
}

func (s *TestSuite) TestEngineFrontendDeleteClearsNVMeTCPPathState(c *C) {
	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")
	ef.syncCurrentNVMeTCPPathLocked()

	err := ef.Delete(nil)
	c.Assert(err, IsNil)
	c.Assert(ef.ActivePath, Equals, "")
	c.Assert(ef.PreferredPath, Equals, "")
	c.Assert(len(ef.NvmeTCPPathMap), Equals, 0)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredUblkReturnsTrue(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns true for UBLK frontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendUBLK, 1024, 0, 0, make(chan interface{}, 1))

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, true)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNvmeTcpBlockdevNewEngine(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns true for new NVMe/TCP blockdev engine (port=0)")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, true)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNvmeTcpBlockdevExistingEngine(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns false for existing NVMe/TCP blockdev engine (port!=0)")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend.TargetPort = 3000

	required, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, IsNil)
	c.Assert(required, Equals, false)
}

func (s *TestSuite) TestIsInitiatorCreationRequiredNilNvmeTcpFrontendReturnsError(c *C) {
	fmt.Println("Testing isInitiatorCreationRequired returns error when NvmeTcpFrontend is nil for non-UBLK frontend")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend = nil

	_, err := ef.isInitiatorCreationRequired("10.0.0.1")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid NvmeTcpFrontend"), Equals, true)
}

func (s *TestSuite) TestSyncRemoteEngineTargetANAStatesSubsystemNotFoundIsNonFatal(c *C) {
	fmt.Println("Testing syncRemoteEngineTargetANAStates treats old engine subsystem-not-found as success")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	var calls []string
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		calls = append(calls, engineName+":"+string(anaState))
		if engineName == "engine-a" && anaState == NvmeTCPANAStateInaccessible {
			return fmt.Errorf("failed to set ANA state: Unable to find subsystem with NQN xyz")
		}
		return nil
	}

	err := ef.syncRemoteEngineTargetANAStates("10.0.0.1", "engine-a", "10.0.0.2", "engine-b")
	c.Assert(err, IsNil)
	c.Assert(len(calls), Equals, 3)
	c.Assert(calls[0], Equals, "engine-b:non-optimized")
	c.Assert(calls[1], Equals, "engine-a:inaccessible")
	c.Assert(calls[2], Equals, "engine-b:optimized")
}

func (s *TestSuite) TestSyncRemoteEngineTargetANAStatesOtherErrorStillFails(c *C) {
	fmt.Println("Testing syncRemoteEngineTargetANAStates propagates non-subsystem errors on old engine")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		if engineName == "engine-a" && anaState == NvmeTCPANAStateInaccessible {
			return fmt.Errorf("connection refused")
		}
		return nil
	}

	err := ef.syncRemoteEngineTargetANAStates("10.0.0.1", "engine-a", "10.0.0.2", "engine-b")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "connection refused"), Equals, true)
}

func (s *TestSuite) TestSyncRemoteEngineTargetANAStatesPhase3FailureRollsBack(c *C) {
	fmt.Println("Testing syncRemoteEngineTargetANAStates rolls back ANA states when phase 3 promotion fails")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	var calls []string
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		calls = append(calls, engineName+":"+string(anaState))
		if engineName == "engine-b" && anaState == NvmeTCPANAStateOptimized {
			return fmt.Errorf("phase 3 failed")
		}
		return nil
	}

	err := ef.syncRemoteEngineTargetANAStates("10.0.0.1", "engine-a", "10.0.0.2", "engine-b")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "phase 3 failed"), Equals, true)
	c.Assert(calls, DeepEquals, []string{
		"engine-b:non-optimized",
		"engine-a:inaccessible",
		"engine-b:optimized",
		"engine-b:inaccessible",
		"engine-a:optimized",
	})
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetNvmfPromotingFailureRollsBackANAStates(c *C) {
	fmt.Println("Testing switchOverTargetNvmfPhased promoting rolls back ANA states on promotion failure")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))

	updateRequired := false
	var calls []string
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		calls = append(calls, engineName+":"+string(anaState))
		if engineName == "engine-b" && anaState == NvmeTCPANAStateOptimized {
			return fmt.Errorf("promoting failed")
		}
		return nil
	}

	err := ef.switchOverTargetNvmfPhased(SwitchoverPhasePromoting, "engine-b", "10.0.0.2", 3000, "engine-a", "10.0.0.1", 2000,
		getStableVolumeNQN("vol-a"), getStableVolumeNGUID("vol-a"), &updateRequired)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "promoting failed"), Equals, true)
	c.Assert(calls, DeepEquals, []string{
		"engine-b:optimized",
		"engine-b:inaccessible",
		"engine-a:optimized",
	})
	c.Assert(updateRequired, Equals, false)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevPromotingStep2FailureRollsBackANAStates(c *C) {
	fmt.Println("Testing switchOverTargetBlockdevPhased promoting rolls back ANA states when Step 2 (loadInitiatorNVMeDeviceInfo) fails")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	// Populate old path so resolveRemoteEngineName succeeds.
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		EngineName: "engine-a",
	}

	updateRequired := false
	var calls []string
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		tag := engineName + ":" + string(anaState)
		calls = append(calls, tag)
		// Make the rollback new→inaccessible fail to verify error aggregation.
		if engineName == "engine-b" && anaState == NvmeTCPANAStateInaccessible {
			return fmt.Errorf("rollback new failed")
		}
		return nil
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return fmt.Errorf("reload device failed")
	}

	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")

	err := ef.switchOverTargetBlockdevPhased(SwitchoverPhasePromoting, "engine-b", "10.0.0.2:3000",
		"10.0.0.2", 3000, "engine-a", "10.0.0.1", 2000,
		oldNQN, oldNGUID, oldNQN, oldNGUID, "/dev/longhorn/vol-a", true, &updateRequired)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "reload device failed"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "rollback new failed"), Equals, true)
	// Step 1 promotes new, then rollback: new→inaccessible (fails), old→optimized.
	c.Assert(calls, DeepEquals, []string{
		"engine-b:optimized",
		"engine-b:inaccessible",
		"engine-a:optimized",
	})
	c.Assert(updateRequired, Equals, true)
	// Local state must be rolled back to old values.
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.1")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(2000))
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevPromotingStep3FailureRollsBackANAStates(c *C) {
	fmt.Println("Testing switchOverTargetBlockdevPhased promoting rolls back ANA states when Step 3 (loadInitiatorEndpoint) fails")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTCPPathMap["10.0.0.1:2000"] = &NvmeTCPPath{
		TargetIP:   "10.0.0.1",
		TargetPort: 2000,
		EngineName: "engine-a",
	}

	updateRequired := false
	var calls []string
	ef.setRemoteEngineTargetANAStateFn = func(targetIP, engineName string, anaState NvmeTCPANAState) error {
		tag := engineName + ":" + string(anaState)
		calls = append(calls, tag)
		// Make the rollback old→optimized fail to verify error aggregation.
		if engineName == "engine-a" && anaState == NvmeTCPANAStateOptimized {
			return fmt.Errorf("rollback old failed")
		}
		return nil
	}
	ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error {
		return nil // Step 2 succeeds
	}
	ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error {
		return fmt.Errorf("endpoint load failed")
	}

	oldNQN := getStableVolumeNQN("vol-a")
	oldNGUID := getStableVolumeNGUID("vol-a")

	err := ef.switchOverTargetBlockdevPhased(SwitchoverPhasePromoting, "engine-b", "10.0.0.2:3000",
		"10.0.0.2", 3000, "engine-a", "10.0.0.1", 2000,
		oldNQN, oldNGUID, oldNQN, oldNGUID, "/dev/longhorn/vol-a", true, &updateRequired)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "endpoint load failed"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "rollback old failed"), Equals, true)
	// Step 1 promotes new, Step 2 reloads device OK, Step 3 fails → rollback: new→inaccessible, old→optimized (fails).
	c.Assert(calls, DeepEquals, []string{
		"engine-b:optimized",
		"engine-b:inaccessible",
		"engine-a:optimized",
	})
	c.Assert(updateRequired, Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.1")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(2000))
}

func (s *TestSuite) TestIsSubsystemNotFoundError(c *C) {
	fmt.Println("Testing isSubsystemNotFoundError")

	c.Assert(isSubsystemNotFoundError(nil), Equals, false)
	c.Assert(isSubsystemNotFoundError(fmt.Errorf("Unable to find subsystem with NQN xyz")), Equals, true)
	c.Assert(isSubsystemNotFoundError(fmt.Errorf("something else: unable to find subsystem with NQN abc")), Equals, true)
	c.Assert(isSubsystemNotFoundError(fmt.Errorf("connection refused")), Equals, false)
}
