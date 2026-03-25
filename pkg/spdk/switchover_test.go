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

	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestEngineFrontendSwitchOverTargetNvmfSuccess(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP NVMe-oF frontend with successful switchover")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)

	ef.State = lhtypes.InstanceStateRunning
	ef.NvmeTcpFrontend.Nqn = "nqn.2014-08.org.nvmexpress:uuid:test-a"
	ef.NvmeTcpFrontend.Nguid = "old-nguid"
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.Endpoint = GetNvmfEndpoint(ef.NvmeTcpFrontend.Nqn, ef.NvmeTcpFrontend.TargetIP, ef.NvmeTcpFrontend.TargetPort)

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-b")
	c.Assert(ef.EngineIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "10.0.0.2")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(3000))

	expectedNQN := helpertypes.GetNQN("engine-b")
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, expectedNQN)

	expectedEndpoint := GetNvmfEndpoint(expectedNQN, "10.0.0.2", 3000)
	c.Assert(ef.Endpoint, Equals, expectedEndpoint)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after target switchover")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevRequiresSuspended(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend requires suspended state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "must be suspended"), Equals, true)
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

func (s *TestSuite) TestServerEngineFrontendSwitchOverBlockdevRequiresSuspended(c *C) {
	fmt.Println("Testing Server.EngineFrontendSwitchOver for blockdev frontend requires suspended state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning

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

	err := ef.SwitchOverTarget(nil, "", "10.0.0.2:3000")
	c.Assert(err, IsNil)
	c.Assert(ef.EngineName, Equals, "engine-c")
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevNoOpWithoutSuspend(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend with no-op switchover without suspend")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = helpertypes.GetNQN("engine-a")
	ef.NvmeTcpFrontend.Nguid = generateNGUID("engine-a")
	ef.Endpoint = "/dev/longhorn/vol-a"

	resolveCalled := false
	ef.resolveEngineNameByTargetAddressFn = func(targetAddress string) (string, error) {
		resolveCalled = true
		return "", fmt.Errorf("should not resolve engine name for no-op switchover")
	}

	err := ef.SwitchOverTarget(nil, "", "10.0.0.1:2000")
	c.Assert(err, IsNil)
	c.Assert(resolveCalled, Equals, false)

	select {
	case <-updateCh:
		c.Fatal("did not expect update notification for no-op switchover")
	default:
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevRollbackSuccess(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend with successful rollback")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateSuspended

	oldEngineName := "engine-a"
	oldEngineIP := "10.0.0.1"
	oldTargetIP := "10.0.0.1"
	oldTargetPort := int32(2000)
	oldNQN := helpertypes.GetNQN(oldEngineName)
	oldNGUID := generateNGUID(oldEngineName)
	oldEndpoint := "/dev/longhorn/vol-a"

	ef.EngineName = oldEngineName
	ef.EngineIP = oldEngineIP
	ef.NvmeTcpFrontend.TargetIP = oldTargetIP
	ef.NvmeTcpFrontend.TargetPort = oldTargetPort
	ef.NvmeTcpFrontend.Nqn = oldNQN
	ef.NvmeTcpFrontend.Nguid = oldNGUID
	ef.Endpoint = oldEndpoint
	ef.dmDeviceIsBusy = true
	ef.initiator = &initiator.Initiator{
		Endpoint:    oldEndpoint,
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: oldNQN},
	}

	var callTargets []string
	ef.startNvmeTCPInitiatorFn = func(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error) {
		callTargets = append(callTargets, transportAddress+":"+transportServiceID)
		if len(callTargets) == 1 {
			return false, fmt.Errorf("switch failed")
		}
		return true, nil
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "switch failed"), Equals, true)

	c.Assert(len(callTargets), Equals, 2)
	c.Assert(callTargets[0], Equals, "10.0.0.2:3000")
	c.Assert(callTargets[1], Equals, "10.0.0.1:2000")

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateSuspended))
	c.Assert(ef.EngineName, Equals, oldEngineName)
	c.Assert(ef.EngineIP, Equals, oldEngineIP)
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, oldTargetIP)
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, oldTargetPort)
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, oldNQN)
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, oldNGUID)
	c.Assert(ef.Endpoint, Equals, oldEndpoint)
	c.Assert(ef.initiator.NVMeTCPInfo, NotNil)
	c.Assert(ef.initiator.NVMeTCPInfo.SubsystemNQN, Equals, oldNQN)

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after rollback")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevRollbackFailure(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget for SPDK TCP Blockdev frontend with failed rollback")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateSuspended
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = helpertypes.GetNQN("engine-a")
	ef.NvmeTcpFrontend.Nguid = generateNGUID("engine-a")
	ef.initiator = &initiator.Initiator{
		NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn},
	}

	callCount := 0
	ef.startNvmeTCPInitiatorFn = func(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error) {
		callCount++
		if callCount == 1 {
			return false, fmt.Errorf("switch failed")
		}
		return false, fmt.Errorf("rollback failed")
	}

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "switch failed"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "rollback failed"), Equals, true)
	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))

	select {
	case <-updateCh:
	default:
		c.Fatal("expected update notification after rollback failure")
	}
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetBlockdevInProgressGuard(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget in-progress guard for SPDK TCP Blockdev frontend")

	updateCh := make(chan interface{}, 2)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh)
	ef.State = lhtypes.InstanceStateSuspended
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = helpertypes.GetNQN("engine-a")
	ef.NvmeTcpFrontend.Nguid = generateNGUID("engine-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.startNvmeTCPInitiatorFn = func(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error) {
		enteredCh <- struct{}{}
		<-releaseCh
		return false, nil
	}

	firstErrCh := make(chan error, 1)
	go func() {
		firstErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
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
	err := ef.SwitchOverTarget(nil, "engine-c", "10.0.0.3:3000")
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
	ef.State = lhtypes.InstanceStateSuspended
	ef.EngineIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = helpertypes.GetNQN("engine-a")
	ef.NvmeTcpFrontend.Nguid = generateNGUID("engine-a")
	ef.initiator = &initiator.Initiator{NVMeTCPInfo: &initiator.NVMeTCPInfo{SubsystemNQN: ef.NvmeTcpFrontend.Nqn}}
	ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }

	enteredCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	ef.startNvmeTCPInitiatorFn = func(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error) {
		enteredCh <- struct{}{}
		<-releaseCh
		return false, nil
	}

	switchErrCh := make(chan error, 1)
	go func() {
		switchErrCh <- ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
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

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "expansion is in progress"), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSwitchOverTargetRejectedDuringRestore(c *C) {
	fmt.Println("Testing EngineFrontend.SwitchOverTarget is rejected while restore is in progress")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000")
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
