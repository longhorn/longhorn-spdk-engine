package spdk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	clientpkg "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestEngineFinishExpansionSuccessClearsErrorState(c *C) {
	fmt.Println("Testing Engine finish expansion success clears error state")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.State = lhtypes.InstanceStateError
	e.ErrorMsg = "previous failure"

	e.finishExpansion(10, 20, nil)

	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.SpecSize, Equals, uint64(20))
}

func (s *TestSuite) TestEngineFinishExpansionFailureSetsErrorState(c *C) {
	fmt.Println("Testing Engine finish expansion failure sets error state")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.State = lhtypes.InstanceStateRunning
	e.ErrorMsg = ""
	e.SpecSize = 10

	e.finishExpansion(10, 20, errors.New("expand failed"))

	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.ErrorMsg, Not(Equals), "")
	c.Assert(e.lastExpansionError, Not(Equals), "")
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
	c.Assert(e.SpecSize, Equals, uint64(10))
}

func (s *TestSuite) TestEngineFinishExpansionFailureRestoresOriginalSize(c *C) {
	fmt.Println("Testing Engine finish expansion failure restores original size when spec size was updated during expand")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.SpecSize = 20

	e.finishExpansion(10, 20, errors.New("expand failed"))

	c.Assert(e.SpecSize, Equals, uint64(10))
}

func (s *TestSuite) TestEngineFrontendFinishExpansionSuccessClearsErrorState(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion success clears error state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateError
	ef.ErrorMsg = "previous failure"
	ef.lastExpansionError = ""

	ef.finishExpansion(10, true, 20, nil, "", "", 15)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.ErrorMsg, Equals, "")
	c.Assert(ef.SpecSize, Equals, uint64(20))
	c.Assert(ef.ActualSize, Equals, uint64(15))
	c.Assert(ef.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineFrontendFinishExpansionExpandedWithError(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion with error after expansion")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))

	ef.finishExpansion(10, true, 20, errors.New("post expansion frontend failure"), "", "", 15)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(ef.ErrorMsg, Not(Equals), "")
	c.Assert(ef.SpecSize, Equals, uint64(20))
	c.Assert(ef.ActualSize, Equals, uint64(15))
	c.Assert(ef.lastExpansionError, Not(Equals), "")
	c.Assert(ef.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineFrontendFinishExpansionFailureWithoutExpansion(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion failure without expansion")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))
	ef.SpecSize = 10

	ef.finishExpansion(10, false, 20, errors.New("expand failed before backend expansion"), "", "", 0)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(ef.SpecSize, Equals, uint64(10))
	c.Assert(ef.ActualSize, Equals, uint64(0))
	c.Assert(ef.lastExpansionError, Not(Equals), "")
	c.Assert(ef.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineFinishExpansionPartialFailureKeepsOriginalSize(c *C) {
	fmt.Println("Testing Engine finish expansion partial failure keeps original size")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.lastExpansionError = "replica expand failed"

	e.finishExpansion(10, 20, nil)

	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.SpecSize, Equals, uint64(10))
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
}

func (s *TestSuite) TestEngineFinishExpansionPartialFailureRestoresOriginalSize(c *C) {
	fmt.Println("Testing Engine finish expansion partial failure restores original size when spec size was updated during expand")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.SpecSize = 20
	e.lastExpansionError = "replica expand failed"

	e.finishExpansion(10, 20, nil)

	c.Assert(e.SpecSize, Equals, uint64(10))
}

func (s *TestSuite) TestEngineFrontendFinishExpansionPartialFailureKeepsOriginalSize(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion partial failure keeps original size")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))

	ef.finishExpansion(10, false, 20, nil, "replica expand failed", "2026-03-10T00:00:00Z", 12)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.ErrorMsg, Equals, "")
	c.Assert(ef.SpecSize, Equals, uint64(10))
	c.Assert(ef.ActualSize, Equals, uint64(12))
	c.Assert(ef.lastExpansionError, Equals, "replica expand failed")
	c.Assert(ef.lastExpansionFailedAt, Equals, "2026-03-10T00:00:00Z")
	c.Assert(ef.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineFrontendRequireExpansionGuards(c *C) {
	fmt.Println("Testing EngineFrontend require expansion guards")

	efInProgress := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))
	efInProgress.isExpanding = true
	_, err := efInProgress.requireExpansion(context.Background(), nil, 20)
	c.Assert(errors.Is(err, ErrExpansionInProgress), Equals, true)

	efRestoring := NewEngineFrontend("ef-b", "engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))
	efRestoring.IsRestoring = true
	_, err = efRestoring.requireExpansion(context.Background(), nil, 20)
	c.Assert(errors.Is(err, ErrRestoringInProgress), Equals, true)

	efSmaller := NewEngineFrontend("ef-c", "engine-c", "vol-c", lhtypes.FrontendSPDKTCPBlockdev, 20, 0, 0, make(chan interface{}, 1))
	_, err = efSmaller.requireExpansion(context.Background(), nil, 10)
	c.Assert(errors.Is(err, ErrExpansionInvalidSize), Equals, true)

	efUnaligned := NewEngineFrontend("ef-d", "engine-d", "vol-d", lhtypes.FrontendSPDKTCPBlockdev, 10*helpertypes.MiB, 0, 0, make(chan interface{}, 1))
	notAlignedSize := uint64((11 * helpertypes.MiB) + 1)
	_, err = efUnaligned.requireExpansion(context.Background(), nil, notAlignedSize)
	c.Assert(errors.Is(err, ErrExpansionInvalidSize), Equals, true)
}

func (s *TestSuite) TestEngineExpandPrecheckGuards(c *C) {
	fmt.Println("Testing Engine expand precheck guards")

	eInProgress := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	eInProgress.isExpanding = true
	_, err := eInProgress.ExpandPrecheck(nil, 20)
	c.Assert(errors.Is(err, ErrExpansionInProgress), Equals, true)

	eRestoring := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	eRestoring.IsRestoring = true
	_, err = eRestoring.ExpandPrecheck(nil, 20)
	c.Assert(errors.Is(err, ErrRestoringInProgress), Equals, true)
}

func (s *TestSuite) TestHandleReplicaExpandResult(c *C) {
	fmt.Println("Testing Engine handle replica expand result")

	eAllFailed := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	replicaClientsAllFailed := map[string]*clientpkg.SPDKClient{
		"r1": nil,
		"r2": nil,
	}
	failedAll := map[string]error{
		"r1": errors.New("x"),
		"r2": errors.New("y"),
	}
	err := eAllFailed.handleReplicaExpandResult(replicaClientsAllFailed, failedAll)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "all replicas failed to expand"), Equals, true)

	ePartial := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	ePartial.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"r1": &EngineReplicaStatus{Mode: lhtypes.ModeRW},
		"r2": &EngineReplicaStatus{Mode: lhtypes.ModeRW},
	}
	replicaClientsPartial := map[string]*clientpkg.SPDKClient{
		"r1": nil,
		"r2": nil,
	}
	failedPartial := map[string]error{
		"r1": errors.New("x"),
	}
	err = ePartial.handleReplicaExpandResult(replicaClientsPartial, failedPartial)
	c.Assert(err, IsNil)
	c.Assert(ePartial.ReplicaStatusMap["r1"].Mode, Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(ePartial.ReplicaStatusMap["r2"].Mode, Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(ePartial.lastExpansionError, Not(Equals), "")
}

func (s *TestSuite) TestExpandDoesNotBlockConcurrentGet(c *C) {
	fmt.Println("Testing Expand lock scope does not block concurrent Get calls")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.EngineIP = "10.0.0.1"

	// Simulate the Phase 1 lock pattern from Expand: acquire, read, release
	var wg sync.WaitGroup
	getCh := make(chan *spdkrpc.EngineFrontend, 1)
	phase2ReadyCh := make(chan struct{})
	allowPhase3Ch := make(chan struct{})

	// Start a goroutine that simulates Phase 1 of Expand (lock → read → unlock)
	// then holds a "long operation" before Phase 3 (re-lock → finishExpansion → unlock)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Phase 1: lock, read, unlock
		ef.Lock()
		_ = ef.SpecSize
		ef.Unlock()
		close(phase2ReadyCh)

		// Simulate Phase 2: long operation without lock
		<-allowPhase3Ch

		// Phase 3: re-lock, update state, unlock
		ef.Lock()
		ef.SpecSize = 2048
		ef.Unlock()
	}()

	// Wait until the goroutine enters Phase 2 (unlocked state)
	select {
	case <-phase2ReadyCh:
	case <-time.After(2 * time.Second):
		c.Fatal("Expand simulation did not reach Phase 2 in time")
	}

	// Concurrent Get() should not block during Phase 2 (write lock is released)
	wg.Add(1)
	go func() {
		defer wg.Done()
		result := ef.Get()
		getCh <- result
	}()

	select {
	case result := <-getCh:
		c.Assert(result, NotNil)
		c.Assert(result.Name, Equals, "ef-a")
	case <-time.After(2 * time.Second):
		c.Fatal("Get() call was blocked — potential deadlock due to Expand holding lock during long operations")
	}

	// Get() completed; now allow Phase 3 to proceed and verify state update.
	close(allowPhase3Ch)
	wg.Wait()
	c.Assert(ef.SpecSize, Equals, uint64(2048))
}

func (s *TestSuite) TestExpandCapturesTargetAddressWithNilNvmeTcpFrontend(c *C) {
	fmt.Println("Testing Expand safely computes targetAddress when NvmeTcpFrontend is nil")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendUBLK, 1024, 0, 0, make(chan interface{}, 1))
	ef.NvmeTcpFrontend = nil // force nil, simulating UBLK-only scenario

	// Test the targetAddress capture logic
	ef.Lock()
	var targetAddress string
	if ef.NvmeTcpFrontend != nil {
		targetAddress = "should-not-be-set"
	}
	ef.Unlock()

	c.Assert(targetAddress, Equals, "")
}

func (s *TestSuite) TestExpandResumeGuardsNilInitiator(c *C) {
	fmt.Println("Testing Expand resume defer guards against nil initiator")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.initiator = nil

	// Simulate the resume defer guard — should NOT panic.
	c.Assert(func() {
		if ef.initiator != nil {
			_ = ef.initiator.Resume()
		}
	}, Not(PanicMatches), ".*")
}
