package spdk

import (
	"context"
	"errors"
	"fmt"

	cockroacherrors "github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkjsonrpc "github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBuildBdevLvolMap(c *C) {
	fmt.Println("Testing buildBdevLvolMap with valid lvol, invalid lvol with extra alias, and non-lvol bdev")

	lvolValid := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-valid",
			Aliases:     []string{"lvs-a/replica-a"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{},
		},
	}
	lvolInvalidAlias := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-invalid-alias",
			Aliases:     []string{"lvs-a/replica-b", "extra"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{},
		},
	}
	raid := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "raid-a",
			ProductName: spdktypes.BdevProductNameRaid,
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Raid: &spdktypes.BdevRaidInfo{},
		},
	}

	m := buildBdevLvolMap([]spdktypes.BdevInfo{lvolValid, lvolInvalidAlias, raid})
	c.Assert(len(m), Equals, 1)
	c.Assert(m["replica-a"], NotNil)
	c.Assert(m["replica-a"].Name, Equals, "lvol-valid")
}

func (s *TestSuite) TestBuildBdevLvolMapIgnoresInvalidDriverSpecific(c *C) {
	fmt.Println("Testing buildBdevLvolMap ignores invalid driver specific")

	lvolMissingDriverSpecific := spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Name:        "lvol-invalid",
			Aliases:     []string{"lvs-a/replica-a"},
			ProductName: spdktypes.BdevProductNameLvol,
		},
		DriverSpecific: nil,
	}

	m := buildBdevLvolMap([]spdktypes.BdevInfo{lvolMissingDriverSpecific})
	c.Assert(len(m), Equals, 0)
}

func (s *TestSuite) TestBuildLvsUUIDNameMap(c *C) {
	fmt.Println("Testing buildLvsUUIDNameMap with valid lvs list")

	lvsList := []spdktypes.LvstoreInfo{
		{UUID: "uuid-a", Name: "disk-a"},
		{UUID: "uuid-b", Name: "disk-b"},
	}

	m := buildLvsUUIDNameMap(lvsList)
	c.Assert(len(m), Equals, 2)
	c.Assert(m["uuid-a"], Equals, "disk-a")
	c.Assert(m["uuid-b"], Equals, "disk-b")
}

func (s *TestSuite) TestHandleVerifyErrorBrokenPipe(c *C) {
	fmt.Println("Testing handleVerifyError with broken pipe error")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	engine := NewEngine("e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1))
	engineFrontend := NewEngineFrontend("ef1", "e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))

	replica.State = lhtypes.InstanceStateRunning
	engine.State = lhtypes.InstanceStateRunning
	engineFrontend.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync: map[string]*Engine{
			"e1": engine,
		},
		engineFrontendForSync: map[string]*EngineFrontend{
			"ef1": engineFrontend,
		},
	}

	brokenPipeErr := spdkjsonrpc.JSONClientError{
		ID:          1,
		Method:      "mock",
		ErrorDetail: errors.New("write: broken pipe"),
	}
	server := &Server{}
	server.handleVerifyError(brokenPipeErr, state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engine.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engineFrontend.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestHandleVerifyErrorNonBrokenPipeNoStateChange(c *C) {
	fmt.Println("Testing handleVerifyError with non-broken pipe error does not change state")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replica.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
	}

	server := &Server{}
	server.handleVerifyError(errors.New("any other error"), state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestHandleVerifyErrorNoopForNilError(c *C) {
	fmt.Println("Testing handleVerifyError with nil error does not change state")

	replica := NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replica.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r1": replica,
		},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
	}

	server := &Server{}
	server.handleVerifyError(nil, state)

	c.Assert(replica.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestHandleVerifyErrorBrokenPipeKeepsStoppedAndError(c *C) {
	fmt.Println("Testing handleVerifyError with broken pipe error keeps stopped and error states")

	replicaStopped := NewReplica(context.Background(), "r-stopped", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1))
	replicaStopped.State = lhtypes.InstanceStateStopped

	engineErrored := NewEngine("e-err", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1))
	engineErrored.State = lhtypes.InstanceStateError

	engineFrontendRunning := NewEngineFrontend("ef-run", "e-err", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	engineFrontendRunning.State = lhtypes.InstanceStateRunning

	state := &verifyState{
		replicaMapForSync: map[string]*Replica{
			"r-stopped": replicaStopped,
		},
		engineMapForSync: map[string]*Engine{
			"e-err": engineErrored,
		},
		engineFrontendForSync: map[string]*EngineFrontend{
			"ef-run": engineFrontendRunning,
		},
	}

	brokenPipeErr := spdkjsonrpc.JSONClientError{
		ID:          1,
		Method:      "mock",
		ErrorDetail: errors.New("write: broken pipe"),
	}
	server := &Server{}
	server.handleVerifyError(brokenPipeErr, state)

	c.Assert(replicaStopped.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateStopped))
	c.Assert(engineErrored.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(engineFrontendRunning.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestNewVerifyStateLockedCopiesMaps(c *C) {
	fmt.Println("Testing newVerifyState creates copies of maps while locked")

	server := &Server{
		replicaMap: map[string]*Replica{
			"r1": NewReplica(context.Background(), "r1", "disk-a", "uuid-a", 1024, true, make(chan interface{}, 1)),
		},
		engineMap: map[string]*Engine{
			"e1": NewEngine("e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, make(chan interface{}, 1)),
		},
		engineFrontendMap: map[string]*EngineFrontend{
			"ef1": NewEngineFrontend("ef1", "e1", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1)),
		},
		backingImageMap: map[string]*BackingImage{
			"bi1": NewBackingImage(context.Background(), "bi1", "uuid-bi1", "disk-uuid", 1024, "checksum", make(chan interface{}, 1)),
		},
		spdkClient: nil,
	}

	server.Lock()
	state := server.newVerifyState()
	server.Unlock()

	c.Assert(len(state.replicaMap), Equals, 1)
	c.Assert(len(state.replicaMapForSync), Equals, 1)
	c.Assert(len(state.engineMapForSync), Equals, 1)
	c.Assert(len(state.engineFrontendForSync), Equals, 1)
	c.Assert(len(state.backingImageMap), Equals, 1)
	c.Assert(len(state.backingImageForSync), Equals, 1)
	c.Assert(state.spdkClient, IsNil)

	_, ok := state.replicaMap["r1"]
	c.Assert(ok, Equals, true)
	_, ok = state.engineMapForSync["e1"]
	c.Assert(ok, Equals, true)
	_, ok = state.engineFrontendForSync["ef1"]
	c.Assert(ok, Equals, true)
	_, ok = state.backingImageMap["bi1"]
	c.Assert(ok, Equals, true)
}

func (s *TestSuite) TestSyncVerifiedObjectsWithEmptyState(c *C) {
	fmt.Println("Testing syncVerifiedObjects with empty state")

	server := &Server{}
	state := &verifyState{
		replicaMapForSync:     map[string]*Replica{},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
		backingImageForSync:   map[string]*BackingImage{},
		spdkClient:            nil,
	}

	err := server.syncVerifiedObjects(state)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestEngineFrontendCreateRegistersNewFrontend(c *C) {
	fmt.Println("Testing EngineFrontendCreate registers a new frontend in the map")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngineFrontend: make(chan interface{}, 1),
		},
	}

	_, err := srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-test",
		EngineName: "engine-a",
		VolumeName: "vol-a",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   1024,
	})
	c.Assert(err, IsNil)

	srv.RLock()
	ef, ok := srv.engineFrontendMap["ef-test"]
	srv.RUnlock()

	c.Assert(ok, Equals, true)
	c.Assert(ef, NotNil)
	c.Assert(ef.Name, Equals, "ef-test")
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.VolumeName, Equals, "vol-a")
}

func (s *TestSuite) TestEngineFrontendCreateReturnsAlreadyExistsForDuplicate(c *C) {
	fmt.Println("Testing EngineFrontendCreate returns AlreadyExists for duplicate name")

	updateCh := make(chan interface{}, 1)

	existing := NewEngineFrontend("ef-dup", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPNvmf, 1024, 0, 0, updateCh)

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{
			"ef-dup": existing,
		},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngineFrontend: updateCh,
		},
	}

	_, err := srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-dup",
		EngineName: "engine-b",
		VolumeName: "vol-b",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   2048,
	})
	c.Assert(err, NotNil)

	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.AlreadyExists)

	// Original frontend should be untouched
	srv.RLock()
	ef := srv.engineFrontendMap["ef-dup"]
	srv.RUnlock()
	c.Assert(ef.EngineName, Equals, "engine-a")
}

func (s *TestSuite) TestEngineFrontendCreateDoesNotRegisterFailedFrontend(c *C) {
	fmt.Println("Testing EngineFrontendCreate does not leave a stale frontend after target address validation fails")

	srv := &Server{
		engineFrontendMap: map[string]*EngineFrontend{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngineFrontend: make(chan interface{}, 1),
		},
	}

	// "2001:db8::1:9502" is intentionally an un-bracketed IPv6 with
	// port, which net.SplitHostPort cannot parse. This triggers a hard
	// error from Create(), verifying that the failed frontend is NOT
	// registered in the map.
	_, err := srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:          "ef-test",
		EngineName:    "engine-a",
		VolumeName:    "vol-a",
		Frontend:      lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:      1024,
		TargetAddress: "2001:db8::1:9502",
	})
	c.Assert(err, NotNil)
	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.InvalidArgument)

	srv.RLock()
	_, exists := srv.engineFrontendMap["ef-test"]
	srv.RUnlock()
	c.Assert(exists, Equals, false)

	// Retry with empty TargetAddress. splitHostPort("") returns ("", 0, nil)
	// so Create() proceeds without initiator work and succeeds, proving
	// the name is no longer blocked by the earlier failure.
	_, err = srv.EngineFrontendCreate(context.Background(), &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-test",
		EngineName: "engine-a",
		VolumeName: "vol-a",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   1024,
	})
	c.Assert(err, IsNil)

	srv.RLock()
	ef, exists := srv.engineFrontendMap["ef-test"]
	srv.RUnlock()
	c.Assert(exists, Equals, true)
	c.Assert(ef, NotNil)
}

func (s *TestSuite) TestToEngineFrontendCreateGRPCErrorMapsKnownErrors(c *C) {
	fmt.Println("Testing toEngineFrontendCreateGRPCError maps known create failures to stable gRPC codes")

	testCases := []struct {
		name         string
		err          error
		expectedCode grpccodes.Code
	}{
		{
			name:         "invalid argument",
			err:          cockroacherrors.Wrap(ErrEngineFrontendCreateInvalidArgument, "bad target"),
			expectedCode: grpccodes.InvalidArgument,
		},
		{
			name:         "failed precondition",
			err:          cockroacherrors.Wrap(ErrEngineFrontendCreatePrecondition, "invalid state"),
			expectedCode: grpccodes.FailedPrecondition,
		},
		{
			name:         "existing grpc status preserved",
			err:          grpcstatus.Error(grpccodes.Unavailable, "transient"),
			expectedCode: grpccodes.Unavailable,
		},
	}

	for _, tc := range testCases {
		grpcErr := toEngineFrontendCreateGRPCError(tc.err, "failed to create engine frontend %v", "ef-test")
		st, ok := grpcstatus.FromError(grpcErr)
		c.Assert(ok, Equals, true, Commentf("case=%s", tc.name))
		c.Assert(st.Code(), Equals, tc.expectedCode, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestEngineFrontendLifecycleRPCsMapKnownErrors(c *C) {
	fmt.Println("Testing EngineFrontend lifecycle RPCs map precondition and unimplemented errors consistently")

	newServer := func(ef *EngineFrontend) *Server {
		return &Server{
			engineFrontendMap: map[string]*EngineFrontend{
				"ef-test": ef,
			},
		}
	}

	suspendPrecondition := NewEngineFrontend("ef-test", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	suspendPrecondition.State = lhtypes.InstanceStateRunning
	suspendPrecondition.isSwitchingOver = true

	_, err := newServer(suspendPrecondition).EngineFrontendSuspend(context.Background(), &spdkrpc.EngineFrontendSuspendRequest{Name: "ef-test"})
	st, ok := grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)

	resumeUnimplemented := NewEngineFrontend("ef-test", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1024, 0, 0, make(chan interface{}, 1))
	resumeUnimplemented.State = lhtypes.InstanceStateSuspended

	_, err = newServer(resumeUnimplemented).EngineFrontendResume(context.Background(), &spdkrpc.EngineFrontendResumeRequest{Name: "ef-test"})
	st, ok = grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.Unimplemented)

	deletePrecondition := NewEngineFrontend("ef-test", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	deletePrecondition.isSwitchingOver = true

	_, err = newServer(deletePrecondition).EngineFrontendDelete(context.Background(), &spdkrpc.EngineFrontendDeleteRequest{Name: "ef-test"})
	st, ok = grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)

	// Delete while isCreating should also return FailedPrecondition
	deleteWhileCreating := NewEngineFrontend("ef-test", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	deleteWhileCreating.isCreating = true

	_, err = newServer(deleteWhileCreating).EngineFrontendDelete(context.Background(), &spdkrpc.EngineFrontendDeleteRequest{Name: "ef-test"})
	st, ok = grpcstatus.FromError(err)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.FailedPrecondition)
}
