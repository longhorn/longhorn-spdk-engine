package spdk

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestGetTargetCntlidRange(c *C) {
	const ip = "10.42.1.21"

	// Without shared block the target keeps accepting a single host.
	for _, port := range []int32{1, 20006, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, false)
		c.Assert(minCntlid, Equals, getTargetCntlid(ip, port))
		c.Assert(maxCntlid, Equals, minCntlid)
	}

	for _, port := range []int32{1, 20006, 20011, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, true)
		c.Assert(minCntlid >= 1, Equals, true)
		c.Assert(maxCntlid <= maxValidCntlid, Equals, true)
		c.Assert(int(maxCntlid)-int(minCntlid)+1, Equals, sharedBlockMaxFrontends)
	}
}

func (s *TestSuite) TestEngineSharedBlockRaidUUIDAndPtplFile(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, getStableVolumeRaidUUID("vol-a"))
	c.Assert(e.getRaidBdevCreateUUID(), Not(Equals), getStableVolumeNsUUID("vol-a"))
	e.sharedBlockState.volumeUID = "uid-a"
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "uid-a-0.json"))
	e.sharedBlockState.reservationEpoch = 3
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "uid-a-3.json"))

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")
}

func (s *TestSuite) TestEngineSharedBlockReportedInProto(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, false)

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockOnlySupportsBlockdev(c *C) {
	for _, frontend := range []string{lhtypes.FrontendSPDKTCPNvmf, lhtypes.FrontendUBLK} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", frontend, 1048576, 0, 0, make(chan interface{}, 1), nil, true)

		err := ef.handleFrontend(nil, "10.0.0.1:20006")
		c.Assert(err, ErrorMatches, ".*not supported for shared block engine frontend.*")
	}

	// The maintenance mode attaches a shared block volume without a frontend.
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	c.Assert(ef.handleFrontend(nil, "10.0.0.1:20006"), IsNil)
}

// The engine registers a shared block engine frontend before it connects, so
// suspending it while it is creating must fail, as it can connect afterwards.
func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSuspendWhileCreating(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning
	ef.isCreating = true

	err := ef.SuspendWithOptions(nil, false, false)
	c.Assert(err, ErrorMatches, ".*engine frontend ef-a is creating.*")
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
}

// An expansion resumes the engine frontend after its dm resize, so the engine
// must not rely on suspending it then.
func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSuspendWhileExpanding(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning
	ef.isExpanding = true

	err := ef.SuspendWithOptions(nil, false, false)
	c.Assert(err, ErrorMatches, ".*engine frontend ef-a is expanding.*")
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
}

// A shared block engine frontend is not in the map until it is created, but
// may connect to the engine target before that, so suspending it must fail
// instead of reporting it as nonexistent.
func (s *TestSuite) TestServerEngineFrontendSuspendRejectsCreatingSharedBlock(c *C) {
	srv := &Server{engineFrontendMap: map[string]*EngineFrontend{}, creatingSharedBlockFrontends: map[string]int{"ef-a": 1}}

	_, err := srv.EngineFrontendSuspend(context.Background(), &spdkrpc.EngineFrontendSuspendRequest{Name: "ef-a"})
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)

	_, err = srv.EngineFrontendSuspend(context.Background(), &spdkrpc.EngineFrontendSuspendRequest{Name: "ef-b"})
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.NotFound)
}

// The recovery of a shared block engine frontend holds the volume host lock
// while it waits for the engine to finish suspending the engine frontends, so
// suspending it must fail without waiting for the lock.
func (s *TestSuite) TestServerEngineFrontendSuspendRejectsRecoveringSharedBlock(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStatePending))
	srv := &Server{engineFrontendMap: map[string]*EngineFrontend{"ef-a": ef}}
	unlockVolumeHost := srv.acquireVolumeHostLock("vol-a")
	defer unlockVolumeHost()

	errCh := make(chan error, 1)
	go func() {
		_, err := srv.EngineFrontendSuspend(context.Background(), &spdkrpc.EngineFrontendSuspendRequest{Name: "ef-a"})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	case <-time.After(5 * time.Second):
		c.Fatal("suspending the recovering engine frontend waits for the volume host lock")
	}
}

// The engine has not taken the snapshot when it is already suspending the
// engine frontends, so the engine frontend must be able to resume at once.
func (s *TestSuite) TestServerEngineSnapshotCreateWhileFreezing(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	_, err := e.sharedBlockState.frontends.startFreezing()
	c.Assert(err, IsNil)
	srv := &Server{engineMap: map[string]*Engine{"engine-a": e}}

	_, err = srv.EngineSnapshotCreate(context.Background(), &spdkrpc.SnapshotRequest{Name: "engine-a", EngineFrontendName: "ef-a"})
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.Aborted)
	c.Assert(isEngineFrontendsFreezingError(err), Equals, true)
}

// While a shared block engine frontend waits for the engine after a failed
// snapshot, the engine may suspend and resume it as part of the next
// suspension, which takes its lock. The engine may also start suspending the
// engine frontends right after the wait, so the wait ends only with a
// registration under the lock.
func (s *TestSuite) TestEngineFrontendWaitForEngineFrontendsResumed(c *C) {
	freezingErr := grpcstatus.Error(grpccodes.Aborted, ErrSharedBlockFrontendsFreezing.Error())
	for _, tc := range []struct {
		registerErrs    []error
		wantLocked      []bool
		resumedByEngine bool
	}{
		// Freezing, waited, and then freezing again right after the wait.
		{registerErrs: []error{freezingErr, freezingErr, nil, freezingErr, nil, nil}, wantLocked: []bool{true, false, false, true, false, true}},
		{registerErrs: []error{freezingErr, nil}, wantLocked: []bool{true, false}, resumedByEngine: true},
		{registerErrs: []error{grpcstatus.Error(grpccodes.Unavailable, "engine is unreachable")}, wantLocked: []bool{true}},
	} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
		ef.State = lhtypes.InstanceStateRunning
		locked := []bool{}
		ef.registerToEngineFn = func(i *initiator.Initiator) error {
			c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateSuspended))
			unlocked := ef.TryLock()
			locked = append(locked, !unlocked)
			err := tc.registerErrs[len(locked)-1]
			if unlocked {
				if tc.resumedByEngine && err == nil {
					ef.State = lhtypes.InstanceStateRunning
				}
				ef.Unlock()
			}
			return err
		}

		ef.Lock()
		stillSuspended := ef.waitForEngineFrontendsResumed()
		ef.Unlock()
		c.Assert(locked, DeepEquals, tc.wantLocked)
		c.Assert(stillSuspended, Equals, !tc.resumedByEngine)
	}
}

// A recovery holds the volume host lock, so it must stop waiting for the engine
// once a concurrent engine frontend creation evicts it, which waits for the lock.
func (s *TestSuite) TestEngineFrontendRecoveryReconnectCancelledWhileFreezing(c *C) {
	freezingErr := grpcstatus.Error(grpccodes.Aborted, ErrSharedBlockFrontendsFreezing.Error())
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStatePending
	registered := 0
	ef.registerToEngineFn = func(i *initiator.Initiator) error {
		registered++
		ef.Lock()
		ef.State = lhtypes.InstanceStateRunning
		ef.Unlock()
		return freezingErr
	}

	err := ef.reconnectNvmeTCPPathForRecovery(nil)
	c.Assert(errors.Is(err, ErrRecoveryCancelled), Equals, true, Commentf("got %v", err))
	c.Assert(registered, Equals, 1)
}

// An engine may suspend the shared block device before the instance manager
// restarts, so the recovery keeps it suspended for the engine to resume it.
func (s *TestSuite) TestRecoverFromHostSharedBlockKeepsSuspendedDevice(c *C) {
	for _, tc := range []struct {
		suspended  bool
		checkErr   error
		wantState  lhtypes.InstanceState
		wantErrMsg string
	}{
		{suspended: false, wantState: lhtypes.InstanceStateRunning},
		{suspended: true, wantState: lhtypes.InstanceStateSuspended},
		{checkErr: fmt.Errorf("dmsetup failed"), wantState: lhtypes.InstanceStateError, wantErrMsg: ".*failed to check whether the device is suspended.*"},
	} {
		comment := Commentf("suspended %v, check error %v", tc.suspended, tc.checkErr)
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
		ef.metadataDir = c.MkDir()
		ef.getInitiatorEndpointFn = func() string { return "/dev/longhorn/vol-a" }
		ef.loadInitiatorNVMeDeviceInfoFn = func(transportAddress, transportServiceID, subsystemNQN string) error { return nil }
		ef.loadInitiatorEndpointFn = func(dmDeviceIsBusy bool) error { return nil }
		ef.isInitiatorSuspendedFn = func() (bool, error) { return tc.suspended, tc.checkErr }

		err := ef.RecoverFromHost(nil)
		if tc.wantErrMsg == "" {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, ErrorMatches, tc.wantErrMsg, comment)
		}
		c.Assert(ef.State, Equals, tc.wantState, comment)
		ef.signalStop()
	}
}

// An engine frontend suspended by an engine registers again as well, so that
// the engine restarted before resuming it can resume it in a later suspension.
func (s *TestSuite) TestEngineFrontendRegisterToEngineAgain(c *C) {
	for _, tc := range []struct {
		state          lhtypes.InstanceState
		wantRegistered bool
	}{
		{state: lhtypes.InstanceStateRunning, wantRegistered: true},
		{state: lhtypes.InstanceStateSuspended, wantRegistered: true},
		{state: lhtypes.InstanceStatePending, wantRegistered: false},
		{state: lhtypes.InstanceStateError, wantRegistered: false},
	} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
		ef.State = tc.state
		ef.initiator = &initiator.Initiator{}
		registered := false
		ef.registerToEngineFn = func(i *initiator.Initiator) error {
			registered = true
			return nil
		}

		c.Assert(ef.registerToEngineAgain(), IsNil)
		c.Assert(registered, Equals, tc.wantRegistered, Commentf("state %s", tc.state))
	}
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSnapshotCreateWithoutSuspend(c *C) {
	for _, tc := range []struct {
		state     lhtypes.InstanceState
		endpoint  string
		expanding bool
	}{
		{state: lhtypes.InstanceStateRunning, endpoint: ""},
		{state: lhtypes.InstanceStateError, endpoint: "/dev/longhorn/vol-a"},
		{state: lhtypes.InstanceStateRunning, endpoint: "/dev/longhorn/vol-a", expanding: true},
	} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
		ef.State = tc.state
		ef.Endpoint = tc.endpoint
		ef.isExpanding = tc.expanding

		_, err := ef.SnapshotCreate("snap-a")
		c.Assert(errors.Is(err, ErrEngineFrontendLifecyclePrecondition), Equals, true)
		c.Assert(string(ef.State), Equals, string(tc.state))
	}
}

func (s *TestSuite) TestEngineFrontendSharedBlockSnapshotCreateInMaintenanceMode(c *C) {
	errEngineCalled := errors.New("engine called")
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1),
		func(string) (*client.SPDKClient, error) { return nil, errEngineCalled }, true)
	ef.State = lhtypes.InstanceStateRunning

	_, err := ef.SnapshotCreate("snap-a")
	c.Assert(errors.Is(err, errEngineCalled), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSwitchover(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(errors.Is(err, ErrSwitchOverTargetInvalidInput), Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestSaveAndLoadEngineFrontendRecordPreservesSharedBlock(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-1", "engine-a", "vol-a",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].SharedBlock, Equals, true)
}

const (
	testReservationHostA = "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"
	testReservationHostB = "11111111-2222-3333-4444-555555555555"
)

func newTestReservationState(epoch uint64) *spdkrpc.ReservationState {
	return &spdkrpc.ReservationState{
		Epoch:        epoch,
		Type:         3,
		HolderKey:    0xAAAA,
		HolderHostId: testReservationHostA,
		Registrants: []*spdkrpc.ReservationRegistrant{
			{Key: 0xAAAA, HostId: testReservationHostA},
			{Key: 0xFFFFFFFFFFFFFFFF, HostId: testReservationHostB},
		},
	}
}

func (s *TestSuite) TestParseReservationPtplData(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	tests := []struct {
		name  string
		data  string
		valid bool
	}{
		{"written by SPDK", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"bdev UUID in upper case", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + strings.ToUpper(bdevUUID) + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"registered without reservation", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"no registrants", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"","holder_uuid":"","registrants":[]}`, true},
		{"empty", ``, false},
		{"whitespace only", " \n", false},
		{"truncated", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID, false},
		{"bdev UUID missing", `{"ptpl":true,"registrants":[]}`, false},
		{"registrants missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `"}`, false},
		{"host UUID missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":1}]}`, false},
		{"holder key not registered", `{"ptpl":true,"rtype":3,"crkey":1,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
		{"other bdev UUID", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + testReservationHostA + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
	}

	for _, test := range tests {
		_, err := parseReservationPtplData([]byte(test.data), bdevUUID)
		c.Assert(err == nil, Equals, test.valid, Commentf("%s: %v", test.name, err))
	}

	regs := make([]string, sharedBlockMaxFrontends+1)
	for i := range regs {
		regs[i] = fmt.Sprintf(`{"rkey":%d,"host_uuid":"%s"}`, i+1, testReservationHostA)
	}
	data := `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[` + strings.Join(regs, ",") + `]}`
	_, err := parseReservationPtplData([]byte(data), bdevUUID)
	c.Assert(err, ErrorMatches, ".*exceed the maximum.*")
}

func (s *TestSuite) TestReservationStatePtplDataRoundTrip(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	state := newTestReservationState(7)
	data, err := reservationStateToPtplData(state, bdevUUID)
	c.Assert(err, IsNil)

	record, err := parseReservationPtplData(data, bdevUUID)
	c.Assert(err, IsNil)
	got := reservationPtplRecordToState(record, 7)
	c.Assert(proto.Equal(got, state), Equals, true, Commentf("got %v", got))

	// A state without registrants has nothing to restore.
	data, err = reservationStateToPtplData(&spdkrpc.ReservationState{Epoch: 7}, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(data, IsNil)

	// A state without persistence has no registrants.
	record, err = parseReservationPtplData([]byte(`{"ptpl":false,"bdev_uuid":"","registrants":[{"rkey":1,"host_uuid":"`+testReservationHostA+`"}]}`), bdevUUID)
	c.Assert(err, IsNil)
	got = reservationPtplRecordToState(record, 7)
	c.Assert(got.Epoch, Equals, uint64(7))
	c.Assert(len(got.Registrants), Equals, 0)
}

func (s *TestSuite) TestEnginePrepareReservationPtplFile(c *C) {
	metadataDir := c.MkDir()
	newEngine := func(seed *spdkrpc.ReservationState) *Engine {
		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
		e.sharedBlockState.reservationEpoch = seed.Epoch
		e.sharedBlockState.reservationSeed = seed
		e.sharedBlockState.volumeUID = "uid-a"
		if seed.VolumeUid != "" {
			e.sharedBlockState.volumeUID = seed.VolumeUid
		}
		return e
	}
	bdevUUID := getStableVolumeRaidUUID("vol-a")
	readState := func(file string) *spdkrpc.ReservationState {
		data, err := os.ReadFile(file)
		c.Assert(err, IsNil)
		record, err := parseReservationPtplData(data, bdevUUID)
		c.Assert(err, IsNil)
		return reservationPtplRecordToState(record, 0)
	}

	// The first engine of an epoch rebuilds the file from the synced state.
	synced := newTestReservationState(1)
	e := newEngine(synced)
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

	// A newer local state of the same epoch wins over the synced state, e.g.,
	// SPDK persisted a preemption the manager has not synced yet.
	newer := newTestReservationState(1)
	newer.Registrants = newer.Registrants[:1]
	data, err := reservationStateToPtplData(newer, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(synced)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 1)

	// A partial local file falls back to the synced state.
	c.Assert(os.WriteFile(ptplFile, data[:20], 0644), IsNil)
	e = newEngine(synced)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

	// An empty local file may be left by a crash in the middle of a write, so
	// it falls back to the synced state, which is empty once the hosts disabled
	// persistence.
	for _, empty := range [][]byte{nil, []byte("\n")} {
		c.Assert(os.WriteFile(ptplFile, empty, 0644), IsNil)
		e = newEngine(synced)
		c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
		c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

		c.Assert(os.WriteFile(ptplFile, empty, 0644), IsNil)
		e = newEngine(&spdkrpc.ReservationState{Epoch: synced.Epoch})
		c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
		_, err = os.Stat(ptplFile)
		c.Assert(os.IsNotExist(err), Equals, true)
	}

	// A new epoch ignores and removes the file of the previous epoch, which
	// may be stale.
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(newTestReservationState(2))
	newFile := e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(newFile), IsNil)
	c.Assert(len(readState(newFile).Registrants), Equals, 2)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// A new volume with the same name ignores and removes the file of the
	// deleted volume, even of the same epoch.
	newVolumeSeed := &spdkrpc.ReservationState{Epoch: 2, VolumeUid: "uid-b"}
	e = newEngine(newVolumeSeed)
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	_, err = os.Stat(newFile)
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// Without a synced state, there is nothing to restore.
	e = newEngine(&spdkrpc.ReservationState{Epoch: 3})
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	entries, err := os.ReadDir(filepath.Dir(ptplFile))
	c.Assert(err, IsNil)
	c.Assert(len(entries), Equals, 0)

	// A synced state SPDK cannot restore fails the engine target instead of
	// exposing it without the reservation, even with an invalid local file.
	invalid := newTestReservationState(4)
	invalid.HolderKey = 0x1234
	e = newEngine(invalid)
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), NotNil)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), NotNil)
	data, err = os.ReadFile(ptplFile)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 0)
}

func (s *TestSuite) TestEngineCreateSharedBlockRequiresVolumeUID(c *C) {
	server := &Server{engineMap: map[string]*Engine{}}
	for _, reservation := range []*spdkrpc.ReservationState{nil, {Epoch: 1}} {
		_, err := server.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
			Name:        "engine-a",
			VolumeName:  "vol-a",
			SpecSize:    1048576,
			SharedBlock: true,
			Reservation: reservation,
		})
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument)
	}

	// The volume name and UID must not point out of the directory of the
	// ptpl files.
	for _, names := range [][2]string{{"..", "uid-a"}, {"vol-a", ".."}, {"vol-a", "../../frontends/uid-a"}} {
		_, err := server.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
			Name:        "engine-a",
			VolumeName:  names[0],
			SpecSize:    1048576,
			SharedBlock: true,
			Reservation: &spdkrpc.ReservationState{VolumeUid: names[1]},
		})
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument, Commentf("volume name %q, UID %q", names[0], names[1]))
	}
	c.Assert(server.engineMap, HasLen, 0)
}

func (s *TestSuite) TestEngineReservationCleanup(c *C) {
	metadataDir := c.MkDir()
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	e.sharedBlockState.volumeUID = "uid-a"
	other := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	for _, file := range []string{e.getReservationPtplFile(), other.getReservationPtplFile()} {
		c.Assert(os.MkdirAll(filepath.Dir(file), 0755), IsNil)
		c.Assert(os.WriteFile(file, []byte("{}"), 0644), IsNil)
	}
	server := &Server{metadataDir: metadataDir, engineMap: map[string]*Engine{e.Name: e}}
	cleanup := func(volumeName string) error {
		_, err := server.EngineReservationCleanup(context.Background(), &spdkrpc.EngineReservationCleanupRequest{VolumeName: volumeName})
		return err
	}

	// The state of an existing engine is kept.
	c.Assert(grpcstatus.Code(cleanup("vol-a")), Equals, grpccodes.FailedPrecondition)
	_, err := os.Stat(e.getReservationPtplFile())
	c.Assert(err, IsNil)

	// A name that may point out of the directory of the volume is rejected.
	for _, name := range []string{"", ".", "..", "/", "//", "/vol-a", "vol-a/", "../vol-b", "vol-a/.."} {
		c.Assert(grpcstatus.Code(cleanup(name)), Equals, grpccodes.InvalidArgument, Commentf("volume name %q", name))
	}

	delete(server.engineMap, e.Name)
	c.Assert(cleanup("vol-a"), IsNil)
	_, err = os.Stat(e.getReservationPtplDir())
	c.Assert(os.IsNotExist(err), Equals, true)
	_, err = os.Stat(other.getReservationPtplFile())
	c.Assert(err, IsNil)

	// Nothing to clean up for a missing directory.
	c.Assert(cleanup("vol-a"), IsNil)
}

func (s *TestSuite) TestEngineGetReservationState(c *C) {
	metadataDir := c.MkDir()
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	e.sharedBlockState.reservationEpoch = 5
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)

	// Not reported before the ptpl file is prepared, even if the engine is
	// running, e.g., in the maintenance mode without a target.
	e.State = lhtypes.InstanceStateRunning
	c.Assert(e.getWithoutLock().Reservation, IsNil)
	e.sharedBlockState.reservationPrepared = true

	// No file means no persisted state.
	state := e.getWithoutLock().Reservation
	c.Assert(state, NotNil)
	c.Assert(state.Epoch, Equals, uint64(5))
	c.Assert(len(state.Registrants), Equals, 0)

	data, err := reservationStateToPtplData(newTestReservationState(5), getStableVolumeRaidUUID("vol-a"))
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	state = e.getWithoutLock().Reservation
	c.Assert(proto.Equal(state, newTestReservationState(5)), Equals, true, Commentf("got %v", state))

	// A partial file is unknown, e.g., SPDK is writing it.
	c.Assert(os.WriteFile(ptplFile, data[:20], 0644), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// An empty file is unknown, even an old one, as a crash in the middle of
	// a write may leave it empty.
	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	old := time.Now().Add(-time.Hour)
	c.Assert(os.Chtimes(ptplFile, old, old), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// Engines without shared block report nothing.
	other := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, metadataDir)
	other.State = lhtypes.InstanceStateRunning
	other.sharedBlockState.reservationPrepared = true
	c.Assert(other.getWithoutLock().Reservation, IsNil)
}

func (f *fakeEngineFrontendSuspendServer) counts() (suspends, resumes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.suspends, f.resumes
}

// unreachableAddress returns an address no one listens on.
func unreachableAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return address
}

// newFreezeTestEngine returns a shared block engine whose target reports the
// given connected hosts, one set per query.
func newFreezeTestEngine(t *testing.T, connectedHosts ...[]string) *Engine {
	t.Helper()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 10), 0, client.NewSPDKClient, true, "")
	e.NvmeTcpTarget = &NvmeTcpTarget{Nqn: "nqn-a"}
	original := sharedBlockNvmfSubsystemGetControllers
	t.Cleanup(func() { sharedBlockNvmfSubsystemGetControllers = original })
	sharedBlockNvmfSubsystemGetControllers = func(_ *spdkclient.Client, nqn string) ([]spdktypes.NvmfSubsystemController, error) {
		if nqn != "nqn-a" {
			t.Errorf("unexpected NQN %s", nqn)
		}
		if len(connectedHosts) == 0 {
			t.Fatal("unexpected connected host query")
		}
		controllers := []spdktypes.NvmfSubsystemController{}
		for _, host := range connectedHosts[0] {
			controllers = append(controllers, spdktypes.NvmfSubsystemController{Hostnqn: host})
		}
		connectedHosts = connectedHosts[1:]
		return controllers, nil
	}
	fenceNewConnectionsForTest(t, nil)
	return e
}

// fenceNewConnectionsForTest records whether the engine target allows new
// connections, which it does initially. fail, if any, returns the error of a
// call setting whether to allow them.
func fenceNewConnectionsForTest(t *testing.T, fail func(allowAnyHost bool) error) *atomic.Bool {
	t.Helper()

	allowAnyHost := &atomic.Bool{}
	allowAnyHost.Store(true)
	original := sharedBlockNvmfSubsystemAllowAnyHost
	t.Cleanup(func() { sharedBlockNvmfSubsystemAllowAnyHost = original })
	sharedBlockNvmfSubsystemAllowAnyHost = func(_ *spdkclient.Client, nqn string, allow bool) (bool, error) {
		if nqn != "nqn-a" {
			t.Errorf("unexpected NQN %s", nqn)
		}
		if fail != nil {
			if err := fail(allow); err != nil {
				return false, err
			}
		}
		allowAnyHost.Store(allow)
		return true, nil
	}
	return allowAnyHost
}

func registerFrontendForTest(t *testing.T, e *Engine, name, address, hostNQN string) {
	t.Helper()

	if err := e.RegisterFrontend(nil, name, address, hostNQN); err != nil {
		t.Fatalf("register engine frontend %s: %v", name, err)
	}
}

func TestSharedBlockFrontendsSuspended(t *testing.T) {
	caller, callerAddress := startFakeEngineFrontendSuspendServer(t, nil)
	other, otherAddress := startFakeEngineFrontendSuspendServer(t, nil)
	gone, goneAddress := startFakeEngineFrontendSuspendServer(t, grpcstatus.Error(grpccodes.NotFound, "cannot find engine frontend"))

	// The engine frontends on host-c and host-e do not exist or are
	// unreachable, but their hosts are not connected, so they cannot write and
	// are skipped without a request.
	e := newFreezeTestEngine(t, []string{"host-a", "host-b"}, []string{"host-a", "host-b"})
	registerFrontendForTest(t, e, "ef-a", callerAddress, "host-a")
	registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")
	registerFrontendForTest(t, e, "ef-c", goneAddress, "host-c")
	registerFrontendForTest(t, e, "ef-e", unreachableAddress(t), "host-e")

	allowAnyHost := fenceNewConnectionsForTest(t, nil)

	worked := false
	err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "ef-a", false, false, func() error {
		worked = true
		if allowAnyHost.Load() {
			t.Error("engine target allows new connections during the work")
		}
		if suspends, resumes := other.counts(); suspends != 1 || resumes != 0 {
			t.Errorf("ef-b is not suspended during the work: suspends %d, resumes %d", suspends, resumes)
		}
		if err := e.RegisterFrontend(nil, "ef-d", otherAddress, "host-d"); !errors.Is(err, ErrSharedBlockFrontendsFreezing) {
			t.Errorf("expected registration to be rejected during the work, got %v", err)
		}
		if err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error { return nil }); !errors.Is(err, ErrSharedBlockFrontendsFreezing) {
			t.Errorf("expected concurrent suspension to be rejected, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !worked {
		t.Fatal("work did not run")
	}
	if !allowAnyHost.Load() {
		t.Error("engine target does not allow new connections after the work")
	}
	if suspends, resumes := caller.counts(); suspends != 0 || resumes != 0 {
		t.Errorf("the caller is suspended by itself, got suspends %d, resumes %d", suspends, resumes)
	}
	if suspends, resumes := other.counts(); suspends != 1 || resumes != 1 {
		t.Errorf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
	}
	if suspends, resumes := gone.counts(); suspends != 0 || resumes != 0 {
		t.Errorf("ef-c of a host that is not connected must not be called, got suspends %d, resumes %d", suspends, resumes)
	}
	other.mu.Lock()
	defer other.mu.Unlock()
	if other.noFlush || other.noLockFs {
		t.Errorf("ef-b must be suspended with the given options, got noFlush %v, noLockFs %v", other.noFlush, other.noLockFs)
	}
	registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
}

func TestSharedBlockFrontendsSuspendedAfterFrontendReplaced(t *testing.T) {
	deleted, deletedAddress := startFakeEngineFrontendSuspendServer(t, grpcstatus.Error(grpccodes.NotFound, "cannot find engine frontend"))
	current, currentAddress := startFakeEngineFrontendSuspendServer(t, nil)

	// ef-old on host-b is deleted and replaced by ef-new, which keeps host-b
	// connected.
	e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
	registerFrontendForTest(t, e, "ef-old", deletedAddress, "host-b")
	registerFrontendForTest(t, e, "ef-new", currentAddress, "host-b")

	if err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if suspends, _ := deleted.counts(); suspends != 0 {
		t.Errorf("the replaced ef-old must not be suspended, got suspends %d", suspends)
	}
	if suspends, resumes := current.counts(); suspends != 1 || resumes != 1 {
		t.Errorf("ef-new must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
	}
}

func TestSharedBlockFrontendsSuspendedCancelledBeforeWork(t *testing.T) {
	other, otherAddress := startFakeEngineFrontendSuspendServer(t, nil)

	e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
	registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")

	// The caller gives up, e.g., times out, and resumes itself, so the work
	// must not run.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	worked := false
	err := e.withSharedBlockFrontendsSuspended(ctx, nil, "ef-a", false, false, func() error {
		worked = true
		return nil
	})
	if err == nil {
		t.Fatal("expected the cancelled suspension to fail")
	}
	if worked {
		t.Error("work ran after the caller gave up")
	}
	if suspends, resumes := other.counts(); suspends != 1 || resumes != 1 {
		t.Errorf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
	}
	registerFrontendForTest(t, e, "ef-c", otherAddress, "host-c")
}

func TestSharedBlockFrontendsSuspendedFailure(t *testing.T) {
	tests := []struct {
		name            string
		suspendErr      error
		unreachableHost string
		connectedHosts  [][]string
		expectedErr     string
		expectedResumes int
	}{
		{
			name:            "unreachable engine frontend whose host is connected",
			unreachableHost: "host-c",
			connectedHosts:  [][]string{{"host-b", "host-c"}},
			expectedErr:     "failed to suspend engine frontend ef-c",
			expectedResumes: 1,
		},
		{
			name:           "engine frontend fails to suspend",
			suspendErr:     grpcstatus.Error(grpccodes.FailedPrecondition, "engine frontend is creating"),
			connectedHosts: [][]string{{"host-b"}},
			expectedErr:    "failed to suspend engine frontend ef-b",
		},
		{
			name:            "engine frontend suspension times out",
			suspendErr:      grpcstatus.Error(grpccodes.DeadlineExceeded, "context deadline exceeded"),
			connectedHosts:  [][]string{{"host-b"}},
			expectedErr:     "failed to suspend engine frontend ef-b",
			expectedResumes: 1,
		},
		{
			name:           "engine frontend does not exist but its host is connected",
			suspendErr:     grpcstatus.Error(grpccodes.NotFound, "cannot find engine frontend"),
			connectedHosts: [][]string{{"host-b"}},
			expectedErr:    "failed to suspend engine frontend ef-b",
		},
		{
			name:            "host connects without a suspended engine frontend",
			connectedHosts:  [][]string{{"host-b"}, {"host-b", "host-x"}},
			expectedErr:     "host host-x is connected to engine engine-a without a suspended engine frontend",
			expectedResumes: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other, otherAddress := startFakeEngineFrontendSuspendServer(t, tt.suspendErr)

			e := newFreezeTestEngine(t, tt.connectedHosts...)
			registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")
			if tt.unreachableHost != "" {
				registerFrontendForTest(t, e, "ef-c", unreachableAddress(t), tt.unreachableHost)
			}

			err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error {
				t.Error("work must not run")
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("expected error %q, got %v", tt.expectedErr, err)
			}
			// ef-b is resumed if it is suspended, or its suspension times out
			// and may still complete.
			if suspends, resumes := other.counts(); suspends != 1 || resumes != tt.expectedResumes {
				t.Errorf("expected ef-b to be suspended once and resumed %d times, got suspends %d, resumes %d", tt.expectedResumes, suspends, resumes)
			}
			// The failure does not leave the engine frontends being suspended.
			registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
		})
	}
}

func TestSharedBlockFrontendsSuspendedFenceFailure(t *testing.T) {
	other, otherAddress := startFakeEngineFrontendSuspendServer(t, nil)

	e := newFreezeTestEngine(t)
	allowAnyHost := fenceNewConnectionsForTest(t, func(allow bool) error {
		if !allow {
			return errors.New("failed to set allow_any_host")
		}
		return nil
	})
	registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")

	// Nothing is suspended, as a host might connect without being suspended.
	err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error {
		t.Error("work must not run")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "failed to reject new connections") {
		t.Fatalf("expected fence failure, got %v", err)
	}
	if suspends, resumes := other.counts(); suspends != 0 || resumes != 0 {
		t.Errorf("ef-b must not be suspended, got suspends %d, resumes %d", suspends, resumes)
	}

	// The rejection may take effect even if the call fails, so the next
	// registration allows new connections.
	allowAnyHost.Store(false)
	registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
	if !allowAnyHost.Load() {
		t.Error("registration must allow new connections after the fence failure")
	}
}

func TestSharedBlockFrontendsSuspendedAllowFailure(t *testing.T) {
	other, otherAddress := startFakeEngineFrontendSuspendServer(t, nil)

	e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
	registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")
	allowFails := &atomic.Bool{}
	allowFails.Store(true)
	allowAnyHost := fenceNewConnectionsForTest(t, func(allow bool) error {
		if allow && allowFails.Load() {
			return errors.New("failed to set allow_any_host")
		}
		return nil
	})

	// The work is done, so the failure to allow new connections afterwards
	// does not fail it.
	if err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowAnyHost.Load() {
		t.Fatal("engine target must still reject new connections")
	}
	if suspends, resumes := other.counts(); suspends != 1 || resumes != 1 {
		t.Errorf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
	}

	// The registration fails until it allows new connections, so that the
	// engine frontend does not connect before then.
	if err := e.RegisterFrontend(nil, "ef-d", otherAddress, "host-d"); err == nil || !strings.Contains(err.Error(), "failed to allow new connections") {
		t.Fatalf("expected registration to fail to allow new connections, got %v", err)
	}
	allowFails.Store(false)
	registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
	if !allowAnyHost.Load() {
		t.Fatal("registration must allow new connections")
	}

	// New connections are allowed, so the next registration does not allow
	// them again.
	allowAnyHost.Store(false)
	registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
	if allowAnyHost.Load() {
		t.Error("registration must not allow new connections once they are allowed")
	}
}

// The engine frontends on all hosts must be suspended before the replica add
// does anything, and nothing is done if they cannot be.
func TestServerEngineReplicaAddSharedBlockSuspendFailure(t *testing.T) {
	e := newFreezeTestEngine(t, []string{"host-b"})
	e.State = lhtypes.InstanceStateRunning
	registerFrontendForTest(t, e, "ef-b", unreachableAddress(t), "host-b")
	srv := &Server{engineMap: map[string]*Engine{e.Name: e}}

	_, err := srv.EngineReplicaAdd(context.Background(), &spdkrpc.EngineReplicaAddRequest{
		EngineName:     e.Name,
		ReplicaName:    "replica-c",
		ReplicaAddress: "10.0.0.3:20001",
	})
	if err == nil || !strings.Contains(err.Error(), "failed to suspend engine frontend ef-b") {
		t.Fatalf("expected suspension failure, got %v", err)
	}
	if len(e.backends) != 0 {
		t.Fatalf("replica add must not start, got backends %v", e.backends)
	}
}

func TestSharedBlockReplicaAddFrontendSuspendResumeWrapper(t *testing.T) {
	getSPDKClient := func() *spdkclient.Client { return nil }

	t.Run("snapshot step suspends the engine frontends", func(t *testing.T) {
		ef, efAddress := startFakeEngineFrontendSuspendServer(t, nil)
		e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", efAddress, "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(context.Background(), getSPDKClient)

		err := wrapper(func() error {
			if suspends, resumes := ef.counts(); suspends != 1 || resumes != 0 {
				t.Errorf("ef-b is not suspended during the work: suspends %d, resumes %d", suspends, resumes)
			}
			return nil
		}, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if suspends, resumes := ef.counts(); suspends != 1 || resumes != 1 {
			t.Fatalf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
		}
	})

	t.Run("finish step suspends the engine frontends", func(t *testing.T) {
		ef, efAddress := startFakeEngineFrontendSuspendServer(t, nil)
		e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", efAddress, "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(context.Background(), getSPDKClient)

		workErr := errors.New("finish failed")
		err := wrapper(func() error {
			if suspends, resumes := ef.counts(); suspends != 1 || resumes != 0 {
				t.Errorf("ef-b is not suspended during the work: suspends %d, resumes %d", suspends, resumes)
			}
			return workErr
		}, false)
		if !errors.Is(err, workErr) {
			t.Fatalf("expected the work error, got %v", err)
		}
		if suspends, resumes := ef.counts(); suspends != 1 || resumes != 1 {
			t.Fatalf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
		}
		ef.mu.Lock()
		defer ef.mu.Unlock()
		if !ef.noFlush || !ef.noLockFs {
			t.Fatalf("ef-b must be suspended with noflush and nolockfs, got %v, %v", ef.noFlush, ef.noLockFs)
		}
	})

	t.Run("finish step waits for another suspension", func(t *testing.T) {
		ef, efAddress := startFakeEngineFrontendSuspendServer(t, nil)
		e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", efAddress, "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(context.Background(), getSPDKClient)

		// Another suspension, e.g., of a snapshot, is in progress.
		if _, err := e.sharedBlockState.frontends.startFreezing(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var worked atomic.Bool
		done := make(chan error, 1)
		go func() {
			done <- wrapper(func() error {
				worked.Store(true)
				if suspends, _ := ef.counts(); suspends != 1 {
					t.Errorf("ef-b is not suspended during the work: suspends %d", suspends)
				}
				return nil
			}, false)
		}()
		time.Sleep(100 * time.Millisecond)
		if worked.Load() {
			t.Fatal("finish step must not run during another suspension")
		}

		e.sharedBlockState.frontends.stopFreezing()
		select {
		case err := <-done:
			if err != nil || !worked.Load() {
				t.Fatalf("expected work to run, got worked %v, err %v", worked.Load(), err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("finish step does not run after the other suspension")
		}
	})

	t.Run("finish step proceeds without the suspension", func(t *testing.T) {
		e := newFreezeTestEngine(t, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", unreachableAddress(t), "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(context.Background(), getSPDKClient)

		worked := false
		err := wrapper(func() error {
			worked = true
			// No suspension, e.g., of a snapshot, can start and resume the
			// engine frontends during the work.
			if err := e.withSharedBlockFrontendsSuspended(context.Background(), nil, "", false, false, func() error { return nil }); !errors.Is(err, ErrSharedBlockFrontendsFreezing) {
				t.Errorf("expected suspension to be rejected during the work, got %v", err)
			}
			return nil
		}, false)
		if err != nil || !worked {
			t.Fatalf("expected work to run, got worked %v, err %v", worked, err)
		}
		if _, err := e.sharedBlockState.frontends.startFreezing(); err != nil {
			t.Fatalf("expected suspension to be allowed after the work, got %v", err)
		}
	})

	t.Run("finish step keeps new connections to be allowed when proceeding without the suspension", func(t *testing.T) {
		e := newFreezeTestEngine(t)
		registerFrontendForTest(t, e, "ef-b", unreachableAddress(t), "host-b")
		allowAnyHost := fenceNewConnectionsForTest(t, func(allow bool) error {
			if !allow {
				return errors.New("failed to set allow_any_host")
			}
			return nil
		})
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(context.Background(), getSPDKClient)

		// The rejection may take effect even if the call fails.
		if err := wrapper(func() error { return nil }, false); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		allowAnyHost.Store(false)
		registerFrontendForTest(t, e, "ef-d", unreachableAddress(t), "host-d")
		if !allowAnyHost.Load() {
			t.Error("registration must allow new connections after the work")
		}
	})
}
