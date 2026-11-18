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

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestServerEngineReplicaListEmptyForShardedEngine(c *C) {
	fmt.Println("Testing Server.EngineReplicaList returns an empty replica map for EC engines")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", "", nil),
	}

	srv := &Server{
		engineMap: map[string]*Engine{e.Name: e},
	}

	resp, err := srv.EngineReplicaList(context.Background(), &spdkrpc.EngineReplicaListRequest{
		EngineName: e.Name,
	})
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.Replicas, NotNil)
	c.Assert(len(resp.Replicas), Equals, 0)
}

func (s *TestSuite) TestServerEngineReplicaListNotFound(c *C) {
	fmt.Println("Testing Server.EngineReplicaList returns NotFound for an unknown engine")

	srv := &Server{
		engineMap: map[string]*Engine{},
	}

	_, err := srv.EngineReplicaList(context.Background(), &spdkrpc.EngineReplicaListRequest{
		EngineName: "missing",
	})
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestBackendLocalBdevName(c *C) {
	fmt.Println("Testing Backend.LocalBdevName is recorded at construction and shown by String")

	for _, u := range []Backend{
		newReplicaBackend("r1", "10.0.0.1:1234", "lvs/head", nil),
		newShardGroupBackend("sg1", "10.0.0.1:1234", "lvs/head", nil),
	} {
		c.Assert(u.LocalBdevName(), Equals, "lvs/head")
		c.Assert(strings.Contains(fmt.Sprintf("%+v", u), "LocalBdevName:lvs/head"), Equals, true)
	}

	remote := newReplicaBackend("r2", "10.0.0.2:1234", "", nil)
	c.Assert(remote.LocalBdevName(), Equals, "")
}

func (s *TestSuite) TestRestoreFrontendBlocksRestoreAndFrontendCreate(c *C) {
	fmt.Println("Testing a registered restore frontend rejects a new restore and an engine frontend create, and the rejection carries the teardown error once the teardown has given up")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	prevEF := NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)
	prevEF.State = lhtypes.InstanceStateRunning

	srv := &Server{
		engineMap:          map[string]*Engine{e.Name: e},
		engineFrontendMap:  map[string]*EngineFrontend{},
		restoreFrontendMap: map[string]*EngineFrontend{e.Name: prevEF},
		volumeHostLocks:    map[string]*volumeHostLockEntry{},
	}

	restoreReq := &spdkrpc.EngineBackupRestoreRequest{
		EngineName: e.Name,
		BackupUrl:  "backup://target?backup=backup-1&volume=vol-a",
	}
	createReq := &spdkrpc.EngineFrontendCreateRequest{
		Name:       "ef-test",
		EngineName: "engine-a",
		VolumeName: "vol-a",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   1024,
	}

	// While the teardown is still running there is no error to report.
	_, err := srv.EngineBackupRestore(context.Background(), restoreReq)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "has not finished tearing down"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "(state running)"), Equals, true)

	_, err = srv.EngineFrontendCreate(context.Background(), createReq)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "has not finished tearing down"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "(state running)"), Equals, true)

	// Once the teardown has given up, the rejection is the only place the
	// teardown error is reported.
	prevEF.Lock()
	prevEF.State = lhtypes.InstanceStateError
	prevEF.ErrorMsg = "failed to tear down the restore initiator after retrying for 2m0s; the NVMe connection to nqn.vol-a may still be held: controller busy"
	prevEF.Unlock()

	_, err = srv.EngineBackupRestore(context.Background(), restoreReq)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "(state error)"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "controller busy"), Equals, true)

	_, err = srv.EngineFrontendCreate(context.Background(), createReq)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "(state error)"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "controller busy"), Equals, true)
}

func (s *TestSuite) TestRestoreFrontendBlocksRestoreOnReplacementEngineOfSameVolume(c *C) {
	fmt.Println("Testing a registered restore frontend rejects a restore on a differently named engine of the same volume and allows one on another volume")

	// engine-a was deleted while its restore teardown was still running, so
	// its restore frontend stays registered. engine-b replaces it for vol-a.
	prevEF := NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)
	prevEF.State = lhtypes.InstanceStateRunning

	srv := &Server{
		engineFrontendMap:  map[string]*EngineFrontend{},
		restoreFrontendMap: map[string]*EngineFrontend{"engine-a": prevEF},
	}

	err := srv.validateEngineReadyForRestoreLocked("engine-b", "vol-a")
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "cannot restore backup to engine engine-b"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "has not finished tearing down"), Equals, true)

	// A different volume shares neither the NQN nor the dm device.
	c.Assert(srv.validateEngineReadyForRestoreLocked("engine-c", "vol-b"), IsNil)
}

func (s *TestSuite) TestRegisterRestoreFrontendRejectsOccupiedEngineName(c *C) {
	fmt.Println("Testing a restore on an engine name whose previous restore teardown is still running is rejected even when the engine now belongs to another volume")

	// engine-a (vol-a) was deleted while its restore teardown was still
	// running, then engine-a was recreated for vol-b. The volume check does
	// not match, so only the engine-name slot protects vol-a's guard.
	prevEF := NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)
	prevEF.State = lhtypes.InstanceStateRunning
	e := NewEngine("engine-a", "vol-b", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

	srv := &Server{
		engineMap:          map[string]*Engine{e.Name: e},
		engineFrontendMap:  map[string]*EngineFrontend{},
		restoreFrontendMap: map[string]*EngineFrontend{"engine-a": prevEF},
	}

	tempEF, err := srv.registerRestoreFrontend(e, "vol-b", 1024)
	c.Assert(tempEF, IsNil)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(strings.Contains(err.Error(), "cannot restore backup to engine engine-a"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "has not finished tearing down"), Equals, true)
	c.Assert(srv.restoreFrontendMap["engine-a"], Equals, prevEF)
}

func (s *TestSuite) TestServerEngineDeleteStopsAndUnregistersRestoreTeardown(c *C) {
	fmt.Println("Testing Server.EngineDelete signals the restore frontend to stop and drops its entry when the teardown goroutine has already exited")

	prevEF := NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)
	// The teardown gave up before the deletion: the goroutine has exited
	// with the frontend in error and the entry still registered.
	prevEF.State = lhtypes.InstanceStateError
	prevEF.markRestoreTeardownDone()

	// The engine is already gone (e.g. recreated after an errored restore
	// teardown); only the stale restore frontend entry remains.
	srv := &Server{
		engineMap:          map[string]*Engine{},
		restoreFrontendMap: map[string]*EngineFrontend{"engine-a": prevEF},
	}

	_, err := srv.EngineDelete(context.Background(), &spdkrpc.EngineDeleteRequest{
		Name: "engine-a",
	})
	c.Assert(err, IsNil)
	// Nobody else will drop the entry, so the deletion does it.
	c.Assert(len(srv.restoreFrontendMap), Equals, 0)
	c.Assert(isClosed(prevEF.stopCh), Equals, true)

	// Deleting the same engine again must not panic on the closed channel.
	srv.restoreFrontendMap["engine-a"] = prevEF
	_, err = srv.EngineDelete(context.Background(), &spdkrpc.EngineDeleteRequest{
		Name: "engine-a",
	})
	c.Assert(err, IsNil)
	c.Assert(len(srv.restoreFrontendMap), Equals, 0)
}

// newServerWithBlockedRestoreTeardown builds a server whose only state is a
// restore frontend for engine-a with its teardown goroutine blocked inside the
// disconnect. Closing releaseDisconnect lets the disconnect return
// disconnectErr. The returned cleanup restores the package knobs.
func newServerWithBlockedRestoreTeardown(c *C, disconnectErr error) (srv *Server, restoreEF *EngineFrontend, releaseDisconnect chan struct{}, cleanup func()) {
	originalStop := stopRestoreInitiator
	originalInterval := restoreInitiatorTeardownRetryInterval
	originalTimeout := restoreInitiatorTeardownTimeout
	cleanup = func() {
		stopRestoreInitiator = originalStop
		restoreInitiatorTeardownRetryInterval = originalInterval
		restoreInitiatorTeardownTimeout = originalTimeout
	}
	restoreInitiatorTeardownRetryInterval = 10 * time.Second
	restoreInitiatorTeardownTimeout = time.Hour

	disconnectEntered := make(chan struct{})
	releaseDisconnect = make(chan struct{})
	stopRestoreInitiator = func(*initiator.Initiator) error {
		close(disconnectEntered)
		<-releaseDisconnect
		return disconnectErr
	}

	restoreEF = NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)
	restoreEF.State = lhtypes.InstanceStateRunning
	restoreEF.IsRestoring = true
	restoreEF.initiator = &initiator.Initiator{}

	srv = &Server{
		engineMap:          map[string]*Engine{},
		restoreFrontendMap: map[string]*EngineFrontend{"engine-a": restoreEF},
	}
	// Same wiring as EngineBackupRestore: the goroutine removes its own
	// entry under the server lock.
	restoreEF.unregisterRestoreFrontendFn = func() {
		srv.Lock()
		if srv.restoreFrontendMap["engine-a"] == restoreEF {
			delete(srv.restoreFrontendMap, "engine-a")
		}
		srv.Unlock()
	}

	go restoreEF.teardownRestoreFrontend()
	select {
	case <-disconnectEntered:
	case <-time.After(5 * time.Second):
		c.Fatal("the restore teardown never entered the disconnect")
	}
	return srv, restoreEF, releaseDisconnect, cleanup
}

func (s *TestSuite) TestServerEngineDeleteWaitsForRunningRestoreTeardown(c *C) {
	fmt.Println("Testing Server.EngineDelete does not return while a restore teardown attempt is still inside the disconnect, and the entry is gone once it ends")

	srv, restoreEF, releaseDisconnect, cleanup := newServerWithBlockedRestoreTeardown(c, nil)
	defer cleanup()

	deleteDone := make(chan error, 1)
	go func() {
		_, err := srv.EngineDelete(context.Background(), &spdkrpc.EngineDeleteRequest{Name: "engine-a"})
		deleteDone <- err
	}()

	// The stop is signalled right away, but the attempt already inside the
	// disconnect cannot be interrupted, so the deletion must keep waiting and
	// the entry must keep gating restores and frontend creates.
	select {
	case <-restoreEF.stopCh:
	case <-time.After(5 * time.Second):
		c.Fatal("EngineDelete did not signal the restore frontend to stop")
	}
	select {
	case err := <-deleteDone:
		c.Fatalf("EngineDelete returned (err=%v) while the disconnect was still running", err)
	case <-time.After(200 * time.Millisecond):
	}
	srv.RLock()
	c.Assert(srv.restoreFrontendMap["engine-a"], Equals, restoreEF)
	srv.RUnlock()

	close(releaseDisconnect)
	select {
	case err := <-deleteDone:
		c.Assert(err, IsNil)
	case <-time.After(5 * time.Second):
		c.Fatal("EngineDelete did not return after the disconnect ended")
	}
	srv.RLock()
	c.Assert(len(srv.restoreFrontendMap), Equals, 0)
	srv.RUnlock()
}

func (s *TestSuite) TestServerEngineDeleteWaitTimeoutLeavesRestoreEntryRegistered(c *C) {
	fmt.Println("Testing Server.EngineDelete returns after the wait timeout with the restore entry still registered, and the teardown goroutine drops it when the disconnect ends")

	originalWaitTimeout := restoreTeardownWaitTimeout
	defer func() { restoreTeardownWaitTimeout = originalWaitTimeout }()
	restoreTeardownWaitTimeout = 20 * time.Millisecond

	// The disconnect fails when released: the goroutine must still drop the
	// entry, because the engine it protected is gone.
	srv, restoreEF, releaseDisconnect, cleanup := newServerWithBlockedRestoreTeardown(c, fmt.Errorf("controller busy"))
	defer cleanup()

	_, err := srv.EngineDelete(context.Background(), &spdkrpc.EngineDeleteRequest{Name: "engine-a"})
	c.Assert(err, IsNil)
	c.Assert(isClosed(restoreEF.stopCh), Equals, true)

	// The deletion gave up waiting. The volume-wide disconnect is still
	// running, so the entry must stay to keep rejecting a recreated engine's
	// restore or frontend create.
	srv.RLock()
	c.Assert(srv.restoreFrontendMap["engine-a"], Equals, restoreEF)
	srv.RUnlock()

	close(releaseDisconnect)
	select {
	case <-restoreEF.restoreTeardownDone:
	case <-time.After(5 * time.Second):
		c.Fatal("the restore teardown did not end after the disconnect was released")
	}
	srv.RLock()
	c.Assert(len(srv.restoreFrontendMap), Equals, 0)
	srv.RUnlock()
	// Not marked error: the engine deletion, not the failed attempt, ended
	// the teardown.
	c.Assert(string(restoreEF.State), Equals, string(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestServerEngineReplicaAddRejectedOnShardedEngine(c *C) {
	fmt.Println("Testing Server.EngineReplicaAdd rejects EC engines with FailedPrecondition")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", "", nil),
	}

	srv := &Server{
		engineMap: map[string]*Engine{e.Name: e},
	}

	_, err := srv.EngineReplicaAdd(context.Background(), &spdkrpc.EngineReplicaAddRequest{
		EngineName:     e.Name,
		ReplicaName:    "r-new",
		ReplicaAddress: "10.0.0.99:1234",
	})
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineReplicaDeleteRejectedOnShardedEngine(c *C) {
	fmt.Println("Testing Server.EngineReplicaDelete rejects EC engines with FailedPrecondition")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", "", nil),
	}

	srv := &Server{
		engineMap: map[string]*Engine{e.Name: e},
	}

	_, err := srv.EngineReplicaDelete(context.Background(), &spdkrpc.EngineReplicaDeleteRequest{
		EngineName:     e.Name,
		ReplicaName:    "r-stale",
		ReplicaAddress: "10.0.0.99:1234",
	})
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
}

func (s *TestSuite) TestServerEngineSnapshotHashStatusUnimplementedForShardedEngine(c *C) {
	fmt.Println("Testing Server.EngineSnapshotHashStatus returns Unimplemented for EC engines")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", "", nil),
	}

	srv := &Server{
		engineMap: map[string]*Engine{e.Name: e},
	}

	_, err := srv.EngineSnapshotHashStatus(context.Background(), &spdkrpc.SnapshotHashStatusRequest{
		Name:         e.Name,
		SnapshotName: "snap-1",
	})
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.Unimplemented)
}

// The engine uses the head lvol of a Running ShardGroup in this Server
// directly, when the requested address is the address of that ShardGroup.
// In every other case it connects over NVMe-oF, and the head lvol stays
// exposed.
func (s *TestSuite) TestStopExposeLocalShardGroup(c *C) {
	fmt.Println("Testing Server.stopExposeLocalShardGroup returns the head lvol alias only for a Running local ShardGroup at the requested address")

	_, _, restore := mockShardGroupExposeBdev(nil, nil)
	sg := newExposedLocalShardGroup()
	sgAddress := "10.0.0.1:20002"
	srv := &Server{shardGroupMap: map[string]*ShardGroup{sg.Name: sg}}

	c.Assert(srv.stopExposeLocalShardGroup(nil, "on-another-node", "10.0.0.2:20002"), Equals, "")

	// Same name, but the request points to a ShardGroup on another node.
	c.Assert(srv.stopExposeLocalShardGroup(nil, sg.Name, "10.0.0.2:20002"), Equals, "")
	c.Assert(sg.IsExposed, Equals, true)

	sg.State = lhtypes.InstanceStateStopped
	c.Assert(srv.stopExposeLocalShardGroup(nil, sg.Name, sgAddress), Equals, "")
	c.Assert(sg.IsExposed, Equals, true)

	sg.State = lhtypes.InstanceStateRunning
	c.Assert(srv.stopExposeLocalShardGroup(nil, sg.Name, sgAddress), Equals, sg.HeadAlias)
	c.Assert(sg.IsExposed, Equals, false)
	restore()

	_, _, restore = mockShardGroupExposeBdev(fmt.Errorf("stop failed"), nil)
	defer restore()
	sg.IsExposed = true
	c.Assert(srv.stopExposeLocalShardGroup(nil, sg.Name, sgAddress), Equals, "")
	c.Assert(sg.IsExposed, Equals, true)
}

// EngineDelete exposes the head lvol again for each ShardGroup the engine
// used directly. A ShardGroup that was deleted first is skipped.
func (s *TestSuite) TestStartExposeLocalShardGroups(c *C) {
	fmt.Println("Testing the head lvols an engine used directly are found and exposed again")

	_, started, restore := mockShardGroupExposeBdev(nil, nil)
	defer restore()

	sg := newExposedLocalShardGroup()
	sg.IsExposed = false
	srv := &Server{shardGroupMap: map[string]*ShardGroup{sg.Name: sg}}

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		sg.Name: newShardGroupBackend(sg.Name, "10.0.0.1:20002", sg.HeadAlias, nil),
	}
	c.Assert(getLocalShardGroupNames(e), DeepEquals, []string{sg.Name})

	e.backends = map[string]Backend{
		sg.Name: newShardGroupBackend(sg.Name, "10.0.0.1:20002", "", nil),
		"r1":    newReplicaBackend("r1", "10.0.0.1:1234", "lvs/head", nil),
	}
	c.Assert(getLocalShardGroupNames(e), HasLen, 0)

	srv.startExposeLocalShardGroups(nil, []string{sg.Name, "deleted-first"})
	c.Assert(sg.IsExposed, Equals, true)
	c.Assert(*started, DeepEquals, []string{sg.Nqn})
}

// EngineCreate stops exposing the head lvol of a local ShardGroup for the
// engine to use. When the engine does not take it, EngineCreate exposes it
// again.
func (s *TestSuite) TestEngineCreateExposesUnusedLocalShardGroupAgain(c *C) {
	fmt.Println("Testing Server.EngineCreate exposes the head lvol again when the engine does not take it")

	stopped, started, restore := mockShardGroupExposeBdev(nil, nil)
	defer restore()

	sg := newExposedLocalShardGroup()
	srv := &Server{
		engineMap:     map[string]*Engine{},
		shardGroupMap: map[string]*ShardGroup{sg.Name: sg},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeEngine: make(chan interface{}, 1),
		},
		// The size validation fails, so Create returns before the engine
		// takes any backend.
		newServiceClient: func(string) (*client.SPDKClient, error) {
			return nil, fmt.Errorf("unreachable")
		},
	}

	_, err := srv.EngineCreate(context.Background(), &spdkrpc.EngineCreateRequest{
		Name:              "engine-a",
		VolumeName:        "vol-a",
		SpecSize:          10,
		DataLayoutType:    spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_SHARDED,
		ReplicaAddressMap: map[string]string{sg.Name: "10.0.0.1:20002"},
	})

	c.Assert(err, NotNil)
	c.Assert(*stopped, DeepEquals, []string{sg.Nqn})
	c.Assert(*started, DeepEquals, []string{sg.Nqn})
	c.Assert(sg.IsExposed, Equals, true)
}
