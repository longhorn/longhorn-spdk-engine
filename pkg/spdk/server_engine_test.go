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

func (s *TestSuite) TestServerEngineReplicaListEmptyForShardedEngine(c *C) {
	fmt.Println("Testing Server.EngineReplicaList returns an empty replica map for EC engines")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil),
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
	prevEF.ErrorMsg = "failed to tear down the restore initiator within 2m0s; the NVMe connection to nqn.vol-a may still be held: controller busy"
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
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil),
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
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil),
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
		"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil),
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
