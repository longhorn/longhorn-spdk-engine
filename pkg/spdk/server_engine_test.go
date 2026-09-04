package spdk

import (
	"context"
	"fmt"
	"strings"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

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
	fmt.Println("Testing Server.EngineDelete signals the restore frontend to stop its teardown retries and drops its entry so a recreated engine can restore again")

	prevEF := NewEngineFrontend("engine-a-restore", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 2), nil)

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
	c.Assert(len(srv.restoreFrontendMap), Equals, 0)

	// A recreated engine reuses the volume NQN, so a late teardown retry
	// must not run against it.
	select {
	case <-prevEF.stopCh:
	default:
		c.Fatal("EngineDelete did not signal the restore frontend to stop")
	}

	// Deleting the same engine again must not panic on the closed channel.
	srv.restoreFrontendMap["engine-a"] = prevEF
	_, err = srv.EngineDelete(context.Background(), &spdkrpc.EngineDeleteRequest{
		Name: "engine-a",
	})
	c.Assert(err, IsNil)
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
