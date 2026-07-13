package spdk

import (
	"context"
	"fmt"

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
