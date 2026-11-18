package spdk

import (
	"context"
	"fmt"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

const (
	testCachedSpecSize = uint64(100 * 1024 * 1024)
	testGrownSpecSize  = uint64(200 * 1024 * 1024)
)

func newShardGroupReuseFixture(state lhtypes.InstanceState) (*Server, *ShardGroup) {
	updateCh := make(chan interface{}, 1)
	sg := NewShardGroup(context.Background(), "vol-a", "vol-a", testCachedSpecSize,
		2, 1, 64, map[string]*ShardEndpoint{}, false, updateCh)
	sg.State = state

	srv := &Server{
		shardGroupMap: map[string]*ShardGroup{sg.Name: sg},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeShardGroup: updateCh,
		},
	}
	return srv, sg
}

func newShardGroupCreateRequest(specSize uint64) *spdkrpc.ShardGroupCreateRequest {
	return &spdkrpc.ShardGroupCreateRequest{
		Name:       "vol-a",
		VolumeName: "vol-a",
		SpecSize:   specSize,
		Spec: &spdkrpc.ShardGroupSpec{
			DataChunks:   2,
			ParityChunks: 1,
			StripSizeKb:  64,
			Shards:       map[string]*spdkrpc.ShardEndpoint{},
		},
	}
}

// A larger requested size on a stopped record means re-attach after an
// interrupted expansion: the manager committed the new size but the cached
// record still holds the old one. Rejecting it would leave the volume
// unattachable until the process restarts.
func (s *TestSuite) TestGetOrCreateShardGroupStoppedReuseAdoptsGrownSpecSize(c *C) {
	fmt.Println("Testing Server.getOrCreateShardGroup adopts a grown SpecSize on a stopped record")

	srv, sg := newShardGroupReuseFixture(lhtypes.InstanceStateStopped)

	got, err := srv.getOrCreateShardGroup(newShardGroupCreateRequest(testGrownSpecSize))
	c.Assert(err, IsNil)
	c.Assert(got, Equals, sg)
	c.Assert(got.SpecSize, Equals, testGrownSpecSize)
}

// Shrinking is never legitimate; the corrupted-state tripwire must hold.
func (s *TestSuite) TestGetOrCreateShardGroupStoppedReuseRejectsShrunkSpecSize(c *C) {
	fmt.Println("Testing Server.getOrCreateShardGroup rejects a shrunk SpecSize on a stopped record")

	srv, sg := newShardGroupReuseFixture(lhtypes.InstanceStateStopped)
	sg.SpecSize = testGrownSpecSize

	_, err := srv.getOrCreateShardGroup(newShardGroupCreateRequest(testCachedSpecSize))
	c.Assert(err, NotNil)
	c.Assert(sg.SpecSize, Equals, testGrownSpecSize)
}

// A size mismatch on a non-stopped record is not the re-attach path; resizing
// a live shardgroup goes through Expand, so ShardGroupCreate must reject it.
func (s *TestSuite) TestGetOrCreateShardGroupRunningReuseRejectsGrownSpecSize(c *C) {
	fmt.Println("Testing Server.getOrCreateShardGroup rejects a grown SpecSize on a running record")

	srv, sg := newShardGroupReuseFixture(lhtypes.InstanceStateRunning)

	_, err := srv.getOrCreateShardGroup(newShardGroupCreateRequest(testGrownSpecSize))
	c.Assert(err, NotNil)
	c.Assert(sg.SpecSize, Equals, testCachedSpecSize)
}

// A ShardGroup that is not Pending or Stopped may still be attached to its
// shards, so a create request must not change its shards or salvage flag.
func (s *TestSuite) TestGetOrCreateShardGroupKeepsShardsWhenNotPendingOrStopped(c *C) {
	fmt.Println("Testing Server.getOrCreateShardGroup keeps the shards and salvage flag of a ShardGroup that is not Pending or Stopped")

	for _, state := range []lhtypes.InstanceState{lhtypes.InstanceStateRunning, lhtypes.InstanceStateError} {
		srv, sg := newShardGroupReuseFixture(state)
		sg.Shards = map[string]*ShardEndpoint{"vol-a-0": {Address: "10.0.0.1:20001", SlotIndex: 0}}

		req := newShardGroupCreateRequest(testCachedSpecSize)
		req.Spec.SalvageRequested = true
		req.Spec.Shards = map[string]*spdkrpc.ShardEndpoint{"vol-a-0": {Address: "10.0.0.2:20001", SlotIndex: 0}}

		got, err := srv.getOrCreateShardGroup(req)
		c.Assert(err, IsNil, Commentf("state %v", state))
		c.Assert(got, Equals, sg, Commentf("state %v", state))
		c.Assert(sg.Shards["vol-a-0"].Address, Equals, "10.0.0.1:20001", Commentf("state %v", state))
		c.Assert(sg.SalvageRequested, Equals, false, Commentf("state %v", state))
	}
}

// A shard that runs in this Server and is Running is recorded as LocalShard.
// A shard that is absent or not Running keeps LocalShard nil, so the
// ShardGroup attaches it over NVMe-oF.
func (s *TestSuite) TestGetOrCreateShardGroupResolvesLocalShards(c *C) {
	fmt.Println("Testing Server.getOrCreateShardGroup marks shards served by this process as local")

	srv, _ := newShardGroupReuseFixture(lhtypes.InstanceStateStopped)
	srv.shardMap = map[string]*Shard{
		"vol-a-0": {Name: "vol-a-0", Alias: "vol-a-0-lvs/shard-vol-a-0", IP: "10.0.0.1", Port: 20001, State: lhtypes.InstanceStateRunning},
		"vol-a-1": {Name: "vol-a-1", Alias: "vol-a-1-lvs/shard-vol-a-1", IP: "10.0.0.1", Port: 20002, State: lhtypes.InstanceStateStopped},
	}

	req := newShardGroupCreateRequest(testCachedSpecSize)
	req.Spec.Shards = map[string]*spdkrpc.ShardEndpoint{
		"vol-a-0": {Address: "10.0.0.1:20001", SlotIndex: 0},
		"vol-a-1": {Address: "10.0.0.1:20002", SlotIndex: 1},
		"vol-a-2": {Address: "10.0.0.2:20003", SlotIndex: 2},
	}

	got, err := srv.getOrCreateShardGroup(req)
	c.Assert(err, IsNil)
	c.Assert(got.Shards, HasLen, 3)
	c.Assert(got.Shards["vol-a-0"].LocalShard, Equals, srv.shardMap["vol-a-0"])
	c.Assert(got.Shards["vol-a-0"].Address, Equals, "10.0.0.1:20001")
	c.Assert(got.Shards["vol-a-0"].SlotIndex, Equals, uint32(0))
	c.Assert(got.Shards["vol-a-1"].LocalShard, IsNil)
	c.Assert(got.Shards["vol-a-2"].LocalShard, IsNil)
}

// Only a Running shard served by this process at the requested address
// resolves to itself. Anything else resolves to nil, which means the
// ShardGroup attaches it over NVMe-oF.
func (s *TestSuite) TestResolveLocalShard(c *C) {
	fmt.Println("Testing Server.resolveLocalShard resolves only Running shards served by this process at the requested address")

	srv := &Server{
		shardMap: map[string]*Shard{
			"vol-a-0": {Name: "vol-a-0", Alias: "vol-a-0-lvs/shard-vol-a-0", IP: "10.0.0.1", Port: 20001, State: lhtypes.InstanceStateRunning},
			"vol-a-1": {Name: "vol-a-1", Alias: "vol-a-1-lvs/shard-vol-a-1", IP: "10.0.0.1", Port: 20002, State: lhtypes.InstanceStateStopped},
			"vol-a-2": {Name: "vol-a-2", Alias: "vol-a-2-lvs/shard-vol-a-2", IP: "10.0.0.1", Port: 20003, State: lhtypes.InstanceStateError},
		},
	}

	c.Assert(srv.resolveLocalShard("vol-a-0", "10.0.0.1:20001"), Equals, srv.shardMap["vol-a-0"])
	// Same name, but the request points to a replacement on another node.
	c.Assert(srv.resolveLocalShard("vol-a-0", "10.0.0.2:20001"), IsNil)
	c.Assert(srv.resolveLocalShard("vol-a-1", "10.0.0.1:20002"), IsNil)
	c.Assert(srv.resolveLocalShard("vol-a-2", "10.0.0.1:20003"), IsNil)
	c.Assert(srv.resolveLocalShard("vol-a-3", "10.0.0.2:20004"), IsNil)
}
