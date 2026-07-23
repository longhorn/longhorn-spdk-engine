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
