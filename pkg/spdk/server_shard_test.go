package spdk

import (
	"context"
	"fmt"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// newDetachableShard builds a non-exposed, port-less shard so Shard.Delete with
// cleanupRequired=false short-circuits before touching SPDK or the port
// allocator. A UUID is set to stand in for the on-disk lvol that detach must
// preserve.
func newDetachableShard(updateCh chan interface{}, state lhtypes.InstanceState) *Shard {
	shard := NewShard("vol-a", 0, "lvs-a", "lvs-uuid-a", 100*1024*1024, updateCh)
	shard.State = state
	shard.UUID = "uuid-a"
	return shard
}

// A repeated ShardCreate with the same unaligned size must reuse the
// cached shard. NewShard rounds the size up to MiB. If the request is
// not rounded too, every retry compares the raw size against the rounded
// one and fails.
func (s *TestSuite) TestGetOrCreateShardReuseAcceptsUnalignedSize(c *C) {
	fmt.Println("Testing Server.getOrCreateShard reuse-path accepts an identical unaligned size")

	// 100,000,000 bytes is 100 MB (decimal), which is NOT a multiple of MiB
	// (1048576). RoundUp will lift it to the next MiB boundary.
	const unalignedSize = uint64(100_000_000)

	srv := &Server{
		shardMap: map[string]*Shard{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeShard: make(chan interface{}, 1),
		},
	}

	req := &spdkrpc.ShardCreateRequest{
		VolumeName: "vol-a",
		SlotIndex:  0,
		SizeBytes:  unalignedSize,
		LvsName:    "lvs-a",
		LvsUuid:    "lvs-uuid-a",
	}

	// There is no live SPDK here, so seed shardMap directly with NewShard
	// (which rounds the size up to MiB). The next call then only runs the
	// reuse check.
	first := NewShard(req.VolumeName, req.SlotIndex, req.LvsName, req.LvsUuid, req.SizeBytes, srv.updateChs[lhtypes.InstanceTypeShard])
	srv.shardMap[first.Name] = first

	// The same unaligned size must reuse the cached shard, not fail.
	got, err := srv.getOrCreateShard(req)
	c.Assert(err, IsNil)
	c.Assert(got, NotNil)
	c.Assert(got, Equals, first)
}

// TestGetOrCreateShardReuseRejectsDifferentSize covers the converse: a
// genuinely different size on reuse is still rejected. This is the guard
// the size normalization preserves.
func (s *TestSuite) TestGetOrCreateShardReuseRejectsDifferentSize(c *C) {
	fmt.Println("Testing Server.getOrCreateShard reuse-path rejects a different size")

	srv := &Server{
		shardMap: map[string]*Shard{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeShard: make(chan interface{}, 1),
		},
	}

	first := NewShard("vol-a", 0, "lvs-a", "lvs-uuid-a", 100*1024*1024, srv.updateChs[lhtypes.InstanceTypeShard])
	srv.shardMap[first.Name] = first

	req := &spdkrpc.ShardCreateRequest{
		VolumeName: "vol-a",
		SlotIndex:  0,
		SizeBytes:  200 * 1024 * 1024, // 200 MiB, different from cached 100 MiB
		LvsName:    "lvs-a",
		LvsUuid:    "lvs-uuid-a",
	}

	_, err := srv.getOrCreateShard(req)
	c.Assert(err, NotNil)
}

// A healthy detach (cleanupRequired=false) leaves the shard Stopped with its
// lvol preserved, so a later ShardCreate can re-attach to it.
func (s *TestSuite) TestShardDeleteDetachLeavesHealthyShardStopped(c *C) {
	fmt.Println("Testing Shard.Delete detach leaves a healthy shard Stopped with its lvol preserved")

	shard := newDetachableShard(make(chan interface{}, 1), lhtypes.InstanceStateRunning)

	err := shard.Delete(nil, false, nil)
	c.Assert(err, IsNil)
	c.Assert(string(shard.State), Equals, string(lhtypes.InstanceStateStopped))
	c.Assert(shard.UUID, Equals, "uuid-a")
}

// An Error shard keeps its Error state and message across detach. Error is
// terminal by the Expand contract; laundering it to Stopped would let a stale
// cached size slip past the ShardCreate boundary check on re-attach.
func (s *TestSuite) TestShardDeleteDetachPreservesErrorState(c *C) {
	fmt.Println("Testing Shard.Delete detach preserves Error state instead of laundering it to Stopped")

	shard := newDetachableShard(make(chan interface{}, 1), lhtypes.InstanceStateError)
	shard.ErrorMsg = "failed expand"

	err := shard.Delete(nil, false, nil)
	c.Assert(err, IsNil)
	c.Assert(string(shard.State), Equals, string(lhtypes.InstanceStateError))
	c.Assert(shard.ErrorMsg, Equals, "failed expand")
	c.Assert(shard.UUID, Equals, "uuid-a")
}

// On detach the server keeps the cached shard in the map and does not bump the
// generation, so the record survives for the re-attach ShardCreate to reuse.
func (s *TestSuite) TestServerShardDeleteDetachRetainsShard(c *C) {
	fmt.Println("Testing Server.ShardDelete detach retains the cached shard without bumping the generation")

	srv := &Server{
		shardMap: map[string]*Shard{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeShard: make(chan interface{}, 1),
		},
	}
	shard := newDetachableShard(srv.updateChs[lhtypes.InstanceTypeShard], lhtypes.InstanceStateRunning)
	srv.shardMap[shard.Name] = shard
	genBefore := srv.shardMapGen

	_, err := srv.ShardDelete(context.Background(), &spdkrpc.ShardDeleteRequest{
		Name:            shard.Name,
		CleanupRequired: false,
	})
	c.Assert(err, IsNil)
	c.Assert(srv.shardMap[shard.Name], NotNil)
	c.Assert(string(srv.shardMap[shard.Name].State), Equals, string(lhtypes.InstanceStateStopped))
	c.Assert(srv.shardMapGen, Equals, genBefore)
}

// A cleanup delete of an already-absent shard is a no-op: it must not bump the
// generation, otherwise an idempotent re-delete triggers a wasted verify cycle.
func (s *TestSuite) TestServerShardDeleteAbsentShardDoesNotBumpGeneration(c *C) {
	fmt.Println("Testing Server.ShardDelete of an absent shard does not bump the generation")

	srv := &Server{
		shardMap: map[string]*Shard{},
		updateChs: map[lhtypes.InstanceType]chan interface{}{
			lhtypes.InstanceTypeShard: make(chan interface{}, 1),
		},
	}
	genBefore := srv.shardMapGen

	_, err := srv.ShardDelete(context.Background(), &spdkrpc.ShardDeleteRequest{
		Name:            "vol-a-0",
		CleanupRequired: true,
	})
	c.Assert(err, IsNil)
	c.Assert(srv.shardMapGen, Equals, genBefore)
}
