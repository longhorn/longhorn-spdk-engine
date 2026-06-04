package spdk

import (
	"fmt"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

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
