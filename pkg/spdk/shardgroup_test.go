package spdk

import (
	"context"
	"fmt"

	. "gopkg.in/check.v1"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// TestCopyEcCountersFromBdevInfoMapping verifies that each of the 18 EC
// counters lands in its correct proto field. Distinct values per source
// field catch any cross-wiring at the helper boundary; a zero remaining on
// EcStatus after the copy means the helper missed that field.
func (s *TestSuite) TestCopyEcCountersFromBdevInfoMapping(c *C) {
	fmt.Println("Testing copyEcCountersFromBdevInfo maps all 18 EC counters")

	info := &spdktypes.BdevEcInfo{
		UnmapsSubmitted:            101,
		UnmapsCompleted:            102,
		UnmapsDeferredBusy:         103,
		UnmapsViaWriteZeros:        104,
		UnmapFanoutMisses:          105,
		UnmappedStripes:            106,
		DegradedReadEioDirty:       201,
		DegradedReadsReconstructed: 202,
		RmwTotal:                   301,
		RmwDeferredScrub:           302,
		RmwDeferredDirty:           303,
		RmwDeferredInflight:        304,
		FullStripeWrites:           401,
		FullStripeWritesDeferred:   402,
		UnmapsFailed:               501,
		UnmappedReadsSynthesized:   502,
		WritesIntoUnmapped:         503,
		WritesIntoUnmappedFailed:   504,
	}

	status := &spdkrpc.EcStatus{}
	copyEcCountersFromBdevInfo(status, info)

	// UNMAP fan-out
	c.Assert(status.UnmapsSubmitted, Equals, uint64(101))
	c.Assert(status.UnmapsCompleted, Equals, uint64(102))
	c.Assert(status.UnmapsDeferredBusy, Equals, uint64(103))
	c.Assert(status.UnmapsViaWriteZeros, Equals, uint64(104))
	c.Assert(status.UnmapFanoutMisses, Equals, uint64(105))
	c.Assert(status.UnmappedStripes, Equals, uint64(106))

	// Degraded reads - DegradedReadEioDirty is the alert-grade signal
	c.Assert(status.DegradedReadEioDirty, Equals, uint64(201))
	c.Assert(status.DegradedReadsReconstructed, Equals, uint64(202))

	// RMW pressure
	c.Assert(status.RmwTotal, Equals, uint64(301))
	c.Assert(status.RmwDeferredScrub, Equals, uint64(302))
	c.Assert(status.RmwDeferredDirty, Equals, uint64(303))
	c.Assert(status.RmwDeferredInflight, Equals, uint64(304))

	// Full-stripe writes
	c.Assert(status.FullStripeWrites, Equals, uint64(401))
	c.Assert(status.FullStripeWritesDeferred, Equals, uint64(402))

	// UNMAP and write-into-unmapped counters. WritesIntoUnmappedFailed is the
	// one worth alerting on: the write reached disk, but the stripe stayed
	// marked unmapped, so later reads return zeros instead of that data.
	c.Assert(status.UnmapsFailed, Equals, uint64(501))
	c.Assert(status.UnmappedReadsSynthesized, Equals, uint64(502))
	c.Assert(status.WritesIntoUnmapped, Equals, uint64(503))
	c.Assert(status.WritesIntoUnmappedFailed, Equals, uint64(504))
}

// TestCopyEcCountersFromBdevInfoZeroValues exercises the all-zero path
// (fresh array, no traffic yet) to confirm zero is propagated, not skipped.
func (s *TestSuite) TestCopyEcCountersFromBdevInfoZeroValues(c *C) {
	fmt.Println("Testing copyEcCountersFromBdevInfo preserves zero values")

	status := &spdkrpc.EcStatus{
		// Pre-populate with non-zero to verify the helper overwrites.
		UnmapsSubmitted:      999,
		DegradedReadEioDirty: 999,
	}
	info := &spdktypes.BdevEcInfo{}
	copyEcCountersFromBdevInfo(status, info)

	c.Assert(status.UnmapsSubmitted, Equals, uint64(0))
	c.Assert(status.DegradedReadEioDirty, Equals, uint64(0))
	c.Assert(status.RmwTotal, Equals, uint64(0))
	c.Assert(status.FullStripeWrites, Equals, uint64(0))
}

// TestEcUsableFitsSpecBoundary pins the exact equality boundary of the
// create-time capacity guard: usable == spec must pass (zero slack is the
// correctly-provisioned case), and any shortfall must fail.
func (s *TestSuite) TestEcUsableFitsSpecBoundary(c *C) {
	fmt.Println("Testing ecUsableFitsSpec equality boundary and shortfall")

	const blockSize = uint32(4096)
	const blocks = uint64(1000)
	usable := blocks * uint64(blockSize)

	// Exact fit passes (minimum slack for a correct volume is 0).
	c.Assert(ecUsableFitsSpec(blocks, blockSize, usable), Equals, true)
	// One byte short fails.
	c.Assert(ecUsableFitsSpec(blocks, blockSize, usable+1), Equals, false)
	// Comfortably oversized passes.
	c.Assert(ecUsableFitsSpec(blocks, blockSize, usable-uint64(blockSize)), Equals, true)
	// Large capacity (no uint overflow at realistic EC sizes).
	c.Assert(ecUsableFitsSpec(1<<40, blockSize, (1<<40)*uint64(blockSize)), Equals, true)
}

// TestEcUsableCreationCapBoundary pins the creation-cap boundary to the
// cluster-floored comparison SPDK and admission (ValidateECCreationSize)
// use: a partial cluster over the raw cap still passes; one whole cluster
// over fails.
func (s *TestSuite) TestEcUsableCreationCapBoundary(c *C) {
	fmt.Println("Testing ecUsableExceedsCreationCap cluster-floored boundary")

	cap := uint64(spdktypes.EcLvstoreMaxCreationSize)
	cluster := uint64(spdktypes.EcLvstoreClusterSize)

	// At the cap passes.
	c.Assert(ecUsableExceedsCreationCap(cap), Equals, false)
	// A partial cluster over passes: SPDK floors the device to clusters.
	c.Assert(ecUsableExceedsCreationCap(cap+cluster-1), Equals, false)
	// One whole cluster over fails.
	c.Assert(ecUsableExceedsCreationCap(cap+cluster), Equals, true)
}

// TestLvstoreUsableFitsSpecBoundary covers the equality boundary and the
// production failure geometry: the 33 GiB head needed 8448 clusters, but
// blobstore metadata left only 8439.
func (s *TestSuite) TestLvstoreUsableFitsSpecBoundary(c *C) {
	fmt.Println("Testing lvstoreUsableFitsSpec equality boundary and the production geometry")

	const clusterSize = uint64(4 << 20)
	const spec = uint64(33 << 30)

	// Exact fit passes: 8448 clusters back a 33 GiB head with zero slack.
	c.Assert(lvstoreUsableFitsSpec(8448, clusterSize, spec), Equals, true)
	// The production failure: blobstore metadata took 9 of the 8448 clusters.
	c.Assert(lvstoreUsableFitsSpec(8439, clusterSize, spec), Equals, false)
	// One byte short fails.
	c.Assert(lvstoreUsableFitsSpec(1000, clusterSize, 1000*clusterSize+1), Equals, false)
}

func (s *TestSuite) TestShardGroupExpandPreconditions(c *C) {
	const initialSize = uint64(4 << 20)

	cases := []struct {
		name     string
		state    types.InstanceState
		target   uint64
		wantErr  bool
		wantCode grpccodes.Code // checked only when not codes.OK
	}{
		{"rejects pending", types.InstanceStatePending, 8 << 20, true, grpccodes.FailedPrecondition},
		{"rejects stopped", types.InstanceStateStopped, 8 << 20, true, grpccodes.FailedPrecondition},
		{"rejects error", types.InstanceStateError, 8 << 20, true, grpccodes.FailedPrecondition},
		{"rejects unaligned size", types.InstanceStateRunning, 8<<20 + 1, true, grpccodes.OK},
		{"rejects shrink", types.InstanceStateRunning, 2 << 20, true, grpccodes.OK},
		// SpecSize is committed only after the full expansion chain succeeds,
		// so being at the target size means there is nothing to do.
		{"no-op at target size", types.InstanceStateRunning, initialSize, false, grpccodes.OK},
	}

	for _, tc := range cases {
		fmt.Println("Testing ShardGroup.Expand:", tc.name)

		sg := NewShardGroup(context.Background(), "sg-1", "vol-1", initialSize, 2, 1, 64,
			map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
		sg.State = tc.state

		err := sg.Expand(nil, tc.target, 0)
		if tc.wantErr {
			c.Assert(err, NotNil, Commentf("case=%s", tc.name))
			if tc.wantCode != grpccodes.OK {
				c.Assert(grpcstatus.Code(err), Equals, tc.wantCode, Commentf("case=%s", tc.name))
			}
		} else {
			c.Assert(err, IsNil, Commentf("case=%s", tc.name))
		}
		// SpecSize is never mutated on a rejected or no-op Expand.
		c.Assert(sg.SpecSize, Equals, initialSize, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestExceedsInPlaceGrowthCeilingBoundary(c *C) {
	fmt.Println("Testing exceedsInPlaceGrowthCeiling boundary and the unknown-creation-size fail-open")

	const creationSize = uint64(4 << 30)
	ceiling := uint64(spdktypes.EcLvstoreMaxGrowthFactor) * creationSize

	// Exactly 10x is allowed (ceiling is inclusive).
	c.Assert(exceedsInPlaceGrowthCeiling(ceiling, creationSize), Equals, false)
	// One MiB past the ceiling is rejected.
	c.Assert(exceedsInPlaceGrowthCeiling(ceiling+1<<20, creationSize), Equals, true)
	// Unknown creation size never rejects.
	c.Assert(exceedsInPlaceGrowthCeiling(1<<60, 0), Equals, false)
}

// TestShardGroupExpandGrowthCeilingPreflight verifies the ceiling rejection
// happens before any SPDK call: a nil spdkClient would panic otherwise.
func (s *TestSuite) TestShardGroupExpandGrowthCeilingPreflight(c *C) {
	fmt.Println("Testing ShardGroup.Expand rejects beyond-ceiling targets before any SPDK call")

	const creationSize = uint64(4 << 20)
	sg := NewShardGroup(context.Background(), "sg-1", "vol-1", creationSize, 2, 1, 64,
		map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
	sg.State = types.InstanceStateRunning

	overCeiling := uint64(spdktypes.EcLvstoreMaxGrowthFactor)*creationSize + 1<<20
	err := sg.Expand(nil, overCeiling, creationSize)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	// The volume is untouched and no update is broadcast.
	c.Assert(sg.SpecSize, Equals, creationSize)
	c.Assert(len(sg.UpdateCh), Equals, 0)
}

// TestShardGroupExpandPrecheckGrowthCeilingGate verifies the precheck rejects
// beyond-ceiling targets before any SPDK call: a nil spdkClient would panic
// otherwise.
func (s *TestSuite) TestShardGroupExpandPrecheckGrowthCeilingGate(c *C) {
	fmt.Println("Testing ShardGroup.ExpandPrecheck rejects beyond-ceiling targets before any SPDK call")

	const creationSize = uint64(4 << 20)
	sg := NewShardGroup(context.Background(), "sg-1", "vol-1", creationSize, 2, 1, 64,
		map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
	sg.State = types.InstanceStateRunning

	overCeiling := uint64(spdktypes.EcLvstoreMaxGrowthFactor)*creationSize + 1<<20
	required, err := sg.ExpandPrecheck(nil, overCeiling, creationSize)
	c.Assert(err, NotNil)
	c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
	c.Assert(required, Equals, false)

	// At or below the current size the precheck short-circuits to "no
	// expansion required" before the ceiling gate.
	required, err = sg.ExpandPrecheck(nil, creationSize, creationSize)
	c.Assert(err, IsNil)
	c.Assert(required, Equals, false)
}

func (s *TestSuite) TestShardGroupExpandDoesNotBroadcastWithoutSizeChange(c *C) {
	fmt.Println("Testing ShardGroup.Expand does not broadcast an update when SpecSize is unchanged")

	sg := NewShardGroup(context.Background(), "sg-1", "vol-1", 4<<20, 2, 1, 64,
		map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
	sg.State = types.InstanceStateRunning

	// No-op at target size and rejected shrink both leave SpecSize untouched.
	c.Assert(sg.Expand(nil, 4<<20, 0), IsNil)
	c.Assert(sg.Expand(nil, 2<<20, 0), NotNil)
	c.Assert(len(sg.UpdateCh), Equals, 0)
}

func (s *TestSuite) TestShardGroupSnapshotDeleteRejectsNonRunningState(c *C) {
	fmt.Println("Testing ShardGroup.SnapshotDelete rejects non-running states without broadcasting")

	for _, state := range []types.InstanceState{
		types.InstanceStatePending,
		types.InstanceStateStopped,
		types.InstanceStateError,
	} {
		sg := NewShardGroup(context.Background(), "sg-1", "vol-1", 4<<20, 2, 1, 64,
			map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
		sg.State = state

		err := sg.SnapshotDelete(nil, "snap-1")
		c.Assert(err, NotNil)
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
		c.Assert(len(sg.UpdateCh), Equals, 0)
	}
}
