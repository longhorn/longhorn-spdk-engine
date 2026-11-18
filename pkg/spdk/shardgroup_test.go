package spdk

import (
	"context"
	"fmt"

	. "gopkg.in/check.v1"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
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

// TestCopyEcHealthFromBdevInfo verifies that a failed slot reaches the proto
// response. The controller only marks a shard failed when it sees the slot
// state here.
func (s *TestSuite) TestCopyEcHealthFromBdevInfo(c *C) {
	fmt.Println("Testing copyEcHealthFromBdevInfo maps EC state and slot states")

	info := &spdktypes.BdevEcInfo{
		State:             spdktypes.BdevEcStateDegraded,
		RebuildInProgress: true,
		BaseBdevs: []spdktypes.EcBaseBdev{
			{Slot: 0, Role: spdktypes.BdevEcSlotRoleData, State: spdktypes.BdevEcSlotStateNormal, Name: "shard-0n1"},
			{Slot: 1, Role: spdktypes.BdevEcSlotRoleData, State: spdktypes.BdevEcSlotStateFailed, Name: "<failed>"},
			{Slot: 2, Role: spdktypes.BdevEcSlotRoleParity, State: spdktypes.BdevEcSlotStateReplacing, Name: "shard-2n1"},
		},
	}

	status := &spdkrpc.EcStatus{}
	copyEcHealthFromBdevInfo(status, info)

	c.Assert(status.State, Equals, spdkrpc.EcState_EC_STATE_DEGRADED)
	c.Assert(status.RebuildInProgress, Equals, true)
	c.Assert(status.RebuildProgress, IsNil)
	c.Assert(status.Slots, HasLen, 3)

	c.Assert(status.Slots[0].SlotIndex, Equals, uint32(0))
	c.Assert(status.Slots[0].Role, Equals, spdkrpc.EcSlotRole_EC_SLOT_ROLE_DATA)
	c.Assert(status.Slots[0].State, Equals, spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL)
	c.Assert(status.Slots[0].BdevName, Equals, "shard-0n1")

	c.Assert(status.Slots[1].SlotIndex, Equals, uint32(1))
	c.Assert(status.Slots[1].Role, Equals, spdkrpc.EcSlotRole_EC_SLOT_ROLE_DATA)
	c.Assert(status.Slots[1].State, Equals, spdkrpc.EcSlotState_EC_SLOT_STATE_FAILED)

	c.Assert(status.Slots[2].SlotIndex, Equals, uint32(2))
	c.Assert(status.Slots[2].Role, Equals, spdkrpc.EcSlotRole_EC_SLOT_ROLE_PARITY)
	c.Assert(status.Slots[2].State, Equals, spdkrpc.EcSlotState_EC_SLOT_STATE_REPLACING)
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

// TestDiscoveredSpecSize pins the resume rule for salvage after an
// interrupted expansion: a head smaller than the requested spec wins (the
// expansion must re-run from the head size); a head at or above the spec
// keeps the requested size (clean path or SPDK cluster rounding).
func (s *TestSuite) TestDiscoveredSpecSize(c *C) {
	fmt.Println("Testing discoveredSpecSize resume rule")

	const gib = uint64(1 << 30)

	// Interrupted expansion: head still at the old size.
	c.Assert(discoveredSpecSize(10*gib, 20*gib), Equals, 10*gib)
	// Clean path: head matches the spec.
	c.Assert(discoveredSpecSize(10*gib, 10*gib), Equals, 10*gib)
	// Cluster rounding: head slightly larger than the spec.
	c.Assert(discoveredSpecSize(10*gib+2*1024*1024, 10*gib), Equals, 10*gib)
	// One byte short is still a pending expansion.
	c.Assert(discoveredSpecSize(10*gib-1, 10*gib), Equals, 10*gib-1)
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

// newExposedLocalShard returns a Running, exposed shard as Shard.Create leaves
// it, without contacting SPDK.
func newExposedLocalShard() *Shard {
	shard := NewShard("vol-a", 0, "disk-a", "lvs-uuid-a", testCachedSpecSize, make(chan interface{}, 1))
	shard.UUID = "lvol-uuid-a"
	shard.IP = "10.0.0.1"
	shard.Port = 20001
	shard.State = types.InstanceStateRunning
	shard.IsExposed = true
	return shard
}

// mockShardExposeBdev replaces shardStopExposeBdev and shardStartExposeBdev
// with mocks. The mocks do not call SPDK. They record the NQN they are
// called with and return stopErr and startErr.
//
// Call the returned restore function at the end of the test to put the real
// functions back.
func mockShardExposeBdev(stopErr, startErr error) (stopped, started *[]string, restore func()) {
	origStop, origStart := shardStopExposeBdev, shardStartExposeBdev
	stopped, started = &[]string{}, &[]string{}
	shardStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		*stopped = append(*stopped, nqn)
		return stopErr
	}
	shardStartExposeBdev = func(_ *spdkclient.Client, nqn, _, _, _, _ string) error {
		*started = append(*started, nqn)
		return startErr
	}
	return stopped, started, func() {
		shardStopExposeBdev, shardStartExposeBdev = origStop, origStart
	}
}

// Attaching a local shard stops exposing it over NVMe-oF and returns its lvol
// alias. Detaching it exposes it again. Repeating either call does not call
// SPDK a second time.
//
// The test passes a nil spdkClient on purpose. If the code took the remote
// path, the first SPDK call would panic on the nil client.
func (s *TestSuite) TestLocalShardUnexposedWhileAttached(c *C) {
	fmt.Println("Testing a local shard is not exposed while it is attached to the ShardGroup")

	stopped, started, restore := mockShardExposeBdev(nil, nil)
	defer restore()

	sg := NewShardGroup(context.Background(), "vol-a", "vol-a", testCachedSpecSize,
		2, 1, 64, map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
	shard := newExposedLocalShard()
	endpoint := &ShardEndpoint{Address: "10.0.0.1:20001", SlotIndex: 0, LocalShard: shard}

	bdevName, err := sg.attachShardBaseBdev(nil, endpoint, maxRetries)
	c.Assert(err, IsNil)
	c.Assert(bdevName, Equals, "disk-a/shard-vol-a-0")
	c.Assert(shard.IsExposed, Equals, false)
	c.Assert(shard.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Assert(*stopped, DeepEquals, []string{shard.Nqn})

	// A second attach finds the export already removed and does not ask
	// SPDK again.
	_, err = sg.attachShardBaseBdev(nil, endpoint, maxRetries)
	c.Assert(err, IsNil)
	c.Assert(*stopped, HasLen, 1)

	c.Assert(sg.detachShardBaseBdev(nil, endpoint), IsNil)
	c.Assert(shard.IsExposed, Equals, true)
	c.Assert(*started, DeepEquals, []string{shard.Nqn})

	// A second detach finds the export already restored.
	c.Assert(sg.detachShardBaseBdev(nil, endpoint), IsNil)
	c.Assert(*started, HasLen, 1)
}

// A failed stop or start must leave IsExposed matching what SPDK still has.
// A failed stop leaves the shard Running and exposed. A failed start moves
// the shard to Error, because it would otherwise look healthy while
// unreachable.
func (s *TestSuite) TestLocalShardExposeFailure(c *C) {
	fmt.Println("Testing a failed stop or start of a local shard expose")

	stopped, _, restore := mockShardExposeBdev(fmt.Errorf("stop failed"), fmt.Errorf("start failed"))
	defer restore()

	shard := newExposedLocalShard()
	_, err := shard.StopExposeForLocalAttach(nil)
	c.Assert(err, NotNil)
	c.Assert(shard.IsExposed, Equals, true)
	c.Assert(shard.State, Equals, types.InstanceState(types.InstanceStateRunning))

	shard.IsExposed = false
	c.Assert(shard.StartExposeAfterLocalDetach(nil), NotNil)
	c.Assert(shard.IsExposed, Equals, false)
	c.Assert(shard.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Assert(shard.ErrorMsg, Not(Equals), "")
	// The failed start removes the subsystem it may have left behind.
	c.Assert(*stopped, DeepEquals, []string{shard.Nqn, shard.Nqn})
}

// Only a Running shard can be attached locally, and only a Running shard gets
// its export restored: a stopped or failed shard has no port to expose on.
func (s *TestSuite) TestLocalShardExposeRequiresRunning(c *C) {
	fmt.Println("Testing only a Running local shard can be attached and exposed again")

	stopped, started, restore := mockShardExposeBdev(nil, nil)
	defer restore()

	for _, state := range []types.InstanceState{
		types.InstanceStatePending,
		types.InstanceStateStopped,
		types.InstanceStateError,
		types.InstanceStateTerminating,
	} {
		shard := newExposedLocalShard()
		shard.State = state

		_, err := shard.StopExposeForLocalAttach(nil)
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
		c.Assert(shard.IsExposed, Equals, true)

		shard.IsExposed = false
		c.Assert(shard.StartExposeAfterLocalDetach(nil), IsNil)
		c.Assert(shard.IsExposed, Equals, false)
	}
	c.Assert(*stopped, HasLen, 0)
	c.Assert(*started, HasLen, 0)
}

// newExposedLocalShardGroup returns a Running ShardGroup with an exposed head
// lvol, as ShardGroup.Create leaves it, without contacting SPDK.
func newExposedLocalShardGroup() *ShardGroup {
	sg := NewShardGroup(context.Background(), "vol-a", "vol-a", testCachedSpecSize,
		2, 1, 64, map[string]*ShardEndpoint{}, false, make(chan interface{}, 1))
	sg.HeadLvolUUID = "head-uuid-a"
	sg.IP = "10.0.0.1"
	sg.Port = 20002
	sg.State = types.InstanceStateRunning
	sg.IsExposed = true
	return sg
}

// mockShardGroupExposeBdev replaces shardGroupStopExposeBdev and
// shardGroupStartExposeBdev with mocks. The mocks do not call SPDK. They
// record the NQN they are called with and return stopErr and startErr.
//
// Call the returned restore function at the end of the test to put the real
// functions back.
func mockShardGroupExposeBdev(stopErr, startErr error) (stopped, started *[]string, restore func()) {
	origStop, origStart := shardGroupStopExposeBdev, shardGroupStartExposeBdev
	stopped, started = &[]string{}, &[]string{}
	shardGroupStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		*stopped = append(*stopped, nqn)
		return stopErr
	}
	shardGroupStartExposeBdev = func(_ *spdkclient.Client, nqn, _, _, _, _ string) error {
		*started = append(*started, nqn)
		return startErr
	}
	return stopped, started, func() {
		shardGroupStopExposeBdev, shardGroupStartExposeBdev = origStop, origStart
	}
}

// Attaching a local engine stops exposing the head lvol and returns its
// alias. The ShardGroup stays Running and keeps its port and NQN, because the
// manager needs them to start the engine. Detaching the engine exposes the
// head lvol again. Repeating either call does not call SPDK a second time.
func (s *TestSuite) TestLocalHeadLvolUnexposedWhileEngineAttached(c *C) {
	fmt.Println("Testing a local head lvol is not exposed while an engine uses it directly")

	stopped, started, restore := mockShardGroupExposeBdev(nil, nil)
	defer restore()

	sg := newExposedLocalShardGroup()
	nqn, port := sg.Nqn, sg.Port

	headAlias, err := sg.StopExposeForLocalAttach(nil)
	c.Assert(err, IsNil)
	c.Assert(headAlias, Not(Equals), "")
	c.Assert(headAlias, Equals, sg.HeadAlias)
	c.Assert(sg.IsExposed, Equals, false)
	c.Assert(sg.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Assert(sg.Nqn, Equals, nqn)
	c.Assert(sg.Port, Equals, port)
	c.Assert(*stopped, DeepEquals, []string{nqn})

	_, err = sg.StopExposeForLocalAttach(nil)
	c.Assert(err, IsNil)
	c.Assert(*stopped, HasLen, 1)

	c.Assert(sg.StartExposeAfterLocalDetach(nil), IsNil)
	c.Assert(sg.IsExposed, Equals, true)
	c.Assert(*started, DeepEquals, []string{nqn})

	c.Assert(sg.StartExposeAfterLocalDetach(nil), IsNil)
	c.Assert(*started, HasLen, 1)
}

// A failed stop leaves the head lvol exposed. A failed start leaves the
// ShardGroup Running and unexposed, and removes the subsystem that the failed
// expose may have left behind. The ShardGroup must not move to Error, because
// the manager never recreates a ShardGroup in Error.
func (s *TestSuite) TestLocalHeadLvolExposeFailure(c *C) {
	fmt.Println("Testing a failed stop or start of a local head lvol expose")

	_, _, restore := mockShardGroupExposeBdev(fmt.Errorf("stop failed"), nil)
	sg := newExposedLocalShardGroup()
	_, err := sg.StopExposeForLocalAttach(nil)
	restore()
	c.Assert(err, NotNil)
	c.Assert(sg.IsExposed, Equals, true)
	c.Assert(sg.State, Equals, types.InstanceState(types.InstanceStateRunning))

	stopped, _, restore := mockShardGroupExposeBdev(nil, fmt.Errorf("start failed"))
	defer restore()
	sg.IsExposed = false
	c.Assert(sg.StartExposeAfterLocalDetach(nil), NotNil)
	c.Assert(sg.IsExposed, Equals, false)
	c.Assert(sg.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Assert(*stopped, DeepEquals, []string{sg.Nqn})
}

// Only a Running ShardGroup can be attached locally, and only a Running
// ShardGroup gets its head lvol exposed again.
func (s *TestSuite) TestLocalHeadLvolExposeRequiresRunning(c *C) {
	fmt.Println("Testing only a Running ShardGroup can be attached locally and exposed again")

	stopped, started, restore := mockShardGroupExposeBdev(nil, nil)
	defer restore()

	for _, state := range []types.InstanceState{
		types.InstanceStatePending,
		types.InstanceStateStopped,
		types.InstanceStateError,
		types.InstanceStateTerminating,
	} {
		sg := newExposedLocalShardGroup()
		sg.State = state

		_, err := sg.StopExposeForLocalAttach(nil)
		c.Assert(grpcstatus.Code(err), Equals, grpccodes.FailedPrecondition)
		c.Assert(sg.IsExposed, Equals, true)

		sg.IsExposed = false
		c.Assert(sg.StartExposeAfterLocalDetach(nil), IsNil)
		c.Assert(sg.IsExposed, Equals, false)
	}
	c.Assert(*stopped, HasLen, 0)
	c.Assert(*started, HasLen, 0)
}
