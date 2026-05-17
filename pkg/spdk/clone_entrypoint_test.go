package spdk

import (
	"fmt"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestReplica creates a minimal Replica in InstanceStatePending, suitable
// for construct()-based unit tests.
func newTestReplica(name, lvsName string) *Replica {
	return &Replica{
		Name:               name,
		LvsName:            lvsName,
		State:              types.InstanceState(types.InstanceStatePending),
		cloneEntrypointMap: map[string]*CloneEntrypointInfo{},
		log:                safelog.NewSafeLogger(logrus.WithField("test", name)),
	}
}

// newTestRunningCloneReplica creates a Running clone replica suitable for
// syncCloneReplicaInfo unit tests.  ActiveChain[0] is nil (entrypoint removed)
// and ActiveChain[1] is the chain root snapshot.
func newTestRunningCloneReplica(dstName, srcName, lvsName, epLvolName, rootSnapName string) *Replica {
	return &Replica{
		Name:                    dstName,
		LvsName:                 lvsName,
		State:                   types.InstanceState(types.InstanceStateRunning),
		isCloneReplica:          true,
		cloneSourceReplicaName:  srcName,
		cloneEntrypointLvolName: epLvolName,
		ActiveChain: []*Lvol{
			nil, // entrypoint not in chain (already removed from SPDK)
			{Name: rootSnapName, Alias: lvsName + "/" + rootSnapName},
		},
		cloneEntrypointMap: map[string]*CloneEntrypointInfo{},
		SnapshotLvolMap:    map[string]*Lvol{},
		log:                safelog.NewSafeLogger(logrus.WithField("test", dstName)),
	}
}

// makeBdevLvol is a test helper that creates a minimal BdevInfo suitable for
// constructActiveChainFromSnapshotLvolMap. The alias is set to lvsName/lvolName.
func makeBdevLvol(lvsName, lvolName, baseSnapshot string, clones []string) *spdktypes.BdevInfo {
	return &spdktypes.BdevInfo{
		BdevInfoBasic: spdktypes.BdevInfoBasic{
			Aliases:   []string{spdktypes.GetLvolAlias(lvsName, lvolName)},
			BlockSize: 512,
			NumBlocks: 2048,
			UUID:      fmt.Sprintf("uuid-%s", lvolName),
		},
		DriverSpecific: &spdktypes.BdevDriverSpecific{
			Lvol: &spdktypes.BdevDriverSpecificLvol{
				BaseSnapshot: baseSnapshot,
				Clones:       clones,
				Xattrs:       map[string]string{},
			},
		},
	}
}

// buildCloneReplicaBdevLvolMap builds the minimal bdevLvolMap needed by
// construct() for a dst (clone) replica whose ActiveChain[0] is epLvolName.
func buildCloneReplicaBdevLvolMap(lvsName, dstReplicaName, rootSnapName, epLvolName string) map[string]*spdktypes.BdevInfo {
	return map[string]*spdktypes.BdevInfo{
		dstReplicaName: makeBdevLvol(lvsName, dstReplicaName, rootSnapName, nil),
		rootSnapName:   makeBdevLvol(lvsName, rootSnapName, epLvolName, []string{dstReplicaName}),
		epLvolName:     makeBdevLvol(lvsName, epLvolName, "", []string{rootSnapName}),
	}
}

// buildSrcOnlyBdevLvolMap builds a bdevLvolMap for a src replica that has no
// snapshots of its own, plus any extra entries (e.g. entrypoint lvols).
func buildSrcOnlyBdevLvolMap(lvsName, srcReplicaName string, extras map[string]*spdktypes.BdevInfo) map[string]*spdktypes.BdevInfo {
	m := map[string]*spdktypes.BdevInfo{
		srcReplicaName: makeBdevLvol(lvsName, srcReplicaName, "", nil),
	}
	for k, v := range extras {
		m[k] = v
	}
	return m
}

// ---------------------------------------------------------------------------
// Naming utilities
// ---------------------------------------------------------------------------

func (s *TestSuite) TestCloneEntrypointNaming(c *C) {
	replicaName := "pvc-abc-r-12345678"
	snapshotName := "snap-1"

	epName := GetCloneEntrypointLvolName(replicaName, snapshotName)
	c.Assert(epName, Equals, fmt.Sprintf("%s-clone-ep-%s", replicaName, snapshotName))

	tmpHeadName := GetCloneEntrypointTmpHeadLvolName(replicaName, snapshotName)
	c.Assert(tmpHeadName, Equals, fmt.Sprintf("%s-tmp-head", epName))

	c.Assert(IsCloneEntrypointLvol(epName), Equals, true)
	c.Assert(IsCloneEntrypointLvol(tmpHeadName), Equals, false)
	c.Assert(IsCloneEntrypointTmpHeadLvol(tmpHeadName), Equals, true)
	c.Assert(IsCloneEntrypointTmpHeadLvol(epName), Equals, false)

	c.Assert(IsCloneEntrypointOfReplica(replicaName, epName), Equals, true)
	c.Assert(IsCloneEntrypointOfReplica(replicaName, tmpHeadName), Equals, false)
	c.Assert(IsCloneEntrypointOfReplica("other-replica", epName), Equals, false)

	extractedSnap := GetSnapshotNameFromCloneEntrypointLvolName(replicaName, epName)
	c.Assert(extractedSnap, Equals, snapshotName)

	extractedReplica := GetSourceReplicaNameFromCloneEntrypointLvolName(epName)
	c.Assert(extractedReplica, Equals, replicaName)

	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("clone-replica"), Equals, "clone-replica")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("clone-replica-snap-s1"), Equals, "clone-replica")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("pvc-abc-r-12345678-snap-snap1"), Equals, "pvc-abc-r-12345678")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("pvc-abc-r-12345678"), Equals, "pvc-abc-r-12345678")
}

// ---------------------------------------------------------------------------
// construct()
// ---------------------------------------------------------------------------

// TestConstruct verifies construct() behaviour across the three replica archetypes:
//
//   - Part A: dst clone replica — recognises clone entrypoint, sets metadata
//   - Part B: replica whose root snapshot parent is a backing image — not a clone
//   - Part C: src replica — populates cloneEntrypointMap from entrypoint lvols
func (s *TestSuite) TestConstruct(c *C) {
	// Part A: dst clone replica — construct sets isCloneReplica, epLvol in
	// ActiveChain[0], Stopped state.
	{
		srcReplicaName := "src-r-00000001"
		dstReplicaName := "dst-r-00000001"
		snapshotName := "snap-1"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)

		r := newTestReplica(dstReplicaName, "test-disk")
		bdevLvolMap := buildCloneReplicaBdevLvolMap("test-disk", dstReplicaName, rootSnapName, epLvolName)

		err := r.construct(bdevLvolMap)
		c.Assert(err, IsNil)
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateStopped))
		c.Assert(r.ErrorMsg, Equals, "")
		c.Assert(r.isCloneReplica, Equals, true)
		c.Assert(r.cloneEntrypointLvolName, Equals, epLvolName)
		c.Assert(r.cloneSourceReplicaName, Equals, srcReplicaName)
		c.Assert(r.ActiveChain[0], NotNil)
		c.Assert(r.ActiveChain[0].Name, Equals, epLvolName)
	}

	// Part B: replica with backing image parent — NOT treated as clone.
	{
		replicaName := "dst-r-00000001"
		rootSnapName := GetReplicaSnapshotLvolName(replicaName, "snap-1")
		biLvolName := GetBackingImageSnapLvolName("my-backing-image", "disk-uuid-1")

		c.Assert(types.IsBackingImageSnapLvolName(biLvolName), Equals, true)
		c.Assert(IsCloneEntrypointLvol(biLvolName), Equals, false)

		r := newTestReplica(replicaName, "test-disk")
		bdevLvolMap := map[string]*spdktypes.BdevInfo{
			replicaName:  makeBdevLvol("test-disk", replicaName, rootSnapName, nil),
			rootSnapName: makeBdevLvol("test-disk", rootSnapName, biLvolName, []string{replicaName}),
			biLvolName:   makeBdevLvol("test-disk", biLvolName, "", []string{rootSnapName}),
		}

		err := r.construct(bdevLvolMap)
		c.Assert(err, IsNil)
		c.Assert(r.isCloneReplica, Equals, false)
		c.Assert(r.cloneEntrypointLvolName, Equals, "")
		c.Assert(r.cloneSourceReplicaName, Equals, "")
	}

	// Part C: src replica — cloneEntrypointMap populated correctly.
	// tmp-head children are excluded from CloneReplicas; multiple entrypoints
	// across different snapshots are each discovered; CloneReplicas counts and
	// SnapshotName/SnapshotLvolName fields are correct.
	{
		srcReplicaName := "src-r-00000001"
		snap1, snap2 := "snap-1", "snap-2"
		ep1LvolName := GetCloneEntrypointLvolName(srcReplicaName, snap1)
		ep2LvolName := GetCloneEntrypointLvolName(srcReplicaName, snap2)
		tmpHeadLvolName := GetCloneEntrypointTmpHeadLvolName(srcReplicaName, snap1)

		r := newTestReplica(srcReplicaName, "test-disk")
		bdevLvolMap := buildSrcOnlyBdevLvolMap("test-disk", srcReplicaName, map[string]*spdktypes.BdevInfo{
			ep1LvolName: {
				DriverSpecific: &spdktypes.BdevDriverSpecific{
					Lvol: &spdktypes.BdevDriverSpecificLvol{
						Clones: []string{
							tmpHeadLvolName,
							"dst-r-00000001-snap-snap-1",
							"dst-r-00000002-snap-snap-1",
						},
					},
				},
			},
			ep2LvolName: {
				DriverSpecific: &spdktypes.BdevDriverSpecific{
					Lvol: &spdktypes.BdevDriverSpecificLvol{
						Clones: []string{
							"clone-b-r-00000001-snap-snap-2",
							"clone-c-r-00000001-snap-snap-2",
						},
					},
				},
			},
		})

		err := r.construct(bdevLvolMap)
		c.Assert(err, IsNil)
		c.Assert(len(r.cloneEntrypointMap), Equals, 2)

		// ep1: tmp-head excluded; both real clone children present
		ep1Info := r.cloneEntrypointMap[ep1LvolName]
		c.Assert(ep1Info, NotNil)
		c.Assert(ep1Info.SnapshotName, Equals, snap1)
		c.Assert(ep1Info.SnapshotLvolName, Equals, GetReplicaSnapshotLvolName(srcReplicaName, snap1))
		c.Assert(len(ep1Info.CloneReplicas), Equals, 2)
		c.Assert(ep1Info.CloneReplicas["dst-r-00000001"], Equals, true)
		c.Assert(ep1Info.CloneReplicas["dst-r-00000002"], Equals, true)

		// ep2: two clone replicas, correct metadata
		ep2Info := r.cloneEntrypointMap[ep2LvolName]
		c.Assert(ep2Info, NotNil)
		c.Assert(ep2Info.SnapshotName, Equals, snap2)
		c.Assert(ep2Info.SnapshotLvolName, Equals, GetReplicaSnapshotLvolName(srcReplicaName, snap2))
		c.Assert(len(ep2Info.CloneReplicas), Equals, 2)
		c.Assert(ep2Info.CloneReplicas["clone-b-r-00000001"], Equals, true)
		c.Assert(ep2Info.CloneReplicas["clone-c-r-00000001"], Equals, true)
	}
}

// ---------------------------------------------------------------------------
// constructActiveChainFromSnapshotLvolMap() — Clone replica filter out other lvols for EntryPoint Children
// ---------------------------------------------------------------------------

func (s *TestSuite) TestConstructActiveChainFiltersChildren(c *C) {
	lvsName := "test-disk"
	srcReplicaName := "src-r-00000001"
	snapshotName := "snap-1"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)

	replicaA := "clone-a-r-00000001"
	replicaB := "clone-b-r-00000001"
	rootSnapA := GetReplicaSnapshotLvolName(replicaA, snapshotName)
	rootSnapB := GetReplicaSnapshotLvolName(replicaB, snapshotName)

	// The clone entrypoint has both replicas' root snapshot lvols as clones.
	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		epLvolName: makeBdevLvol(lvsName, epLvolName, "", []string{rootSnapA, rootSnapB}),
		replicaA:   makeBdevLvol(lvsName, replicaA, rootSnapA, nil),
		replicaB:   makeBdevLvol(lvsName, replicaB, rootSnapB, nil),
	}

	// snapshotLvolMap: each replica has a root snapshot whose Parent is the clone entrypoint.
	snapshotLvolMapA := map[string]*Lvol{
		rootSnapA: {
			Name:   rootSnapA,
			Parent: epLvolName,
			Children: map[string]*Lvol{
				replicaA: {Name: replicaA, Parent: rootSnapA},
			},
		},
	}
	snapshotLvolMapB := map[string]*Lvol{
		rootSnapB: {
			Name:   rootSnapB,
			Parent: epLvolName,
			Children: map[string]*Lvol{
				replicaB: {Name: replicaB, Parent: rootSnapB},
			},
		},
	}

	// --- replica A ---
	chainA, err := constructActiveChainFromSnapshotLvolMap(replicaA, snapshotLvolMapA, bdevLvolMap)
	c.Assert(err, IsNil)
	// Chain: [0]=clone entrypoint base, [1]=root snap, [2]=head
	c.Assert(len(chainA) >= 2, Equals, true)
	// chainA[0] is the entrypoint base; its Children should contain only rootSnapA.
	c.Assert(len(chainA[0].Children), Equals, 1)
	_, hasRootA := chainA[0].Children[rootSnapA]
	c.Assert(hasRootA, Equals, true)

	// --- replica B ---
	chainB, err := constructActiveChainFromSnapshotLvolMap(replicaB, snapshotLvolMapB, bdevLvolMap)
	c.Assert(err, IsNil)
	c.Assert(len(chainB) >= 2, Equals, true)
	c.Assert(len(chainB[0].Children), Equals, 1)
	_, hasRootB := chainB[0].Children[rootSnapB]
	c.Assert(hasRootB, Equals, true)

	// --- Isolation: mutating one chain base does not affect the other ---
	chainA[0].Children["extra-child"] = &Lvol{Name: "extra-child"}
	c.Assert(len(chainA[0].Children), Equals, 2)
	c.Assert(len(chainB[0].Children), Equals, 1) // B must be unaffected

	// --- Backing image path ---
	biLvolName := GetBackingImageSnapLvolName("my-image", "disk-uuid-1")
	c.Assert(types.IsBackingImageSnapLvolName(biLvolName), Equals, true)

	replicaC := "bi-r-00000001"
	rootSnapC := GetReplicaSnapshotLvolName(replicaC, "snap-1")

	replicaD := "bi-r-00000002"
	rootSnapD := GetReplicaSnapshotLvolName(replicaD, "snap-1")

	bdevLvolMapBI := map[string]*spdktypes.BdevInfo{
		biLvolName: makeBdevLvol(lvsName, biLvolName, "", []string{rootSnapC, rootSnapD}),
		replicaC:   makeBdevLvol(lvsName, replicaC, rootSnapC, nil),
		replicaD:   makeBdevLvol(lvsName, replicaD, rootSnapD, nil),
	}

	snapshotLvolMapC := map[string]*Lvol{
		rootSnapC: {
			Name:   rootSnapC,
			Parent: biLvolName,
			Children: map[string]*Lvol{
				replicaC: {Name: replicaC, Parent: rootSnapC},
			},
		},
	}
	snapshotLvolMapD := map[string]*Lvol{
		rootSnapD: {
			Name:   rootSnapD,
			Parent: biLvolName,
			Children: map[string]*Lvol{
				replicaD: {Name: replicaD, Parent: rootSnapD},
			},
		},
	}

	chainC, err := constructActiveChainFromSnapshotLvolMap(replicaC, snapshotLvolMapC, bdevLvolMapBI)
	c.Assert(err, IsNil)
	c.Assert(len(chainC[0].Children), Equals, 1)
	_, hasRootC := chainC[0].Children[rootSnapC]
	c.Assert(hasRootC, Equals, true)

	chainD, err := constructActiveChainFromSnapshotLvolMap(replicaD, snapshotLvolMapD, bdevLvolMapBI)
	c.Assert(err, IsNil)
	c.Assert(len(chainD[0].Children), Equals, 1)
	_, hasRootD := chainD[0].Children[rootSnapD]
	c.Assert(hasRootD, Equals, true)

	// Isolation between backing-image chains
	chainC[0].Children["injected"] = &Lvol{Name: "injected"}
	c.Assert(len(chainC[0].Children), Equals, 2)
	c.Assert(len(chainD[0].Children), Equals, 1) // D must be unaffected
}

// ---------------------------------------------------------------------------
// ServiceReplicaToProtoReplica() — clone metadata serialization
// ---------------------------------------------------------------------------

func (s *TestSuite) TestServiceReplicaToProtoReplicaCloneMetadata(c *C) {
	srcReplicaName := "src-r-00000001"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, "snap-1")
	ep2Name := GetCloneEntrypointLvolName(srcReplicaName, "snap-2")

	// Clone replica: fields are forwarded to proto
	cloneR := newTestReplica("dst-r-00000001", "test-disk")
	cloneR.isCloneReplica = true
	cloneR.cloneSourceReplicaName = srcReplicaName
	cloneR.cloneEntrypointLvolName = epLvolName
	cloneR.SnapshotLvolMap = map[string]*Lvol{}

	proto := ServiceReplicaToProtoReplica(cloneR)
	c.Assert(proto, NotNil)
	c.Assert(proto.IsCloneReplica, Equals, true)
	c.Assert(proto.CloneSourceReplicaName, Equals, srcReplicaName)
	c.Assert(proto.CloneEntrypointLvolName, Equals, epLvolName)

	// Source replica: entrypoint map is forwarded with clone counts
	srcR := newTestReplica(srcReplicaName, "test-disk")
	srcR.SnapshotLvolMap = map[string]*Lvol{}
	srcR.cloneEntrypointMap = map[string]*CloneEntrypointInfo{
		epLvolName: {
			LvolName:         epLvolName,
			SnapshotName:     "snap-1",
			SnapshotLvolName: GetReplicaSnapshotLvolName(srcReplicaName, "snap-1"),
			CloneReplicas:    map[string]bool{"clone-a": true, "clone-b": true},
		},
		ep2Name: {
			LvolName:         ep2Name,
			SnapshotName:     "snap-2",
			SnapshotLvolName: GetReplicaSnapshotLvolName(srcReplicaName, "snap-2"),
			CloneReplicas:    map[string]bool{"clone-c": true},
		},
	}

	proto = ServiceReplicaToProtoReplica(srcR)
	c.Assert(proto, NotNil)
	c.Assert(proto.IsCloneReplica, Equals, false)
	c.Assert(proto.CloneEntrypointMap, NotNil)
	c.Assert(len(proto.CloneEntrypointMap), Equals, 2)
	c.Assert(proto.CloneEntrypointMap[epLvolName], Equals, int32(2))
	c.Assert(proto.CloneEntrypointMap[ep2Name], Equals, int32(1))

	// Plain replica: no clone metadata
	plainR := newTestReplica("plain-r-00000001", "test-disk")
	plainR.SnapshotLvolMap = map[string]*Lvol{}

	proto = ServiceReplicaToProtoReplica(plainR)
	c.Assert(proto, NotNil)
	c.Assert(proto.IsCloneReplica, Equals, false)
	c.Assert(proto.CloneSourceReplicaName, Equals, "")
	c.Assert(proto.CloneEntrypointLvolName, Equals, "")
	c.Assert(proto.CloneEntrypointMap, IsNil)
}

// ---------------------------------------------------------------------------
// syncWithBdevLvolMap()
// ---------------------------------------------------------------------------

// TestSyncWithBdevLvolMap verifies the dispatch and guard logic of
// syncWithBdevLvolMap:
//
//   - Part A: Pending dst clone → construct() → Stopped with clone metadata
//   - Part B: Running src replica → syncCloneEntrypoints discovers entrypoint
//   - Part C: Running with isSnapshotCloning=true → early return, no state change
//   - Part D: Running clone with stale ActiveChain[0] (wrong entrypoint name) →
//     syncCloneReplicaInfo returns "does not match expected entrypoint" error
func (s *TestSuite) TestSyncWithBdevLvolMap(c *C) {
	// Part A: Pending dst clone → dispatched to construct() → Stopped state.
	{
		srcReplicaName := "src-r-00000001"
		dstReplicaName := "dst-r-00000001"
		snapshotName := "snap-1"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)

		r := newTestReplica(dstReplicaName, "test-disk")
		bdevLvolMap := buildCloneReplicaBdevLvolMap("test-disk", dstReplicaName, rootSnapName, epLvolName)

		err := r.syncWithBdevLvolMap(nil, bdevLvolMap)
		c.Assert(err, IsNil)
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateStopped))
		c.Assert(r.isCloneReplica, Equals, true)
		c.Assert(r.cloneEntrypointLvolName, Equals, epLvolName)
		c.Assert(r.cloneSourceReplicaName, Equals, srcReplicaName)
	}

	// Part B: Running src replica → dispatched to syncCloneEntrypoints → entrypoint discovered.
	{
		srcReplicaName2 := "src-r-00000002"
		snapshotName2 := "snap-2"
		ep2LvolName := GetCloneEntrypointLvolName(srcReplicaName2, snapshotName2)
		cloneChild := GetReplicaSnapshotLvolName("dst-r-00000002", snapshotName2)

		rSrc := &Replica{
			Name:               srcReplicaName2,
			LvsName:            "test-disk",
			State:              types.InstanceState(types.InstanceStateRunning),
			cloneEntrypointMap: map[string]*CloneEntrypointInfo{},
			SnapshotLvolMap:    map[string]*Lvol{},
			log:                safelog.NewSafeLogger(logrus.WithField("test", srcReplicaName2)),
		}
		bdevLvolMapSrc := buildSrcOnlyBdevLvolMap("test-disk", srcReplicaName2, map[string]*spdktypes.BdevInfo{
			ep2LvolName: {
				DriverSpecific: &spdktypes.BdevDriverSpecific{
					Lvol: &spdktypes.BdevDriverSpecificLvol{
						Clones: []string{cloneChild},
					},
				},
			},
		})

		err := rSrc.syncWithBdevLvolMap(nil, bdevLvolMapSrc)
		c.Assert(err, IsNil)
		c.Assert(len(rSrc.cloneEntrypointMap), Equals, 1)
		ep2Info, ok := rSrc.cloneEntrypointMap[ep2LvolName]
		c.Assert(ok, Equals, true)
		c.Assert(ep2Info.SnapshotName, Equals, snapshotName2)
	}

	// Part C: Running with isSnapshotCloning=true → early return, state unchanged.
	// This guards the window between SnapshotCloneDstStart setting isCloneReplica
	// and SnapshotCloneDstFinish committing the in-memory chain update.
	{
		srcReplicaName := "src-r-00000001"
		dstReplicaName := "dst-r-00000001"
		snapshotName := "snap-1"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		lvsName := "test-disk"

		headSvcLvol := &Lvol{
			Name:     dstReplicaName,
			UUID:     fmt.Sprintf("uuid-%s", dstReplicaName),
			Alias:    lvsName + "/" + dstReplicaName,
			SpecSize: 2048 * 512,
			Parent:   epLvolName,
			Children: map[string]*Lvol{},
		}
		r := &Replica{
			Name:                    dstReplicaName,
			LvsName:                 lvsName,
			State:                   types.InstanceState(types.InstanceStateRunning),
			isCloneReplica:          true,
			cloneSourceReplicaName:  srcReplicaName,
			cloneEntrypointLvolName: epLvolName,
			isSnapshotCloning:       true, // clone dst finish not yet complete
			SnapshotLvolMap:         map[string]*Lvol{},
			ActiveChain:             []*Lvol{nil, headSvcLvol},
			Head:                    headSvcLvol,
			cloneEntrypointMap:      map[string]*CloneEntrypointInfo{},
			log:                     safelog.NewSafeLogger(logrus.WithField("test", dstReplicaName)),
		}

		// bdevLvolMap shows the clone entrypoint as chain base — validateAndUpdate
		// would error with "chain base is nil" if it ran. The isSnapshotCloning
		// guard must prevent syncWithBdevLvolMap from reaching that point.
		bdevLvolMap := map[string]*spdktypes.BdevInfo{
			dstReplicaName: makeBdevLvol(lvsName, dstReplicaName, epLvolName, nil),
			epLvolName:     makeBdevLvol(lvsName, epLvolName, "", []string{dstReplicaName}),
		}

		err := r.syncWithBdevLvolMap(nil, bdevLvolMap)
		c.Assert(err, IsNil)
		// State must be unchanged — sync was skipped entirely.
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
		c.Assert(r.ActiveChain[0], IsNil)
	}

	// Part D: Running clone replica whose ActiveChain[0] has a name that differs
	// from cloneEntrypointLvolName — syncCloneReplicaInfo detects the mismatch.
	{
		srcReplicaName := "src-r-00000004"
		dstReplicaName := "dst-r-00000004"
		snapshotName := "snap-4"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)
		wrongEpLvolName := "some-stale-entrypoint"

		r := &Replica{
			Name:                    dstReplicaName,
			LvsName:                 "test-disk",
			State:                   types.InstanceState(types.InstanceStateRunning),
			isCloneReplica:          true,
			cloneSourceReplicaName:  srcReplicaName,
			cloneEntrypointLvolName: epLvolName,
			ActiveChain: []*Lvol{
				{Name: wrongEpLvolName, Alias: "test-disk/" + wrongEpLvolName},
				{Name: rootSnapName, Alias: "test-disk/" + rootSnapName},
			},
			cloneEntrypointMap: map[string]*CloneEntrypointInfo{},
			SnapshotLvolMap:    map[string]*Lvol{},
			log:                safelog.NewSafeLogger(logrus.WithField("test", dstReplicaName)),
		}

		err := r.syncWithBdevLvolMap(nil, map[string]*spdktypes.BdevInfo{})
		c.Assert(err, NotNil)
		c.Assert(err, ErrorMatches, ".*does not match expected entrypoint.*")
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateError))
	}
}

// ---------------------------------------------------------------------------
// syncCloneReplicaInfo() — all three cases via syncWithBdevLvolMap
// ---------------------------------------------------------------------------

// TestSyncCloneReplicaInfo verifies all three branches of syncCloneReplicaInfo,
// exercised via syncWithBdevLvolMap (nil spdkClient, Running dst clone replica):
//
//   - Part A (Case 1): chain root's parent is the expected entrypoint — no error
//   - Part B (Case 2): chain root's parent is the source snapshot directly (entrypoint
//     was removed, SPDK reparented the child) — repair fails without SPDK client
//   - Part C (Case 3): chain root's parent is an unrelated lvol — corruption error
func (s *TestSuite) TestSyncCloneReplicaInfo(c *C) {
	// Part A — Case 1: parent matches the expected entrypoint (happy path)
	{
		srcReplicaName := "src-r-00000001"
		dstReplicaName := "dst-r-00000001"
		snapshotName := "snap-1"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)

		r := newTestRunningCloneReplica(dstReplicaName, srcReplicaName, "test-disk", epLvolName, rootSnapName)
		bdevLvolMap := map[string]*spdktypes.BdevInfo{
			rootSnapName: makeBdevLvol("test-disk", rootSnapName, epLvolName, []string{dstReplicaName}),
		}

		err := r.syncWithBdevLvolMap(nil, bdevLvolMap)
		c.Assert(err, IsNil)
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
		c.Assert(r.isCloneReplica, Equals, true)
		c.Assert(r.cloneEntrypointLvolName, Equals, epLvolName)
	}

	// Part B — Case 2: chain root's parent is the source snapshot directly.
	// Entrypoint was removed, SPDK reparented to source snapshot.
	// Without an SPDK client the repair fails → replica Error state.
	{
		srcReplicaName := "src-r-00000002"
		dstReplicaName := "dst-r-00000002"
		snapshotName := "snap-2"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)
		srcSnapshotLvolName := GetReplicaSnapshotLvolName(srcReplicaName, snapshotName)

		r := newTestRunningCloneReplica(dstReplicaName, srcReplicaName, "test-disk", epLvolName, rootSnapName)
		bdevLvolMap := map[string]*spdktypes.BdevInfo{
			rootSnapName: makeBdevLvol("test-disk", rootSnapName, srcSnapshotLvolName, []string{dstReplicaName}),
		}

		err := r.syncWithBdevLvolMap(nil, bdevLvolMap)
		c.Assert(err, NotNil)
		c.Assert(err, ErrorMatches, ".*cannot repair clone entrypoint without SPDK client.*")
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateError))
	}

	// Part C — Case 3: chain root's parent is an unrelated lvol (corruption).
	{
		srcReplicaName := "src-r-00000003"
		dstReplicaName := "dst-r-00000003"
		snapshotName := "snap-3"
		epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
		rootSnapName := GetReplicaSnapshotLvolName(dstReplicaName, snapshotName)

		r := newTestRunningCloneReplica(dstReplicaName, srcReplicaName, "test-disk", epLvolName, rootSnapName)
		bdevLvolMap := map[string]*spdktypes.BdevInfo{
			rootSnapName: makeBdevLvol("test-disk", rootSnapName, "some-unrelated-lvol", []string{dstReplicaName}),
		}

		err := r.syncWithBdevLvolMap(nil, bdevLvolMap)
		c.Assert(err, NotNil)
		c.Assert(err, ErrorMatches, ".*does not match expected entrypoint.*")
		c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateError))
	}
}
