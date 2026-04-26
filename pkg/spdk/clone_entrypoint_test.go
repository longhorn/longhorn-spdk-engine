package spdk

import (
	"fmt"

	"github.com/sirupsen/logrus"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestReplica(name, lvsName string) *Replica {
	return &Replica{
		Name:               name,
		LvsName:            lvsName,
		State:              types.InstanceState(types.InstanceStateRunning),
		cloneEntrypointMap: map[string]*CloneEntrypointInfo{},
		log:                safelog.NewSafeLogger(logrus.WithField("test", name)),
	}
}

// ---------------------------------------------------------------------------
// Naming convention tests
// ---------------------------------------------------------------------------

func (s *TestSuite) TestCloneEntrypointNamingConventions(c *C) {
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
}

func (s *TestSuite) TestGetCloneReplicaNameFromEntrypointChildLvol(c *C) {
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("clone-replica"), Equals, "clone-replica")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("clone-replica-snap-s1"), Equals, "clone-replica")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("pvc-abc-r-12345678-snap-snap1"), Equals, "pvc-abc-r-12345678")
	c.Assert(GetCloneReplicaNameFromEntrypointChildLvol("pvc-abc-r-12345678"), Equals, "pvc-abc-r-12345678")
}

// ---------------------------------------------------------------------------
// recoverCloneReplicaInfo tests
// ---------------------------------------------------------------------------

func (s *TestSuite) TestRecoverCloneReplicaInfoDetectsClone(c *C) {
	r := newTestReplica("dst-r-00000001", "test-disk")
	epLvolName := GetCloneEntrypointLvolName("src-r-00000001", "snap-1")

	r.ActiveChain = []*Lvol{
		{Name: epLvolName},
		{Name: "dst-r-00000001-snap-snap-1", Parent: epLvolName},
		{Name: "dst-r-00000001"},
	}

	r.recoverCloneReplicaInfo()

	c.Assert(r.isCloneReplica, Equals, true)
	c.Assert(r.cloneEntrypointLvolName, Equals, epLvolName)
	c.Assert(r.cloneSourceReplicaName, Equals, "src-r-00000001")
}

func (s *TestSuite) TestRecoverCloneReplicaInfoSkipsNonClone(c *C) {
	r := newTestReplica("normal-r-00000001", "test-disk")
	r.ActiveChain = []*Lvol{
		nil,
		{Name: "normal-r-00000001-snap-snap-1", Parent: ""},
		{Name: "normal-r-00000001"},
	}

	r.recoverCloneReplicaInfo()

	c.Assert(r.isCloneReplica, Equals, false)
	c.Assert(r.cloneEntrypointLvolName, Equals, "")
}

func (s *TestSuite) TestRecoverCloneReplicaInfoSkipsShortChain(c *C) {
	// Single-element chain
	r := newTestReplica("r-00000001", "test-disk")
	r.ActiveChain = []*Lvol{nil}

	r.recoverCloneReplicaInfo()
	c.Assert(r.isCloneReplica, Equals, false)

	// Empty chain — must not panic
	r2 := newTestReplica("r-00000002", "test-disk")
	r2.ActiveChain = []*Lvol{}

	r2.recoverCloneReplicaInfo()
	c.Assert(r2.isCloneReplica, Equals, false)
}

func (s *TestSuite) TestRecoverCloneReplicaInfoSkipsNonEntrypointParent(c *C) {
	r := newTestReplica("dst-r-00000001", "test-disk")
	r.ActiveChain = []*Lvol{
		nil,
		{Name: "dst-r-00000001-snap-snap-1", Parent: "src-r-00000001-snap-snap-1"},
		{Name: "dst-r-00000001"},
	}

	r.recoverCloneReplicaInfo()

	c.Assert(r.isCloneReplica, Equals, false)
}

func (s *TestSuite) TestRecoverCloneReplicaInfoSkipsNonEntrypointAtIndex0(c *C) {
	r := newTestReplica("dst-r-00000001", "test-disk")
	r.ActiveChain = []*Lvol{
		{Name: "some-backing-image-snap"},
		{Name: "dst-r-00000001-snap-snap-1", Parent: "some-backing-image-snap"},
		{Name: "dst-r-00000001"},
	}

	r.recoverCloneReplicaInfo()

	c.Assert(r.isCloneReplica, Equals, false)
}

// ---------------------------------------------------------------------------
// recoverCloneEntrypointInfo tests
// ---------------------------------------------------------------------------

func (s *TestSuite) TestRecoverCloneEntrypointInfo(c *C) {
	srcReplicaName := "src-r-00000001"
	snapshotName := "snap-1"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)

	r := newTestReplica(srcReplicaName, "test-disk")

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		epLvolName: {
			DriverSpecific: &spdktypes.BdevDriverSpecific{
				Lvol: &spdktypes.BdevDriverSpecificLvol{
					Clones: []string{
						"dst-r-00000001-snap-snap-1",
						"dst-r-00000002-snap-snap-1",
					},
				},
			},
		},
		"unrelated-lvol": {
			DriverSpecific: &spdktypes.BdevDriverSpecific{
				Lvol: &spdktypes.BdevDriverSpecificLvol{},
			},
		},
	}

	r.recoverCloneEntrypointInfo(bdevLvolMap)

	c.Assert(len(r.cloneEntrypointMap), Equals, 1)
	epInfo, ok := r.cloneEntrypointMap[epLvolName]
	c.Assert(ok, Equals, true)
	c.Assert(epInfo.SnapshotName, Equals, snapshotName)
	c.Assert(epInfo.SnapshotLvolName, Equals, GetReplicaSnapshotLvolName(srcReplicaName, snapshotName))
	c.Assert(len(epInfo.CloneReplicas), Equals, 2)
	c.Assert(epInfo.CloneReplicas["dst-r-00000001"], Equals, true)
	c.Assert(epInfo.CloneReplicas["dst-r-00000002"], Equals, true)
}

func (s *TestSuite) TestRecoverCloneEntrypointInfoSkipsTmpHead(c *C) {
	srcReplicaName := "src-r-00000001"
	snapshotName := "snap-1"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
	tmpHeadLvolName := GetCloneEntrypointTmpHeadLvolName(srcReplicaName, snapshotName)

	r := newTestReplica(srcReplicaName, "test-disk")

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		epLvolName: {
			DriverSpecific: &spdktypes.BdevDriverSpecific{
				Lvol: &spdktypes.BdevDriverSpecificLvol{
					Clones: []string{
						tmpHeadLvolName,
						"dst-r-00000001-snap-snap-1",
					},
				},
			},
		},
	}

	r.recoverCloneEntrypointInfo(bdevLvolMap)

	epInfo := r.cloneEntrypointMap[epLvolName]
	c.Assert(len(epInfo.CloneReplicas), Equals, 1)
	c.Assert(epInfo.CloneReplicas["dst-r-00000001"], Equals, true)
}

func (s *TestSuite) TestRecoverCloneEntrypointInfoNoEntrypoints(c *C) {
	r := newTestReplica("src-r-00000001", "test-disk")

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		"some-other-lvol": {
			DriverSpecific: &spdktypes.BdevDriverSpecific{
				Lvol: &spdktypes.BdevDriverSpecificLvol{},
			},
		},
	}

	r.recoverCloneEntrypointInfo(bdevLvolMap)
	c.Assert(len(r.cloneEntrypointMap), Equals, 0)
}

// ---------------------------------------------------------------------------
// syncCloneReplicaInfo tests
// ---------------------------------------------------------------------------

func (s *TestSuite) TestSyncCloneReplicaInfoSkipsNonClone(c *C) {
	r := newTestReplica("r-00000001", "test-disk")
	r.isCloneReplica = false

	r.syncCloneReplicaInfo(nil, nil)
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
}

func (s *TestSuite) TestSyncCloneReplicaInfoHappyPath(c *C) {
	srcReplicaName := "src-r-00000001"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, "snap-1")
	rootSnapName := "dst-r-00000001-snap-snap-1"

	r := newTestReplica("dst-r-00000001", "test-disk")
	r.isCloneReplica = true
	r.cloneSourceReplicaName = srcReplicaName
	r.cloneEntrypointLvolName = epLvolName
	r.ActiveChain = []*Lvol{
		{Name: epLvolName},
		{Name: rootSnapName, Parent: epLvolName},
		{Name: "dst-r-00000001"},
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		rootSnapName: makeBdevLvol("test-disk", rootSnapName, epLvolName, nil),
	}

	r.syncCloneReplicaInfo(nil, bdevLvolMap)
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Assert(r.ErrorMsg, Equals, "")
}

func (s *TestSuite) TestSyncCloneReplicaInfoCorruptedParent(c *C) {
	srcReplicaName := "src-r-00000001"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, "snap-1")
	rootSnapName := "dst-r-00000001-snap-snap-1"

	r := newTestReplica("dst-r-00000001", "test-disk")
	r.isCloneReplica = true
	r.cloneSourceReplicaName = srcReplicaName
	r.cloneEntrypointLvolName = epLvolName
	r.ActiveChain = []*Lvol{
		{Name: epLvolName},
		{Name: rootSnapName, Parent: "some-random-lvol"},
		{Name: "dst-r-00000001"},
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		rootSnapName: makeBdevLvol("test-disk", rootSnapName, "some-random-lvol", nil),
	}

	r.syncCloneReplicaInfo(nil, bdevLvolMap)
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Assert(r.ErrorMsg, Not(Equals), "")
}

func (s *TestSuite) TestSyncCloneReplicaInfoRepairWithNilClient(c *C) {
	srcReplicaName := "src-r-00000001"
	snapshotName := "snap-1"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, snapshotName)
	srcSnapLvolName := GetReplicaSnapshotLvolName(srcReplicaName, snapshotName)
	rootSnapName := "dst-r-00000001-snap-snap-1"

	r := newTestReplica("dst-r-00000001", "test-disk")
	r.isCloneReplica = true
	r.cloneSourceReplicaName = srcReplicaName
	r.cloneEntrypointLvolName = epLvolName
	r.ActiveChain = []*Lvol{
		{Name: epLvolName},
		{Name: rootSnapName, Parent: srcSnapLvolName},
		{Name: "dst-r-00000001"},
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		rootSnapName: makeBdevLvol("test-disk", rootSnapName, srcSnapLvolName, nil),
	}

	r.syncCloneReplicaInfo(nil, bdevLvolMap)

	// Repair attempted but fails because SPDK client is nil → error state
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateError))
	c.Assert(r.ErrorMsg, Not(Equals), "")
}

func (s *TestSuite) TestSyncCloneReplicaInfoSkipsShortChain(c *C) {
	r := newTestReplica("dst-r-00000001", "test-disk")
	r.isCloneReplica = true
	r.ActiveChain = []*Lvol{nil}

	r.syncCloneReplicaInfo(nil, nil)
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
}

func (s *TestSuite) TestSyncCloneReplicaInfoDetectsStaleActiveChain0(c *C) {
	srcReplicaName := "src-r-00000001"
	epLvolName := GetCloneEntrypointLvolName(srcReplicaName, "snap-1")
	rootSnapName := "dst-r-00000001-snap-snap-1"

	r := newTestReplica("dst-r-00000001", "test-disk")
	r.isCloneReplica = true
	r.cloneSourceReplicaName = srcReplicaName
	r.cloneEntrypointLvolName = epLvolName
	r.ActiveChain = []*Lvol{
		{Name: "wrong-entrypoint-name"},
		{Name: rootSnapName, Parent: epLvolName},
		{Name: "dst-r-00000001"},
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		rootSnapName: makeBdevLvol("test-disk", rootSnapName, epLvolName, nil),
	}

	// Should still succeed (parent matches), but logs a warning about ActiveChain[0] mismatch
	r.syncCloneReplicaInfo(nil, bdevLvolMap)
	c.Assert(r.State, Equals, types.InstanceState(types.InstanceStateRunning))
	c.Assert(r.ErrorMsg, Equals, "")
}

// ---------------------------------------------------------------------------
// recoverCloneEntrypointInfo — multiple snapshot entrypoints
// ---------------------------------------------------------------------------

func (s *TestSuite) TestRecoverCloneEntrypointInfoMultipleSnapshots(c *C) {
	srcReplicaName := "src-r-00000001"

	snap1 := "snap-1"
	snap2 := "snap-2"
	ep1LvolName := GetCloneEntrypointLvolName(srcReplicaName, snap1)
	ep2LvolName := GetCloneEntrypointLvolName(srcReplicaName, snap2)

	r := newTestReplica(srcReplicaName, "test-disk")

	bdevLvolMap := map[string]*spdktypes.BdevInfo{
		ep1LvolName: {
			DriverSpecific: &spdktypes.BdevDriverSpecific{
				Lvol: &spdktypes.BdevDriverSpecificLvol{
					Clones: []string{
						"clone-a-r-00000001-snap-snap-1",
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
	}

	r.recoverCloneEntrypointInfo(bdevLvolMap)

	c.Assert(len(r.cloneEntrypointMap), Equals, 2)

	ep1Info, ok := r.cloneEntrypointMap[ep1LvolName]
	c.Assert(ok, Equals, true)
	c.Assert(ep1Info.SnapshotName, Equals, snap1)
	c.Assert(ep1Info.SnapshotLvolName, Equals, GetReplicaSnapshotLvolName(srcReplicaName, snap1))
	c.Assert(len(ep1Info.CloneReplicas), Equals, 1)
	c.Assert(ep1Info.CloneReplicas["clone-a-r-00000001"], Equals, true)

	ep2Info, ok := r.cloneEntrypointMap[ep2LvolName]
	c.Assert(ok, Equals, true)
	c.Assert(ep2Info.SnapshotName, Equals, snap2)
	c.Assert(ep2Info.SnapshotLvolName, Equals, GetReplicaSnapshotLvolName(srcReplicaName, snap2))
	c.Assert(len(ep2Info.CloneReplicas), Equals, 2)
	c.Assert(ep2Info.CloneReplicas["clone-b-r-00000001"], Equals, true)
	c.Assert(ep2Info.CloneReplicas["clone-c-r-00000001"], Equals, true)
}

// ---------------------------------------------------------------------------
// recoverCloneReplicaInfo — backing image exclusivity
// ---------------------------------------------------------------------------

func (s *TestSuite) TestRecoverCloneReplicaInfoSkipsBackingImage(c *C) {
	backingImageLvolName := "bi-my-backing-image-disk-uuid"

	c.Assert(types.IsBackingImageSnapLvolName(backingImageLvolName), Equals, true)
	c.Assert(IsCloneEntrypointLvol(backingImageLvolName), Equals, false)

	r := newTestReplica("dst-r-00000001", "test-disk")
	r.ActiveChain = []*Lvol{
		{Name: backingImageLvolName},
		{Name: "dst-r-00000001-snap-snap-1", Parent: backingImageLvolName},
		{Name: "dst-r-00000001"},
	}

	r.recoverCloneReplicaInfo()

	c.Assert(r.isCloneReplica, Equals, false)
	c.Assert(r.cloneEntrypointLvolName, Equals, "")
	c.Assert(r.cloneSourceReplicaName, Equals, "")
}

// ---------------------------------------------------------------------------
// ServiceReplicaToProtoReplica — clone metadata serialization
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
