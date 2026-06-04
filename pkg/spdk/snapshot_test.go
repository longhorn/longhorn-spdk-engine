package spdk

import (
	"errors"
	"fmt"
	"strings"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSnapshotOperationPreCheckCreateGeneratesName(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock generates snapshot name for create operation")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

	snapshotName, err := e.snapshotOperationPreCheckWithoutLock("", SnapshotOperationCreate)
	c.Assert(err, IsNil)
	c.Assert(snapshotName, Not(Equals), "")
	c.Assert(len(snapshotName), Equals, 8)
}

func (s *TestSuite) TestSnapshotOperationPreCheckDeleteEmptyName(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock returns error for delete operation with empty snapshot name")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

	_, err := e.snapshotOperationPreCheckWithoutLock("", SnapshotOperationDelete)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "empty snapshot name"), Equals, true)
}

func (s *TestSuite) TestSnapshotOperationPreCheckCreateFailsWhenSnapshotMaxCountReached(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock returns error when snapshot max count is reached")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.SnapshotMaxCount = 2
	e.SnapshotMap["snap-1"] = &api.Lvol{Name: "snap-1"}
	e.SnapshotMap["snap-2"] = &api.Lvol{Name: "snap-2"}

	_, err := e.snapshotOperationPreCheckWithoutLock("", SnapshotOperationCreate)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot count 2 is equal or larger than snapshotMaxCount 2"), Equals, true)
}

func (s *TestSuite) TestSnapshotOperationPreCheckRevertReadsSnapshotsViaUpstream(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert reads snapshot map via Upstream.Get()")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty // revert requires empty frontend
	u := newFakeUpstream("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.View = &UpstreamView{
		SpecSize:  100,
		Snapshots: map[string]*api.Lvol{"snap-1": {Name: "snap-1"}},
	}
	e.upstreams = map[string]Upstream{"r1": u}

	// Snapshot exists on the upstream -> revert pre-check succeeds.
	name, err := e.snapshotOperationPreCheckWithoutLock("snap-1", SnapshotOperationRevert)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, "snap-1")
}

func (s *TestSuite) TestSnapshotOperationPreCheckRevertRejectsMissingSnapshot(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert rejects when snapshot is absent from Upstream.Get()")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty
	u := newFakeUpstream("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.View = &UpstreamView{
		SpecSize:  100,
		Snapshots: map[string]*api.Lvol{}, // no snapshots
	}
	e.upstreams = map[string]Upstream{"r1": u}

	_, err := e.snapshotOperationPreCheckWithoutLock("snap-missing", SnapshotOperationRevert)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "does not contain the reverting snapshot"), Equals, true)
}

func (s *TestSuite) TestSnapshotOperationPreCheckRevertPropagatesUpstreamError(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert propagates Upstream.Get() error")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty
	u := newFakeUpstream("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.ViewErr = errors.New("upstream get failed")
	e.upstreams = map[string]Upstream{"r1": u}

	_, err := e.snapshotOperationPreCheckWithoutLock("snap-1", SnapshotOperationRevert)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "upstream get failed"), Equals, true)
}

func newEngineWithFakeUpstream() (*Engine, *fakeUpstream) {
	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeUpstream("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	e.upstreams = map[string]Upstream{"r1": u}
	return e, u
}

func (s *TestSuite) TestReplicaSnapshotOperationCreateRoutesThroughUpstream(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Create through Upstream.SnapshotCreate")

	e, u := newEngineWithFakeUpstream()

	opts := &api.SnapshotOptions{Timestamp: "2026-05-05T00:00:00Z", UserCreated: true}
	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationCreate, opts)
	c.Assert(err, IsNil)
	c.Assert(len(u.SnapshotCreateCalls), Equals, 1)
	c.Assert(u.SnapshotCreateCalls[0].SnapshotName, Equals, "snap-1")
	c.Assert(u.SnapshotCreateCalls[0].Opts, Equals, opts)
}

func (s *TestSuite) TestReplicaSnapshotOperationDeleteRoutesThroughUpstream(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Delete through Upstream.SnapshotDelete")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationDelete, nil)
	c.Assert(err, IsNil)
	c.Assert(u.SnapshotDeleteCalls, DeepEquals, []string{"snap-1"})
}

func (s *TestSuite) TestReplicaSnapshotOperationPurgeRoutesThroughUpstream(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Purge through Upstream.SnapshotPurge")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "", SnapshotOperationPurge, nil)
	c.Assert(err, IsNil)
	c.Assert(u.SnapshotPurgeCalls, Equals, 1)
}

func (s *TestSuite) TestReplicaSnapshotOperationHashRoutesThroughUpstream(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Hash through Upstream.SnapshotHash with rehash flag")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationHash, true)
	c.Assert(err, IsNil)
	c.Assert(len(u.SnapshotHashCalls), Equals, 1)
	c.Assert(u.SnapshotHashCalls[0].SnapshotName, Equals, "snap-1")
	c.Assert(u.SnapshotHashCalls[0].Rehash, Equals, true)
}

func (s *TestSuite) TestReplicaSnapshotOperationCreateRejectsBadOpts(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects Create when opts is not *api.SnapshotOptions")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationCreate, "not-options-type")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid opts types"), Equals, true)
	c.Assert(len(u.SnapshotCreateCalls), Equals, 0)
}

func (s *TestSuite) TestReplicaSnapshotOperationHashRejectsBadOpts(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects Hash when opts is not bool")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationHash, "not-a-bool")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "rehash should be a boolean"), Equals, true)
	c.Assert(len(u.SnapshotHashCalls), Equals, 0)
}

func (s *TestSuite) TestReplicaSnapshotOperationPropagatesUpstreamError(c *C) {
	fmt.Println("Testing replicaSnapshotOperation propagates errors from the Upstream RPC")

	e, u := newEngineWithFakeUpstream()
	u.SnapshotDeleteErr = errors.New("upstream snapshot delete failed")

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationDelete, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "upstream snapshot delete failed"), Equals, true)
}

func (s *TestSuite) TestReplicaSnapshotOperationUnknownOp(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects an unknown SnapshotOperationType")

	e, u := newEngineWithFakeUpstream()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationType("UNKNOWN"), nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "unknown replica snapshot operation"), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSnapshotOperationCreateFailsWhenInitiatorNil(c *C) {
	fmt.Println("Testing engine frontend snapshotOperation returns error when initiator is nil for create operation")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1), nil)
	ef.State = types.InstanceStateRunning
	ef.Frontend = types.FrontendSPDKTCPBlockdev
	ef.Endpoint = "/dev/longhorn/test"
	ef.initiator = nil

	_, err := ef.snapshotOperation("snap-1", SnapshotOperationCreate, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to suspend before the snapshot operation"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "initiator is not initialized"), Equals, true)
}
