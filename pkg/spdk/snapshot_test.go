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

func (s *TestSuite) TestSnapshotOperationPreCheckRevertReadsSnapshotsViaBackend(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert reads snapshot map via Backend.Get()")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty // revert requires empty frontend
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.View = &BackendView{
		SpecSize:  100,
		Snapshots: map[string]*api.Lvol{"snap-1": {Name: "snap-1"}},
	}
	e.backends = map[string]Backend{"r1": u}

	// Snapshot exists on the backend -> revert pre-check succeeds.
	name, err := e.snapshotOperationPreCheckWithoutLock("snap-1", SnapshotOperationRevert)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, "snap-1")
}

func (s *TestSuite) TestSnapshotOperationPreCheckRevertRejectsMissingSnapshot(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert rejects when snapshot is absent from Backend.Get()")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.View = &BackendView{
		SpecSize:  100,
		Snapshots: map[string]*api.Lvol{}, // no snapshots
	}
	e.backends = map[string]Backend{"r1": u}

	_, err := e.snapshotOperationPreCheckWithoutLock("snap-missing", SnapshotOperationRevert)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "does not contain the reverting snapshot"), Equals, true)
}

func (s *TestSuite) TestSnapshotOperationPreCheckRevertPropagatesBackendError(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock for revert propagates Backend.Get() error")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	u.ViewErr = errors.New("backend get failed")
	e.backends = map[string]Backend{"r1": u}

	_, err := e.snapshotOperationPreCheckWithoutLock("snap-1", SnapshotOperationRevert)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "backend get failed"), Equals, true)
}

func (s *TestSuite) TestSnapshotOperationPreCheckSkipsNonDispatchableBackends(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock skips non-dispatchable backends")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.Frontend = types.FrontendEmpty

	dispatchable := newFakeBackend("r1", "10.0.0.1:1234")
	dispatchable.SetMode(types.ModeRW)
	dispatchable.View = &BackendView{
		SpecSize:  100,
		Snapshots: map[string]*api.Lvol{"snap-1": {Name: "snap-1"}},
	}

	// Both would fail the revert pre-check if visited: errored.Get() returns
	// an error, and addressless is WO which revert rejects.
	errored := newFakeBackend("r2", "10.0.0.2:1234")
	errored.SetMode(types.ModeERR)
	errored.ViewErr = errors.New("must not be called")

	addressless := newFakeBackend("r3", "")
	addressless.SetMode(types.ModeWO)

	e.backends = map[string]Backend{"r1": dispatchable, "r2": errored, "r3": addressless}

	name, err := e.snapshotOperationPreCheckWithoutLock("snap-1", SnapshotOperationRevert)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, "snap-1")
}

func newEngineWithFakeBackend() (*Engine, *fakeBackend) {
	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(types.ModeRW)
	e.backends = map[string]Backend{"r1": u}
	return e, u
}

func (s *TestSuite) TestReplicaSnapshotOperationCreateRoutesThroughBackend(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Create through Backend.SnapshotCreate")

	e, u := newEngineWithFakeBackend()

	opts := &api.SnapshotOptions{Timestamp: "2026-05-05T00:00:00Z", UserCreated: true}
	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationCreate, opts)
	c.Assert(err, IsNil)
	c.Assert(len(u.SnapshotCreateCalls), Equals, 1)
	c.Assert(u.SnapshotCreateCalls[0].SnapshotName, Equals, "snap-1")
	c.Assert(u.SnapshotCreateCalls[0].Opts, Equals, opts)
}

func (s *TestSuite) TestReplicaSnapshotOperationDeleteRoutesThroughBackend(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Delete through Backend.SnapshotDelete")

	e, u := newEngineWithFakeBackend()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationDelete, nil)
	c.Assert(err, IsNil)
	c.Assert(u.SnapshotDeleteCalls, DeepEquals, []string{"snap-1"})
}

func (s *TestSuite) TestReplicaSnapshotOperationPurgeRoutesThroughBackend(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Purge through Backend.SnapshotPurge")

	e, u := newEngineWithFakeBackend()

	err := e.replicaSnapshotOperation(nil, u, "r1", "", SnapshotOperationPurge, nil)
	c.Assert(err, IsNil)
	c.Assert(u.SnapshotPurgeCalls, Equals, 1)
}

func (s *TestSuite) TestReplicaSnapshotOperationHashRoutesThroughBackend(c *C) {
	fmt.Println("Testing replicaSnapshotOperation routes Hash through Backend.SnapshotHash with rehash flag")

	e, u := newEngineWithFakeBackend()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationHash, true)
	c.Assert(err, IsNil)
	c.Assert(len(u.SnapshotHashCalls), Equals, 1)
	c.Assert(u.SnapshotHashCalls[0].SnapshotName, Equals, "snap-1")
	c.Assert(u.SnapshotHashCalls[0].Rehash, Equals, true)
}

func (s *TestSuite) TestReplicaSnapshotOperationCreateRejectsBadOpts(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects Create when opts is not *api.SnapshotOptions")

	e, u := newEngineWithFakeBackend()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationCreate, "not-options-type")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid opts types"), Equals, true)
	c.Assert(len(u.SnapshotCreateCalls), Equals, 0)
}

func (s *TestSuite) TestReplicaSnapshotOperationHashRejectsBadOpts(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects Hash when opts is not bool")

	e, u := newEngineWithFakeBackend()

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationHash, "not-a-bool")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "rehash should be a boolean"), Equals, true)
	c.Assert(len(u.SnapshotHashCalls), Equals, 0)
}

func (s *TestSuite) TestReplicaSnapshotOperationPropagatesBackendError(c *C) {
	fmt.Println("Testing replicaSnapshotOperation propagates errors from the Backend RPC")

	e, u := newEngineWithFakeBackend()
	u.SnapshotDeleteErr = errors.New("backend snapshot delete failed")

	err := e.replicaSnapshotOperation(nil, u, "r1", "snap-1", SnapshotOperationDelete, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "backend snapshot delete failed"), Equals, true)
}

func (s *TestSuite) TestReplicaSnapshotOperationUnknownOp(c *C) {
	fmt.Println("Testing replicaSnapshotOperation rejects an unknown SnapshotOperationType")

	e, u := newEngineWithFakeBackend()

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
