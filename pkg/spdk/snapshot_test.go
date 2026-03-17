package spdk

import (
	"fmt"
	"strings"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSnapshotOperationPreCheckCreateGeneratesName(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock generates snapshot name for create operation")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))

	snapshotName, err := e.snapshotOperationPreCheckWithoutLock(map[string]*client.SPDKClient{}, "", SnapshotOperationCreate)
	c.Assert(err, IsNil)
	c.Assert(snapshotName, Not(Equals), "")
	c.Assert(len(snapshotName), Equals, 8)
}

func (s *TestSuite) TestSnapshotOperationPreCheckDeleteEmptyName(c *C) {
	fmt.Println("Testing snapshotOperationPreCheckWithoutLock returns error for delete operation with empty snapshot name")

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))

	_, err := e.snapshotOperationPreCheckWithoutLock(map[string]*client.SPDKClient{}, "", SnapshotOperationDelete)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "empty snapshot name"), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSnapshotOperationCreateFailsWhenInitiatorNil(c *C) {
	fmt.Println("Testing engine frontend snapshotOperation returns error when initiator is nil for create operation")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1))
	ef.State = types.InstanceStateRunning
	ef.Frontend = types.FrontendSPDKTCPBlockdev
	ef.Endpoint = "/dev/longhorn/test"
	ef.initiator = nil

	_, err := ef.snapshotOperation("snap-1", SnapshotOperationCreate, nil)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to suspend before the snapshot operation"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "initiator is not initialized"), Equals, true)
}
