package spdk

import (
	"fmt"
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestReplicaAddFinalizeUsesFinishWrapper(c *C) {
	fmt.Println("Testing ReplicaAddFinalize uses finish wrapper")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	order := []string{}

	mock := &ReplicaAddMock{
		ShallowCopy: func(_ *client.SPDKClient, _ string, _ string, _ []*api.Lvol, _ bool) error {
			order = append(order, "shallow")
			return nil
		},
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			order = append(order, "finish")
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	wrapperCalled := false
	wrapper := func(finish func() error) error {
		wrapperCalled = true
		order = append(order, "wrapper")
		return finish()
	}

	task := &replicaAddTask{
		srcReplicaName:      "src-r-00000001",
		srcReplicaAddress:   "127.0.0.1:8504",
		dstReplicaName:      "dst-r-00000001",
		dstReplicaAddress:   "127.0.0.1:8504",
		rebuildingSnapshots: []*api.Lvol{},
	}

	err := e.replicaAddFinalize(task, wrapper)
	c.Assert(err, IsNil)

	c.Assert(wrapperCalled, Equals, true)
	c.Assert(len(order), Equals, 3)
	c.Assert(order[0], Equals, "shallow")
	c.Assert(order[1], Equals, "wrapper")
	c.Assert(order[2], Equals, "finish")
}

func (s *TestSuite) TestReplicaAddFinalizeSkipsShallowCopyIfDone(c *C) {
	fmt.Println("Testing ReplicaAddFinalize skips shallow copy if already done")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	order := []string{}

	mock := &ReplicaAddMock{
		ShallowCopy: func(_ *client.SPDKClient, _ string, _ string, _ []*api.Lvol, _ bool) error {
			order = append(order, "shallow")
			return nil
		},
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			order = append(order, "finish")
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	wrapperCalled := false
	wrapper := func(finish func() error) error {
		wrapperCalled = true
		order = append(order, "wrapper")
		return finish()
	}

	task := &replicaAddTask{
		srcReplicaName:      "src-r-00000001",
		srcReplicaAddress:   "127.0.0.1:8504",
		dstReplicaName:      "dst-r-00000001",
		dstReplicaAddress:   "127.0.0.1:8504",
		rebuildingSnapshots: []*api.Lvol{},
		shallowCopyDone:     true,
	}

	err := e.replicaAddFinalize(task, wrapper)
	c.Assert(err, IsNil)

	c.Assert(wrapperCalled, Equals, true)
	c.Assert(len(order), Equals, 2)
	c.Assert(order[0], Equals, "wrapper")
	c.Assert(order[1], Equals, "finish")
}

func (s *TestSuite) TestReplicaAddFinalizeWithoutFinishWrapper(c *C) {
	fmt.Println("Testing ReplicaAddFinalize without finish wrapper")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	order := []string{}

	mock := &ReplicaAddMock{
		ShallowCopy: func(_ *client.SPDKClient, _ string, _ string, _ []*api.Lvol, _ bool) error {
			order = append(order, "shallow")
			return nil
		},
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			order = append(order, "finish")
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	task := &replicaAddTask{
		srcReplicaName:      "src-r-00000001",
		srcReplicaAddress:   "127.0.0.1:8504",
		dstReplicaName:      "dst-r-00000001",
		dstReplicaAddress:   "127.0.0.1:8504",
		rebuildingSnapshots: []*api.Lvol{},
	}

	err := e.replicaAddFinalize(task, nil)
	c.Assert(err, IsNil)

	c.Assert(len(order), Equals, 2)
	c.Assert(order[0], Equals, "shallow")
	c.Assert(order[1], Equals, "finish")
}

func (s *TestSuite) TestReplicaAddFinalizeWithoutFinishWrapperAndShallowCopyDone(c *C) {
	fmt.Println("Testing ReplicaAddFinalize without finish wrapper and shallow copy already done")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	order := []string{}

	mock := &ReplicaAddMock{
		ShallowCopy: func(_ *client.SPDKClient, _ string, _ string, _ []*api.Lvol, _ bool) error {
			order = append(order, "shallow")
			return nil
		},
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			order = append(order, "finish")
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	task := &replicaAddTask{
		srcReplicaName:      "src-r-00000001",
		srcReplicaAddress:   "127.0.0.1:8504",
		dstReplicaName:      "dst-r-00000001",
		dstReplicaAddress:   "127.0.0.1:8504",
		rebuildingSnapshots: []*api.Lvol{},
		shallowCopyDone:     true,
	}

	err := e.replicaAddFinalize(task, nil)
	c.Assert(err, IsNil)

	c.Assert(len(order), Equals, 1)
	c.Assert(order[0], Equals, "finish")
}

func (s *TestSuite) TestReplicaAddFinishCanRetryAfterFailure(c *C) {
	fmt.Println("Testing ReplicaAddFinish can retry after failure")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	finishCallCount := 0
	mock := &ReplicaAddMock{
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			finishCallCount++
			if finishCallCount == 1 {
				return fmt.Errorf("injected finish error")
			}
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	const dstReplicaName = "dst-r-00000001"
	task := &replicaAddTask{
		srcReplicaName:    "src-r-00000001",
		srcReplicaAddress: "127.0.0.1:8504",
		dstReplicaName:    dstReplicaName,
		dstReplicaAddress: "127.0.0.1:8504",
		lastActivityAt:    time.Now(),
	}
	e.pendingReplicaAddTasks[dstReplicaName] = task

	err := e.ReplicaAddFinish(dstReplicaName, nil)
	c.Assert(err, NotNil)
	c.Assert(e.hasPendingReplicaAddTask(dstReplicaName), Equals, true)
	c.Assert(task.inProgress, Equals, false)
	c.Assert(task.lastError, Matches, ".*injected finish error.*")

	err = e.ReplicaAddFinish(dstReplicaName, nil)
	c.Assert(err, IsNil)
	c.Assert(e.hasPendingReplicaAddTask(dstReplicaName), Equals, false)
	c.Assert(finishCallCount, Equals, 2)
}

func (s *TestSuite) TestReplicaAddFinishCanRetryAfterFailureAndShallowCopyDone(c *C) {
	fmt.Println("Testing ReplicaAddFinish can retry after failure and shallow copy already done")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	finishCallCount := 0
	mock := &ReplicaAddMock{
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			finishCallCount++
			if finishCallCount == 1 {
				return fmt.Errorf("injected finish error")
			}
			return nil
		},
	}
	e.SetReplicaAddMock(mock)

	const dstReplicaName = "dst-r-00000001"
	task := &replicaAddTask{
		srcReplicaName:    "src-r-00000001",
		srcReplicaAddress: "127.0.0.1:8504",
		dstReplicaName:    dstReplicaName,
		dstReplicaAddress: "127.0.0.1:8504",
		lastActivityAt:    time.Now(),
		shallowCopyDone:   true,
	}
	e.pendingReplicaAddTasks[dstReplicaName] = task

	err := e.ReplicaAddFinish(dstReplicaName, nil)
	c.Assert(err, NotNil)
	c.Assert(e.hasPendingReplicaAddTask(dstReplicaName), Equals, true)
	c.Assert(task.inProgress, Equals, false)
	c.Assert(task.lastError, Matches, ".*injected finish error.*")
	c.Assert(task.shallowCopyDone, Equals, true)

	err = e.ReplicaAddFinish(dstReplicaName, nil)
	c.Assert(err, IsNil)
	c.Assert(e.hasPendingReplicaAddTask(dstReplicaName), Equals, false)
	c.Assert(finishCallCount, Equals, 2)
}

func (s *TestSuite) TestReplicaAddFinishFailureCleansPendingTaskForStartFlow(c *C) {
	fmt.Println("Testing ReplicaAddFinish failure cleans pending task for start flow")

	e := NewEngine("e1", "vol1", "", 1024, make(chan interface{}, 1))

	mock := &ReplicaAddMock{
		Finish: func(_ *client.SPDKClient, _ *client.SPDKClient, _ string, _ string, _ bool) error {
			return fmt.Errorf("injected finish error")
		},
	}
	e.SetReplicaAddMock(mock)

	const dstReplicaName = "dst-r-00000001"
	task := &replicaAddTask{
		srcReplicaName:           "src-r-00000001",
		srcReplicaAddress:        "127.0.0.1:8504",
		dstReplicaName:           dstReplicaName,
		dstReplicaAddress:        "127.0.0.1:8504",
		lastActivityAt:           time.Now(),
		createdByReplicaAddStart: true,
	}
	e.pendingReplicaAddTasks[dstReplicaName] = task

	err := e.ReplicaAddFinish(dstReplicaName, nil)
	c.Assert(err, NotNil)
	c.Assert(e.hasPendingReplicaAddTask(dstReplicaName), Equals, false)
}
