package spdk

import (
	"fmt"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// Tests for ensureReplicaModeForInfoUpdate, which is called by
// checkAndUpdateInfoFromReplicasNoLock — a function now invoked at the end of
// BackupRestoreFinish to refresh engine state from replica info.
func (s *TestSuite) TestEnsureReplicaModeForInfoUpdateRWQualifies(c *C) {
	fmt.Println("Testing ensureReplicaModeForInfoUpdate: RW mode qualifies for info update")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	rs := &EngineReplicaStatus{Mode: lhtypes.ModeRW, Address: "10.0.0.1:1234"}

	ok := e.ensureReplicaModeForInfoUpdate("replica-1", rs)

	c.Assert(ok, Equals, true)
	c.Assert(rs.Mode, Equals, lhtypes.Mode(lhtypes.ModeRW))
}

func (s *TestSuite) TestEnsureReplicaModeForInfoUpdateWOQualifies(c *C) {
	fmt.Println("Testing ensureReplicaModeForInfoUpdate: WO mode qualifies for info update")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	rs := &EngineReplicaStatus{Mode: lhtypes.ModeWO, Address: "10.0.0.1:1234"}

	ok := e.ensureReplicaModeForInfoUpdate("replica-1", rs)

	c.Assert(ok, Equals, true)
	c.Assert(rs.Mode, Equals, lhtypes.Mode(lhtypes.ModeWO))
}

func (s *TestSuite) TestEnsureReplicaModeForInfoUpdateERRDoesNotQualify(c *C) {
	fmt.Println("Testing ensureReplicaModeForInfoUpdate: ERR mode does not qualify")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	rs := &EngineReplicaStatus{Mode: lhtypes.ModeERR, Address: "10.0.0.1:1234"}

	ok := e.ensureReplicaModeForInfoUpdate("replica-1", rs)

	c.Assert(ok, Equals, false)
	c.Assert(rs.Mode, Equals, lhtypes.Mode(lhtypes.ModeERR))
}

func (s *TestSuite) TestEnsureReplicaModeForInfoUpdateUnexpectedModeDowngradesToERR(c *C) {
	fmt.Println("Testing ensureReplicaModeForInfoUpdate: unexpected mode is downgraded to ERR")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	rs := &EngineReplicaStatus{Mode: lhtypes.Mode("UNKNOWN"), Address: "10.0.0.1:1234"}

	ok := e.ensureReplicaModeForInfoUpdate("replica-1", rs)

	c.Assert(ok, Equals, false)
	c.Assert(rs.Mode, Equals, lhtypes.Mode(lhtypes.ModeERR))
}

// Tests for checkAndUpdateInfoFromReplicasNoLock with edge-case replica maps.
// This function is now called by BackupRestoreFinish after setting replicas
// to ModeRW.
func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockEmptyMap(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock: empty ReplicaStatusMap does not panic")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{}

	// Should not panic with empty map
	e.checkAndUpdateInfoFromReplicasNoLock()
}

func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockAllERRSkipped(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock: all-ERR replicas are skipped without network calls")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1))
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"replica-1": {Mode: lhtypes.ModeERR, Address: "10.0.0.1:1234"},
		"replica-2": {Mode: lhtypes.ModeERR, Address: "10.0.0.2:1234"},
	}

	// All replicas are ERR, so ensureReplicaModeForInfoUpdate returns false
	// for each one. No inspectReplicaForInfoUpdate or network calls occur.
	e.checkAndUpdateInfoFromReplicasNoLock()

	// Modes remain ERR (not downgraded further)
	c.Assert(e.ReplicaStatusMap["replica-1"].Mode, Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.ReplicaStatusMap["replica-2"].Mode, Equals, lhtypes.Mode(lhtypes.ModeERR))
}
