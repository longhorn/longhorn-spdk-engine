package spdk

import (
	"fmt"
	"strings"

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

func (s *TestSuite) TestEngineFrontendTeardownRestoreInitiatorMarksStopped(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreInitiator marks the temporary restore frontend stopped")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1))
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 2000
	ef.NvmeTcpFrontend.Nqn = getStableVolumeNQN("vol-a")
	ef.NvmeTcpFrontend.Nguid = getStableVolumeNGUID("vol-a")

	ef.teardownRestoreInitiator(nil)

	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateStopped))
	c.Assert(ef.Frontend, Equals, "")
	c.Assert(ef.Endpoint, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(0))
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, "")
}

func (s *TestSuite) TestEngineReplicaAddRejectedDuringRestore(c *C) {
	fmt.Println("Testing Engine.ReplicaAdd is rejected while restore is in progress")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1))
	e.State = lhtypes.InstanceStateRunning
	e.IsRestoring = true

	err := e.ReplicaAdd(nil, "replica-new", "10.0.0.2:20000", false, nil)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "restore is in progress"), Equals, true)
}

func (s *TestSuite) TestRecordBackupRestoreStartErrorExposedInRestoreStatus(c *C) {
	fmt.Println("Testing restore start errors are exposed through Engine.RestoreStatus")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1))
	e.ReplicaStatusMap = map[string]*EngineReplicaStatus{
		"replica-1": {Mode: lhtypes.ModeRW, Address: "10.0.0.1:1234"},
		"replica-2": {Mode: lhtypes.ModeRW, Address: "10.0.0.2:1234"},
	}
	backupURL := "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a"
	restoreErr := fmt.Errorf("backup was deleted")

	e.Lock()
	e.recordBackupRestoreStartErrorLocked(nil, backupURL, "", nil, restoreErr)
	e.Unlock()

	status, err := e.RestoreStatus()
	c.Assert(err, IsNil)
	c.Assert(status.Status, HasLen, 2)

	for _, replicaStatus := range status.Status {
		c.Assert(replicaStatus.IsRestoring, Equals, false)
		c.Assert(replicaStatus.LastRestored, Equals, "")
		c.Assert(replicaStatus.CurrentRestoringBackup, Equals, "backup-a")
		c.Assert(replicaStatus.BackupUrl, Equals, backupURL)
		c.Assert(replicaStatus.State, Equals, "error")
		c.Assert(replicaStatus.Error, Equals, restoreErr.Error())
	}
}

func (s *TestSuite) TestRecordBackupRestoreStartErrorPreservesLastRestored(c *C) {
	fmt.Println("Testing restore start errors preserve last restored backup")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1))
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-old&volume=vol-a", "backup-old", e, nil)
	e.restore.FinishRestore()

	e.Lock()
	e.recordBackupRestoreStartErrorLocked(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-new&volume=vol-a", "", nil, fmt.Errorf("backup was deleted"))
	e.Unlock()

	c.Assert(e.restore.LastRestored, Equals, "backup-old")
	c.Assert(e.restore.CurrentRestoringBackup, Equals, "backup-new")
	c.Assert(string(e.restore.State), Equals, "error")
}
