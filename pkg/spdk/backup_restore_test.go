package spdk

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// newTestReplicaBackend is a test-only helper that builds a replicaBackend
// with explicit mode/address so the tests can exercise mode-state branches in
// engine helpers without going through connectNVMfBdev.
func newTestReplicaBackend(name, address string, mode lhtypes.Mode) *replicaBackend {
	u := newReplicaBackend(name, address, nil)
	u.SetMode(mode)
	return u
}

// Tests for ensureReplicaModeForInfoUpdate, which is called by
// checkAndUpdateInfoFromReplicasNoLock - a function now invoked at the end of
// BackupRestoreFinish to refresh engine state from replica info.
func (s *TestSuite) TestEnsureReplicaModeForInfoUpdate(c *C) {
	fmt.Println("Testing ensureReplicaModeForInfoUpdate with various replica modes")

	type testCase struct {
		mode         lhtypes.Mode
		expectedOK   bool
		expectedMode lhtypes.Mode
	}
	testCases := map[string]testCase{
		"RW qualifies": {
			mode:         lhtypes.ModeRW,
			expectedOK:   true,
			expectedMode: lhtypes.ModeRW,
		},
		"WO qualifies": {
			mode:         lhtypes.ModeWO,
			expectedOK:   true,
			expectedMode: lhtypes.ModeWO,
		},
		"ERR does not qualify": {
			mode:         lhtypes.ModeERR,
			expectedOK:   false,
			expectedMode: lhtypes.ModeERR,
		},
		"unexpected mode downgrades to ERR": {
			mode:         lhtypes.Mode("UNKNOWN"),
			expectedOK:   false,
			expectedMode: lhtypes.ModeERR,
		},
	}
	for testName, tc := range testCases {
		c.Logf("testing ensureReplicaModeForInfoUpdate.%v", testName)

		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
		rs := newTestReplicaBackend("replica-1", "10.0.0.1:1234", tc.mode)

		ok := e.ensureReplicaModeForInfoUpdate("replica-1", rs)

		c.Assert(ok, Equals, tc.expectedOK)
		c.Assert(rs.Mode(), Equals, tc.expectedMode)
	}
}

// Tests for checkAndUpdateInfoFromReplicasNoLock with edge-case replica maps.
// This function is now called by BackupRestoreFinish after setting replicas
// to ModeRW.
func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockEmptyMap(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock: empty backends does not panic")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{}

	// Should not panic with empty map
	e.checkAndUpdateInfoFromReplicasNoLock()
}

func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockAllERRSkipped(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock: all-ERR replicas are skipped without network calls")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"replica-1": newTestReplicaBackend("replica-1", "10.0.0.1:1234", lhtypes.ModeERR),
		"replica-2": newTestReplicaBackend("replica-2", "10.0.0.2:1234", lhtypes.ModeERR),
	}

	// All replicas are ERR, so ensureReplicaModeForInfoUpdate returns false
	// for each one. No inspectReplicaForInfoUpdate or network calls occur.
	e.checkAndUpdateInfoFromReplicasNoLock()

	// Modes remain ERR (not downgraded further)
	c.Assert(e.backends["replica-1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["replica-2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
}

func (s *TestSuite) TestEngineFrontendTeardownRestoreInitiatorMarksStopped(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreInitiator marks the temporary restore frontend stopped")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
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

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.State = lhtypes.InstanceStateRunning
	e.IsRestoring = true

	err := e.ReplicaAdd(nil, "replica-new", "10.0.0.2:20000", false, "", "", "", nil)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "restore is in progress"), Equals, true)
}

func (s *TestSuite) TestRecordBackupRestoreStartErrorExposedInRestoreStatus(c *C) {
	fmt.Println("Testing restore start errors are exposed through Engine.RestoreStatus")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"replica-1": newTestReplicaBackend("replica-1", "10.0.0.1:1234", lhtypes.ModeRW),
		"replica-2": newTestReplicaBackend("replica-2", "10.0.0.2:1234", lhtypes.ModeRW),
	}
	backupURL := "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a"
	restoreErr := fmt.Errorf("backup was deleted")

	e.Lock()
	e.recordBackupRestoreStartErrorLocked(nil, backupURL, "", nil, restoreErr)
	e.Unlock()

	status, err := e.RestoreStatus()
	c.Assert(err, IsNil)
	c.Assert(status.Status, HasLen, 2)

	// The start error is not attributed to any replica, so it must surface as
	// an engine-level error and must not be fanned out to replica entries.
	c.Assert(status.EngineError, Equals, restoreErr.Error())
	for _, replicaStatus := range status.Status {
		c.Assert(replicaStatus.IsRestoring, Equals, false)
		c.Assert(replicaStatus.LastRestored, Equals, "")
		c.Assert(replicaStatus.CurrentRestoringBackup, Equals, "backup-a")
		c.Assert(replicaStatus.BackupUrl, Equals, backupURL)
		c.Assert(replicaStatus.State, Equals, "error")
		c.Assert(replicaStatus.Error, Equals, "")
	}
}

func (s *TestSuite) TestRestoreStatusReportsErrorOnlyOnErrorSourceReplica(c *C) {
	fmt.Println("Testing RestoreStatus reports the restore error only on the error source replica")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"replica-1": newTestReplicaBackend("replica-1", "10.0.0.1:1234", lhtypes.ModeRW),
		"replica-2": newTestReplicaBackend("replica-2", "10.0.0.2:1234", lhtypes.ModeRW),
	}
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	e.restore.UpdateRestoreStatus("", 40, fmt.Errorf("qpair wedged"))
	e.restore.RecordErrorSource("replica-1")

	status, err := e.RestoreStatus()
	c.Assert(err, IsNil)
	c.Assert(status.EngineError, Equals, "")
	c.Assert(status.Status["10.0.0.1:1234"].Error, Equals, "qpair wedged")
	c.Assert(status.Status["10.0.0.2:1234"].Error, Equals, "")
}

func (s *TestSuite) TestRecordErrorSourceKeepsFirstRecordedName(c *C) {
	fmt.Println("Testing EngineRestore.RecordErrorSource keeps the first recorded error source")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)

	e.restore.RecordErrorSource("replica-1")
	e.restore.RecordErrorSource("replica-2")
	c.Assert(e.restore.ErrorSourceReplicaName, Equals, "replica-1")

	// A new restore cycle clears the attribution.
	e.restore.StartNewRestore("s3://backupbucket@us-east-1/backupstore?backup=backup-b&volume=vol-a", "backup-b", true)
	c.Assert(e.restore.ErrorSourceReplicaName, Equals, "")
}

func (s *TestSuite) TestRestoreStatusErrorSourceLeftMembershipFallsBackToEngineError(c *C) {
	fmt.Println("Testing RestoreStatus reports engine-level error when the error source is no longer a member")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"replica-2": newTestReplicaBackend("replica-2", "10.0.0.2:1234", lhtypes.ModeRW),
	}
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	e.restore.UpdateRestoreStatus("", 40, fmt.Errorf("qpair wedged"))
	e.restore.RecordErrorSource("replica-1") // not in e.backends anymore

	status, err := e.RestoreStatus()
	c.Assert(err, IsNil)
	c.Assert(status.EngineError, Equals, "qpair wedged")
	c.Assert(status.Status["10.0.0.2:1234"].Error, Equals, "")
}

func (s *TestSuite) TestRecordBackupRestoreStartErrorPreservesLastRestored(c *C) {
	fmt.Println("Testing restore start errors preserve last restored backup")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-old&volume=vol-a", "backup-old", e, nil)
	e.restore.FinishRestore()

	e.Lock()
	e.recordBackupRestoreStartErrorLocked(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-new&volume=vol-a", "", nil, fmt.Errorf("backup was deleted"))
	e.Unlock()

	c.Assert(e.restore.LastRestored, Equals, "backup-old")
	c.Assert(e.restore.CurrentRestoringBackup, Equals, "backup-new")
	c.Assert(string(e.restore.State), Equals, "error")
}

func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockAppliesBackendView(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock applies SnapshotMap/Head/ActualSize from Backend.Get()")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(lhtypes.ModeRW)
	headLvol := &api.Lvol{Name: "vol-head", Parent: "snap-1"}
	snap := &api.Lvol{Name: "snap-1", CreationTime: time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)}
	u.View = &BackendView{
		SpecSize:   100,
		ActualSize: 50,
		Head:       headLvol,
		Snapshots:  map[string]*api.Lvol{"snap-1": snap},
	}
	e.backends = map[string]Backend{"r1": u}

	e.checkAndUpdateInfoFromReplicasNoLock()

	c.Assert(e.SnapshotMap, NotNil)
	c.Assert(e.SnapshotMap["snap-1"], Not(IsNil))
	c.Assert(e.Head, Not(IsNil))
	c.Assert(e.Head.Name, Equals, "vol-head")
	c.Assert(e.ActualSize, Equals, uint64(50))
}

func (s *TestSuite) TestCheckAndUpdateInfoFromReplicasNoLockMarksERROnGetError(c *C) {
	fmt.Println("Testing checkAndUpdateInfoFromReplicasNoLock marks backend ERR when Backend.Get() returns an error")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(lhtypes.ModeRW)
	u.ViewErr = errors.New("backend unavailable")
	e.backends = map[string]Backend{"r1": u}

	e.checkAndUpdateInfoFromReplicasNoLock()

	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
}

func (s *TestSuite) TestResolveReplicaAncestorRoutesBackingImageThroughBackend(c *C) {
	fmt.Println("Testing resolveReplicaAncestor calls BackingImageGet via the Backend interface")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(lhtypes.ModeRW)
	biSnap := &api.Lvol{Name: "bi-snap"}
	u.BackingImageView = &api.BackingImage{Snapshot: biSnap}
	view := &BackendView{
		BackingImageName: "ubuntu-22.04",
		LvsUUID:          "lvs-uuid-1",
		Head:             &api.Lvol{Name: "vol-head", Parent: "bi-snap"},
		Snapshots:        map[string]*api.Lvol{},
	}

	ancestor, foundBI, foundSnap, ok := e.resolveReplicaAncestor("r1", view, u, false, false)
	c.Assert(ok, Equals, true)
	c.Assert(foundBI, Equals, true)
	c.Assert(foundSnap, Equals, false) // no snapshots on this backend
	c.Assert(ancestor, Equals, biSnap)
	// BackingImageGet was forwarded via the Backend interface, not directly to a replica client.
	c.Assert(len(u.BackingImageCalls), Equals, 1)
	c.Assert(u.BackingImageCalls[0].Name, Equals, "ubuntu-22.04")
	c.Assert(u.BackingImageCalls[0].LvsUUID, Equals, "lvs-uuid-1")
}

func (s *TestSuite) TestResolveReplicaAncestorMarksERROnBackingImageError(c *C) {
	fmt.Println("Testing resolveReplicaAncestor marks backend ERR when BackingImageGet fails")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	u := newFakeBackend("r1", "10.0.0.1:1234")
	u.SetMode(lhtypes.ModeRW)
	u.BackingImageGetErr = errors.New("backing image not found")
	view := &BackendView{
		BackingImageName: "ubuntu-22.04",
		LvsUUID:          "lvs-uuid-1",
		Head:             &api.Lvol{Name: "vol-head", Parent: "bi-snap"},
		Snapshots:        map[string]*api.Lvol{},
	}

	_, _, _, ok := e.resolveReplicaAncestor("r1", view, u, false, false)
	c.Assert(ok, Equals, false)
	c.Assert(u.Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
}
