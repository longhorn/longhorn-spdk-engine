package spdk

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	btypes "github.com/longhorn/backupstore/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

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

	c.Assert(ef.teardownRestoreInitiator(), IsNil)

	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateStopped))
	c.Assert(ef.Frontend, Equals, "")
	c.Assert(ef.Endpoint, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.TargetIP, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.TargetPort, Equals, int32(0))
	c.Assert(ef.NvmeTcpFrontend.Nqn, Equals, "")
	c.Assert(ef.NvmeTcpFrontend.Nguid, Equals, "")
}

func (s *TestSuite) TestEngineFrontendTeardownRestoreInitiatorKeepsTerminating(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreInitiator does not overwrite the terminating state")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateTerminating
	ef.IsRestoring = true

	c.Assert(ef.teardownRestoreInitiator(), IsNil)

	// No current caller reaches teardownRestoreInitiator with a terminating
	// frontend. This checks the guard itself, not a real flow: a terminating
	// frontend is never marked stopped.
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateTerminating))
}

func (s *TestSuite) TestEngineFrontendTeardownRestoreInitiatorReleasesLockDuringDisconnect(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreInitiator does not hold the frontend lock while the disconnect is blocked")

	originalStop := stopRestoreInitiator
	defer func() { stopRestoreInitiator = originalStop }()

	disconnectEntered := make(chan struct{})
	releaseDisconnect := make(chan struct{})
	stopRestoreInitiator = func(*initiator.Initiator) error {
		close(disconnectEntered)
		<-releaseDisconnect
		return nil
	}

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.initiator = &initiator.Initiator{}

	done := make(chan error, 1)
	go func() { done <- ef.teardownRestoreInitiator() }()

	select {
	case <-disconnectEntered:
	case <-time.After(5 * time.Second):
		c.Fatal("the disconnect was never entered")
	}

	// The server reads the restore frontend under its own lock while
	// rejecting a create or a restore; that read must not wait for the
	// disconnect.
	c.Assert(ef.TryRLock(), Equals, true)
	ef.RUnlock()

	close(releaseDisconnect)
	select {
	case err := <-done:
		c.Assert(err, IsNil)
	case <-time.After(5 * time.Second):
		c.Fatal("teardownRestoreInitiator did not return after the disconnect was released")
	}
	c.Assert(ef.initiator, IsNil)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateStopped))
}

func (s *TestSuite) TestTeardownRestoreFrontendSuccessStopsAndUnregisters(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreFrontend stops the frontend and unregisters it after a successful teardown")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true
	unregistered := false
	ef.unregisterRestoreFrontendFn = func() { unregistered = true }

	ef.teardownRestoreFrontend()

	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateStopped))
	c.Assert(ef.ErrorMsg, Equals, "")
	c.Assert(ef.IsRestoring, Equals, false)
	// The data path is gone, so the engine can accept a new restore.
	c.Assert(unregistered, Equals, true)
}

func (s *TestSuite) TestTeardownRestoreFrontendExhaustionMarksErrorAndStaysRegistered(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreFrontend marks the frontend error, keeps the initiator handle and stays registered when retries are exhausted")

	originalInterval := restoreInitiatorTeardownRetryInterval
	originalTimeout := restoreInitiatorTeardownTimeout
	defer func() {
		restoreInitiatorTeardownRetryInterval = originalInterval
		restoreInitiatorTeardownTimeout = originalTimeout
	}()
	restoreInitiatorTeardownRetryInterval = time.Millisecond
	restoreInitiatorTeardownTimeout = 5 * time.Millisecond

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true
	// An initiator without NVMe-TCP info makes StopDisconnectFirst fail
	// deterministically without touching the host.
	failingInitiator, err := initiator.NewInitiator("vol-a", "", nil, &initiator.UblkInfo{BdevName: "bdev-a"})
	c.Assert(err, IsNil)
	ef.initiator = failingInitiator
	unregistered := false
	ef.unregisterRestoreFrontendFn = func() { unregistered = true }

	ef.teardownRestoreFrontend()

	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateError))
	c.Assert(strings.Contains(ef.ErrorMsg, "failed to tear down the restore initiator"), Equals, true)
	// The handle stays with the errored frontend; the error message is the
	// only record that the kernel controller may still be connected.
	c.Assert(ef.initiator, NotNil)
	c.Assert(ef.IsRestoring, Equals, false)
	// The data path is still up, so the engine must keep rejecting new
	// restores.
	c.Assert(unregistered, Equals, false)
}

func (s *TestSuite) TestTeardownRestoreFrontendDeletionStopsRetriesAndUnregisters(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreFrontend stops retrying, leaves the state alone and unregisters the frontend when the engine is being deleted")

	originalInterval := restoreInitiatorTeardownRetryInterval
	originalTimeout := restoreInitiatorTeardownTimeout
	defer func() {
		restoreInitiatorTeardownRetryInterval = originalInterval
		restoreInitiatorTeardownTimeout = originalTimeout
	}()
	// A long timeout proves the early return comes from the closed stop
	// channel, not from retry exhaustion.
	restoreInitiatorTeardownRetryInterval = 10 * time.Second
	restoreInitiatorTeardownTimeout = time.Hour

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true
	// An initiator without NVMe-TCP info makes StopDisconnectFirst fail
	// deterministically without touching the host.
	failingInitiator, err := initiator.NewInitiator("vol-a", "", nil, &initiator.UblkInfo{BdevName: "bdev-a"})
	c.Assert(err, IsNil)
	ef.initiator = failingInitiator
	unregistered := false
	ef.unregisterRestoreFrontendFn = func() { unregistered = true }

	done := make(chan struct{})
	go func() {
		ef.teardownRestoreFrontend()
		close(done)
	}()
	// Signal the deletion after the first attempt has failed and the loop is
	// waiting for the next retry.
	time.Sleep(100 * time.Millisecond)
	ef.signalStop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		c.Fatal("teardownRestoreFrontend did not return after the stop channel was closed")
	}

	// The frontend state is left as is, so a late disconnect cannot hit a
	// recreated engine on the same NQN.
	c.Assert(ef.initiator, NotNil)
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
	c.Assert(ef.IsRestoring, Equals, true)
	// The deletion must not leave the engine rejecting new restores forever.
	c.Assert(unregistered, Equals, true)
}

func (s *TestSuite) TestTeardownRestoreFrontendSkipsFirstAttemptOnDeletion(c *C) {
	fmt.Println("Testing EngineFrontend.teardownRestoreFrontend makes no teardown attempt when the engine was deleted before it started")

	updateCh := make(chan interface{}, 1)
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, updateCh, nil)
	ef.State = lhtypes.InstanceStateRunning
	ef.IsRestoring = true
	// With no initiator handle, an attempt would succeed and clear the
	// endpoint. The endpoint staying set proves no attempt was made.
	ef.Endpoint = "/dev/longhorn/vol-a"
	unregistered := false
	ef.unregisterRestoreFrontendFn = func() { unregistered = true }
	ef.signalStop()

	ef.teardownRestoreFrontend()

	c.Assert(ef.Endpoint, Equals, "/dev/longhorn/vol-a")
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
	c.Assert(ef.IsRestoring, Equals, true)
	c.Assert(len(updateCh), Equals, 0)
	c.Assert(unregistered, Equals, true)
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

func (s *TestSuite) TestRestoreCycleContextEndsWithTheCycle(c *C) {
	fmt.Println("Testing the restore cycle context is canceled when the cycle ends and replaced for the next cycle")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	c.Assert(e.restore.Context().Err(), IsNil)

	// Recording the outcome ends the cycle: a block producer still blocked
	// on the bounded channel must exit with the workers.
	e.restore.FinalizeRestore(fmt.Errorf("restore idle"))
	c.Assert(e.restore.Context().Err(), NotNil)

	// The next cycle gets its own context.
	e.restore.StartNewRestore("s3://backupbucket@us-east-1/backupstore?backup=backup-b&volume=vol-a", "backup-b", false)
	c.Assert(e.restore.Context().Err(), IsNil)

	// The stop signal (idle abort, engine deletion) cancels it as well.
	e.restore.signalStop()
	c.Assert(e.restore.Context().Err(), NotNil)
}

func (s *TestSuite) TestRecordBackupRestoreStartErrorPreservesLastRestored(c *C) {
	fmt.Println("Testing restore start errors preserve last restored backup")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-old&volume=vol-a", "backup-old", e, nil)
	e.restore.FinalizeRestore(nil)

	e.Lock()
	e.recordBackupRestoreStartErrorLocked(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-new&volume=vol-a", "", nil, fmt.Errorf("backup was deleted"))
	e.Unlock()

	c.Assert(e.restore.LastRestored, Equals, "backup-old")
	c.Assert(e.restore.CurrentRestoringBackup, Equals, "backup-new")
	c.Assert(string(e.restore.State), Equals, "error")
}

func (s *TestSuite) TestFinalizeRestoreIgnoresLateWorkerReports(c *C) {
	fmt.Println("Testing EngineRestore ignores worker status reports after the outcome is recorded")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	e.restore.UpdateRestoreStatus("", 40, nil)

	idleErr := fmt.Errorf("restore idle at 40%% for 5m0s, timeout 5m0s")
	e.restore.FinalizeRestore(idleErr)
	c.Assert(e.restore.State, Equals, btypes.ProgressStateError)
	c.Assert(e.restore.Error, Equals, idleErr.Error())

	// A worker that was blocked in a write reports after the initiator is
	// torn down. Its cancelled error and progress must not change the
	// recorded outcome.
	e.restore.UpdateRestoreStatus("", 40, fmt.Errorf("write failed: %v", btypes.ErrorMsgRestoreCancelled))
	e.restore.UpdateRestoreStatus("", 100, nil)
	c.Assert(e.restore.State, Equals, btypes.ProgressStateError)
	c.Assert(e.restore.Error, Equals, idleErr.Error())
	c.Assert(e.restore.Progress, Equals, 0)

	// Engine deletion after the outcome is recorded does not rewrite it either.
	e.restore.Stop()
	c.Assert(e.restore.State, Equals, btypes.ProgressStateError)
	c.Assert(e.restore.Error, Equals, idleErr.Error())

	// A new cycle accepts reports again.
	e.restore.StartNewRestore("s3://backupbucket@us-east-1/backupstore?backup=backup-b&volume=vol-a", "backup-b", false)
	e.restore.UpdateRestoreStatus("", 10, nil)
	c.Assert(e.restore.Progress, Equals, 10)
	c.Assert(e.restore.State, Equals, btypes.ProgressStateInProgress)
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

func (s *TestSuite) TestRestoreIdleTimeout(c *C) {
	fmt.Println("Testing restoreIdleTimeout scales with volume size above the floor")

	const gib = uint64(1 << 30)

	type testCase struct {
		specSize uint64
		expected time.Duration
	}
	testCases := map[string]testCase{
		"zero size uses the floor": {
			specSize: 0,
			expected: 10 * time.Minute,
		},
		"small volume uses the floor": {
			specSize: 1 * gib,
			expected: 10 * time.Minute,
		},
		"partial GiB rounds up": {
			specSize: 1,
			expected: 10 * time.Minute,
		},
		"600 GiB matches the floor exactly": {
			specSize: 600 * gib,
			expected: 10 * time.Minute,
		},
		"601 GiB exceeds the floor": {
			specSize: 601 * gib,
			expected: 601 * time.Second,
		},
		"4 TiB scales linearly": {
			specSize: 4096 * gib,
			expected: 4096 * time.Second,
		},
	}
	for testName, tc := range testCases {
		c.Logf("testing restoreIdleTimeout.%v", testName)
		c.Assert(restoreIdleTimeout(tc.specSize), Equals, tc.expected)
	}
}

func (s *TestSuite) TestNvmeIOPathIsUsable(c *C) {
	fmt.Println("Testing nvmeIOPathIsUsable path state interpretation")

	type testCase struct {
		connected  bool
		accessible bool
		state      spdktypes.BdevNvmeQpairState
		expected   bool
	}
	testCases := map[string]testCase{
		"connected with enabled qpair is usable": {
			connected:  true,
			accessible: true,
			state:      spdktypes.BdevNvmeQpairStateEnabled,
			expected:   true,
		},
		"connected with connected qpair is usable": {
			connected:  true,
			accessible: true,
			state:      spdktypes.BdevNvmeQpairStateConnected,
			expected:   true,
		},
		"connected without reported qpair state falls back to the connected and accessible flags": {
			connected:  true,
			accessible: true,
			state:      "",
			expected:   true,
		},
		"qpair stuck in connecting is not usable": {
			connected:  true,
			accessible: true,
			state:      spdktypes.BdevNvmeQpairStateConnecting,
			expected:   false,
		},
		"disconnected qpair is not usable": {
			connected:  true,
			accessible: true,
			state:      spdktypes.BdevNvmeQpairStateDisconnected,
			expected:   false,
		},
		"not connected is never usable": {
			connected:  false,
			accessible: true,
			state:      spdktypes.BdevNvmeQpairStateEnabled,
			expected:   false,
		},
		"inaccessible namespace is not usable": {
			connected:  true,
			accessible: false,
			state:      spdktypes.BdevNvmeQpairStateEnabled,
			expected:   false,
		},
	}
	for testName, tc := range testCases {
		c.Logf("testing nvmeIOPathIsUsable.%v", testName)
		ioPath := spdktypes.BdevNvmeIoPath{Connected: tc.connected, Accessible: tc.accessible, State: tc.state}
		c.Assert(nvmeIOPathIsUsable(ioPath), Equals, tc.expected)
	}
}

func (s *TestSuite) TestNvmeEveryPollGroupHasUsableIOPath(c *C) {
	fmt.Println("Testing nvmeEveryPollGroupHasUsableIOPath requires a usable path on every reactor")

	usablePath := spdktypes.BdevNvmeIoPath{Connected: true, Accessible: true, State: spdktypes.BdevNvmeQpairStateEnabled}
	unusablePath := spdktypes.BdevNvmeIoPath{Connected: false, Accessible: true, State: spdktypes.BdevNvmeQpairStateDisconnected}

	type testCase struct {
		pollGroups     []spdktypes.BdevNvmePollGroupIoPaths
		expectedUsable bool
		expectedDetail string
	}
	testCases := map[string]testCase{
		"every reactor has a usable path": {
			pollGroups: []spdktypes.BdevNvmePollGroupIoPaths{
				{Thread: "nvmf_tgt_poll_group_0", IoPaths: []spdktypes.BdevNvmeIoPath{usablePath}},
				{Thread: "nvmf_tgt_poll_group_1", IoPaths: []spdktypes.BdevNvmeIoPath{usablePath}},
			},
			expectedUsable: true,
		},
		"one usable reactor does not mask an unusable one": {
			pollGroups: []spdktypes.BdevNvmePollGroupIoPaths{
				{Thread: "nvmf_tgt_poll_group_0", IoPaths: []spdktypes.BdevNvmeIoPath{usablePath}},
				{Thread: "nvmf_tgt_poll_group_1", IoPaths: []spdktypes.BdevNvmeIoPath{unusablePath}},
			},
			expectedUsable: false,
			expectedDetail: `thread=nvmf_tgt_poll_group_1: connected=false accessible=true qpair_state="DISCONNECTED"`,
		},
		"a reactor with one unusable and one usable path is usable": {
			pollGroups: []spdktypes.BdevNvmePollGroupIoPaths{
				{Thread: "nvmf_tgt_poll_group_0", IoPaths: []spdktypes.BdevNvmeIoPath{unusablePath, usablePath}},
			},
			expectedUsable: true,
		},
		"a reactor without paths for the bdev is skipped": {
			pollGroups: []spdktypes.BdevNvmePollGroupIoPaths{
				{Thread: "nvmf_tgt_poll_group_0", IoPaths: []spdktypes.BdevNvmeIoPath{usablePath}},
				{Thread: "app_thread"},
			},
			expectedUsable: true,
		},
		"no reactor has paths for the bdev": {
			pollGroups: []spdktypes.BdevNvmePollGroupIoPaths{
				{Thread: "app_thread"},
			},
			expectedUsable: false,
			expectedDetail: "no I/O paths",
		},
		"no poll groups": {
			expectedUsable: false,
			expectedDetail: "no I/O paths",
		},
	}
	for testName, tc := range testCases {
		c.Logf("testing nvmeEveryPollGroupHasUsableIOPath.%v", testName)
		usable, detail := nvmeEveryPollGroupHasUsableIOPath(tc.pollGroups)
		c.Assert(usable, Equals, tc.expectedUsable)
		c.Assert(detail, Equals, tc.expectedDetail)
	}
}

func (s *TestSuite) TestIdleTimerExpiresAfterTimeout(c *C) {
	fmt.Println("Testing idleTimer expires only after the timeout passes without a progress change")

	base := time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC)
	timeout := 10 * time.Minute
	timer := newIdleTimer(timeout, base)

	// The first check starts the timer; it never expires.
	idleFor, expired := timer.check(0, base)
	c.Assert(expired, Equals, false)
	c.Assert(idleFor, Equals, time.Duration(0))

	// Sitting at the same progress exactly at the timeout has not expired.
	idleFor, expired = timer.check(0, base.Add(timeout))
	c.Assert(expired, Equals, false)
	c.Assert(idleFor, Equals, timeout)

	// One tick past the timeout has expired.
	idleFor, expired = timer.check(0, base.Add(timeout+time.Nanosecond))
	c.Assert(expired, Equals, true)
	c.Assert(idleFor, Equals, timeout+time.Nanosecond)

	// A progress change restarts the timer.
	_, expired = timer.check(1, base.Add(timeout+time.Minute))
	c.Assert(expired, Equals, false)
	idleFor, expired = timer.check(1, base.Add(timeout+2*time.Minute))
	c.Assert(expired, Equals, false)
	c.Assert(idleFor, Equals, time.Minute)
}

func (s *TestSuite) TestWaitForRestoreCompleteAbortsIdleRestore(c *C) {
	fmt.Println("Testing waitForRestoreComplete aborts a restore whose progress stays unchanged")

	originalInterval := restorePeriodicRefreshInterval
	originalFloor := restoreIdleTimeoutFloor
	originalPerGiB := restoreIdleTimeoutPerGiB
	defer func() {
		restorePeriodicRefreshInterval = originalInterval
		restoreIdleTimeoutFloor = originalFloor
		restoreIdleTimeoutPerGiB = originalPerGiB
	}()
	restorePeriodicRefreshInterval = 5 * time.Millisecond
	restoreIdleTimeoutFloor = 30 * time.Millisecond
	restoreIdleTimeoutPerGiB = time.Millisecond

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	// Attribution must skip ERR backends and backends without an attached
	// bdev, so the nil SPDK client is never dialed.
	erroredBackend := newFakeBackend("replica-err", "10.0.0.1:1234")
	erroredBackend.SetMode(lhtypes.ModeERR)
	unattachedBackend := newFakeBackend("replica-no-bdev", "10.0.0.2:1234")
	unattachedBackend.SetMode(lhtypes.ModeRW)
	e.backends = map[string]Backend{
		"replica-err":     erroredBackend,
		"replica-no-bdev": unattachedBackend,
	}
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)

	err := e.waitForRestoreComplete(nil)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "idle at"), Equals, true)
	// The idle abort could not be attributed to a replica, so the error stays
	// at the engine level.
	c.Assert(e.restore.ErrorSourceReplicaName, Equals, "")
}

func (s *TestSuite) TestWaitForRestoreCompleteReturnsOnFullProgress(c *C) {
	fmt.Println("Testing waitForRestoreComplete returns nil once progress reaches 100")

	originalInterval := restorePeriodicRefreshInterval
	defer func() {
		restorePeriodicRefreshInterval = originalInterval
	}()
	restorePeriodicRefreshInterval = 5 * time.Millisecond

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	e.restore.UpdateRestoreStatus("", 100, nil)

	c.Assert(e.waitForRestoreComplete(nil), IsNil)
}

func (s *TestSuite) TestWaitForRestoreCompleteFailsOnErrorAtFullProgress(c *C) {
	fmt.Println("Testing waitForRestoreComplete returns the error when progress reaches 100 with an error")

	originalInterval := restorePeriodicRefreshInterval
	defer func() {
		restorePeriodicRefreshInterval = originalInterval
	}()
	restorePeriodicRefreshInterval = 5 * time.Millisecond

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.restore = NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)
	// backupstore reports a failed device sync or close together with
	// progress 100 once all blocks were written.
	e.restore.UpdateRestoreStatus("", 100, errors.New("failed to sync NVMe device"))

	err := e.waitForRestoreComplete(nil)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to sync NVMe device"), Equals, true)
}

func (s *TestSuite) TestEngineRestoreCloseVolumeDevReturnsSyncError(c *C) {
	fmt.Println("Testing EngineRestore.CloseVolumeDev returns the sync error and still closes the device")

	// A pipe cannot be synced, so Sync fails with EINVAL while Close succeeds.
	readEnd, writeEnd, err := os.Pipe()
	c.Assert(err, IsNil)
	defer func() { _ = readEnd.Close() }()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	r := NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)

	err = r.CloseVolumeDev(writeEnd)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to sync"), Equals, true)
	// The device was closed despite the sync failure.
	c.Assert(errors.Is(writeEnd.Close(), os.ErrClosed), Equals, true)
}

func (s *TestSuite) TestEngineRestoreCloseVolumeDevReturnsCloseError(c *C) {
	fmt.Println("Testing EngineRestore.CloseVolumeDev reports both the sync and the close error")

	f, err := os.CreateTemp(c.MkDir(), "voldev")
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	r := NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)

	err = r.CloseVolumeDev(f)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to sync"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "failed to close"), Equals, true)
}

func (s *TestSuite) TestEngineRestoreCloseVolumeDevSucceeds(c *C) {
	fmt.Println("Testing EngineRestore.CloseVolumeDev returns nil when sync and close succeed")

	f, err := os.CreateTemp(c.MkDir(), "voldev")
	c.Assert(err, IsNil)

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	r := NewEngineRestore(nil, "s3://backupbucket@us-east-1/backupstore?backup=backup-a&volume=vol-a", "backup-a", e, nil)

	c.Assert(r.CloseVolumeDev(f), IsNil)
	c.Assert(errors.Is(f.Close(), os.ErrClosed), Equals, true)
}

func (s *TestSuite) TestPreflightReplicaIOPathsForRestorePassesWithoutCandidates(c *C) {
	fmt.Println("Testing preflightReplicaIOPathsForRestore passes when no replica bdev is eligible for inspection")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	// The preflight must skip ERR backends and backends without an attached
	// bdev, so the nil SPDK client is never dialed.
	erroredBackend := newFakeBackend("replica-err", "10.0.0.1:1234")
	erroredBackend.SetMode(lhtypes.ModeERR)
	unattachedBackend := newFakeBackend("replica-no-bdev", "10.0.0.2:1234")
	unattachedBackend.SetMode(lhtypes.ModeRW)
	e.backends = map[string]Backend{
		"replica-err":     erroredBackend,
		"replica-no-bdev": unattachedBackend,
	}

	replicaName, err := e.preflightReplicaIOPathsForRestore(nil)

	c.Assert(err, IsNil)
	c.Assert(replicaName, Equals, "")
}

// newPreflightTestEngine returns an engine with two RW replicas whose bdevs
// are "bdev-a" and "bdev-b", and shortens the preflight retry loop. The
// returned function restores the package-level settings it changed.
func newPreflightTestEngine() (*Engine, func()) {
	originalBdevHasUsableIOPathFn := bdevHasUsableIOPathFn
	originalInterval := restorePreflightRetryInterval
	originalTimeout := restorePreflightTimeout
	restorePreflightRetryInterval = time.Millisecond
	restorePreflightTimeout = 5 * time.Millisecond

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{}
	for _, name := range []string{"a", "b"} {
		backend := newFakeBackend("replica-"+name, "10.0.0.1:1234")
		backend.SetMode(lhtypes.ModeRW)
		backend.SetBdevName("bdev-" + name)
		e.backends["replica-"+name] = backend
	}

	return e, func() {
		bdevHasUsableIOPathFn = originalBdevHasUsableIOPathFn
		restorePreflightRetryInterval = originalInterval
		restorePreflightTimeout = originalTimeout
	}
}

func (s *TestSuite) TestPreflightReplicaIOPathsForRestorePassesWhenAllPathsUsable(c *C) {
	fmt.Println("Testing preflightReplicaIOPathsForRestore passes when every replica has a usable I/O path")

	e, restore := newPreflightTestEngine()
	defer restore()
	bdevHasUsableIOPathFn = func(*spdkclient.Client, string) (bool, string, error) {
		return true, "", nil
	}

	replicaName, err := e.preflightReplicaIOPathsForRestore(nil)

	c.Assert(err, IsNil)
	c.Assert(replicaName, Equals, "")
}

func (s *TestSuite) TestPreflightReplicaIOPathsForRestoreFailsClosedOnInspectionError(c *C) {
	fmt.Println("Testing preflightReplicaIOPathsForRestore fails without a replica attribution when no I/O path state can be read")

	e, restore := newPreflightTestEngine()
	defer restore()
	bdevHasUsableIOPathFn = func(_ *spdkclient.Client, bdevName string) (bool, string, error) {
		return false, "", fmt.Errorf("rpc failed for %s", bdevName)
	}

	replicaName, err := e.preflightReplicaIOPathsForRestore(nil)

	// The path state is unknown, so the restore must not start. No replica is
	// known to be broken, so the error stays at the engine level.
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "cannot verify the replica NVMe I/O paths"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "bdev-b"), Equals, true)
	c.Assert(replicaName, Equals, "")
}

func (s *TestSuite) TestPreflightReplicaIOPathsForRestoreAttributesBrokenReplica(c *C) {
	fmt.Println("Testing preflightReplicaIOPathsForRestore attributes the failure to the broken replica even when another replica cannot be inspected")

	e, restore := newPreflightTestEngine()
	defer restore()
	bdevHasUsableIOPathFn = func(_ *spdkclient.Client, bdevName string) (bool, string, error) {
		if bdevName == "bdev-a" {
			return false, "", fmt.Errorf("rpc failed for %s", bdevName)
		}
		return false, "poll group 1 has no usable path", nil
	}

	replicaName, err := e.preflightReplicaIOPathsForRestore(nil)

	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "replica-b has no usable NVMe I/O path"), Equals, true)
	c.Assert(replicaName, Equals, "replica-b")
}
