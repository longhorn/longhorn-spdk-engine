package spdk

import (
	"time"

	btypes "github.com/longhorn/backupstore/types"
	commonns "github.com/longhorn/go-common-libs/ns"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBackupTerminalStatusInvokesCallbackOnce(c *C) {
	callbackCh := make(chan struct{}, 2)
	backup := &Backup{
		Name:       "backup-a",
		VolumeName: "vol-a",
		log:        logrus.New(),
		onTerminal: func() {
			callbackCh <- struct{}{}
		},
	}

	err := backup.UpdateBackupStatus("snap-a", "vol-a", string(btypes.ProgressStateInProgress), 100, "backup://target", "")
	c.Assert(err, IsNil)
	c.Assert(backup.State, Equals, btypes.ProgressStateComplete)
	c.Assert(backup.terminalSeen, Equals, false)

	select {
	case <-callbackCh:
		c.Fatal("unexpected terminal callback before cleanup")
	case <-time.After(100 * time.Millisecond):
	}

	err = backup.CloseSnapshot("snap-a", "vol-a")
	c.Assert(err, IsNil)
	c.Assert(backup.terminalSeen, Equals, true)

	select {
	case <-callbackCh:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("timed out waiting for terminal callback")
	}

	err = backup.UpdateBackupStatus("snap-a", "vol-a", string(btypes.ProgressStateInProgress), 10, "", "")
	c.Assert(err, IsNil)
	err = backup.CloseSnapshot("snap-a", "vol-a")
	c.Assert(err, IsNil)

	select {
	case <-callbackCh:
		c.Fatal("unexpected second terminal callback")
	case <-time.After(100 * time.Millisecond):
	}
}

func (s *TestSuite) TestBackupTerminalCallbackRunsAfterCleanup(c *C) {
	callbackStarted := make(chan struct{}, 1)
	callbackDone := make(chan struct{}, 1)
	origBackupStopExposeBdev := backupStopExposeBdev
	defer func() {
		backupStopExposeBdev = origBackupStopExposeBdev
	}()
	backupStopExposeBdev = func(_ *spdkclient.Client, _ string) error {
		return nil
	}

	var backup *Backup
	backup = &Backup{
		Name:       "backup-a",
		VolumeName: "vol-a",
		State:      btypes.ProgressStateComplete,
		replica:    &Replica{Name: "replica-a"},
		fragmap:    &Fragmap{},
		log:        logrus.New(),
		onTerminal: func() {
			callbackStarted <- struct{}{}
			if backup.replica != nil || backup.fragmap != nil || backup.devFh != nil || backup.initiator != nil {
				c.Errorf("cleanup had not completed before terminal callback ran")
			}
			callbackDone <- struct{}{}
		},
	}

	err := backup.CloseSnapshot("snap-a", "vol-a")
	c.Assert(err, IsNil)

	select {
	case <-callbackStarted:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("timed out waiting for terminal callback start")
	}

	select {
	case <-callbackDone:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("timed out waiting for terminal callback completion")
	}
}

func (s *TestSuite) TestPruneRetainedBackupsKeepsRecentTerminalStates(c *C) {
	oldRetainCounts := retainBackupStateCounts
	retainBackupStateCounts = map[btypes.ProgressState]int{
		btypes.ProgressStateComplete: 2,
		btypes.ProgressStateError:    1,
	}
	defer func() {
		retainBackupStateCounts = oldRetainCounts
	}()

	server := &Server{
		backupMap: map[string]*Backup{},
	}

	server.trackBackupLocked("complete-oldest", &Backup{
		Name:           "complete-oldest",
		State:          btypes.ProgressStateComplete,
		terminalSeen:   true,
		terminalAt:     time.Now().Add(-5 * time.Second),
		replica:        &Replica{Name: "replica-a"},
		fragmap:        &Fragmap{},
		executor:       &commonns.Executor{},
		subsystemNQN:   "nqn-oldest",
		controllerName: "ctrl-oldest",
	})
	server.trackBackupLocked("complete-middle", &Backup{Name: "complete-middle", State: btypes.ProgressStateComplete, terminalSeen: true, terminalAt: time.Now().Add(-3 * time.Second)})
	server.trackBackupLocked("complete-newest", &Backup{Name: "complete-newest", State: btypes.ProgressStateComplete, terminalSeen: true, terminalAt: time.Now().Add(-1 * time.Second)})
	server.trackBackupLocked("error-oldest", &Backup{Name: "error-oldest", State: btypes.ProgressStateError, terminalSeen: true, terminalAt: time.Now().Add(-4 * time.Second)})
	server.trackBackupLocked("error-newest", &Backup{Name: "error-newest", State: btypes.ProgressStateError, terminalSeen: true, terminalAt: time.Now().Add(-2 * time.Second)})
	server.trackBackupLocked("in-progress", &Backup{Name: "in-progress", State: btypes.ProgressStateInProgress})

	_, exists := server.backupMap["complete-oldest"]
	c.Assert(exists, Equals, false)
	_, exists = server.backupMap["error-oldest"]
	c.Assert(exists, Equals, false)

	_, exists = server.backupMap["complete-middle"]
	c.Assert(exists, Equals, true)
	_, exists = server.backupMap["complete-newest"]
	c.Assert(exists, Equals, true)
	_, exists = server.backupMap["error-newest"]
	c.Assert(exists, Equals, true)
	_, exists = server.backupMap["in-progress"]
	c.Assert(exists, Equals, true)
}

func (s *TestSuite) TestPruneRetainedBackupsReleasesHeavyResourcesBeforeDeletion(c *C) {
	oldRetainCounts := retainBackupStateCounts
	retainBackupStateCounts = map[btypes.ProgressState]int{
		btypes.ProgressStateComplete: 0,
		btypes.ProgressStateError:    0,
	}
	defer func() {
		retainBackupStateCounts = oldRetainCounts
	}()

	server := &Server{
		backupMap: map[string]*Backup{},
	}

	evicted := &Backup{
		Name:           "complete-old",
		State:          btypes.ProgressStateComplete,
		terminalSeen:   true,
		terminalAt:     time.Now(),
		replica:        &Replica{Name: "replica-a"},
		fragmap:        &Fragmap{},
		executor:       &commonns.Executor{},
		subsystemNQN:   "nqn-evicted",
		controllerName: "ctrl-evicted",
	}
	server.trackBackupLocked(evicted.Name, evicted)

	c.Assert(evicted.fragmap, IsNil)
	c.Assert(evicted.executor, IsNil)
	c.Assert(evicted.replica, IsNil)
	c.Assert(evicted.subsystemNQN, Equals, "")
	c.Assert(evicted.controllerName, Equals, "")
	_, exists := server.backupMap[evicted.Name]
	c.Assert(exists, Equals, false)
}

// TestOnBackupTerminalPrunesByTerminalTime verifies that when two backups are
// created in order A, B but B completes before A, pruning evicts B (the earlier
// completion) rather than A, regardless of goroutine scheduling order.
func (s *TestSuite) TestOnBackupTerminalPrunesByTerminalTime(c *C) {
	oldRetainCounts := retainBackupStateCounts
	retainBackupStateCounts = map[btypes.ProgressState]int{
		btypes.ProgressStateComplete: 1,
	}
	defer func() {
		retainBackupStateCounts = oldRetainCounts
	}()

	server := &Server{
		backupMap: map[string]*Backup{},
	}

	// Create A then B.
	backupA := &Backup{Name: "A", State: btypes.ProgressStateInProgress}
	backupB := &Backup{Name: "B", State: btypes.ProgressStateInProgress}
	server.trackBackupLocked("A", backupA)
	server.trackBackupLocked("B", backupB)

	// B completes first via the real status-update path.
	err := backupB.UpdateBackupStatus("snap", "vol", string(btypes.ProgressStateInProgress), 100, "", "")
	c.Assert(err, IsNil)
	c.Assert(backupB.State, Equals, btypes.ProgressStateComplete)
	c.Assert(backupB.terminalAt.IsZero(), Equals, false)
	bTime := backupB.terminalAt

	// Small delay so A gets a strictly later terminalAt.
	time.Sleep(5 * time.Millisecond)

	// A completes second via the real status-update path.
	err = backupA.UpdateBackupStatus("snap", "vol", string(btypes.ProgressStateInProgress), 100, "", "")
	c.Assert(err, IsNil)
	c.Assert(backupA.State, Equals, btypes.ProgressStateComplete)
	c.Assert(backupA.terminalAt.IsZero(), Equals, false)
	c.Assert(backupA.terminalAt.After(bTime), Equals, true)
	backupB.terminalSeen = true
	backupA.terminalSeen = true

	// Simulate callbacks arriving in any order — only prune matters.
	server.onBackupTerminalLocked()

	// Retain limit is 1: A (the most recently completed) must survive; B must be evicted.
	_, existsA := server.backupMap["A"]
	c.Assert(existsA, Equals, true)
	_, existsB := server.backupMap["B"]
	c.Assert(existsB, Equals, false)
}

func (s *TestSuite) TestPruneRetainedBackupsSkipsTerminalBackupsBeforeCleanup(c *C) {
	oldRetainCounts := retainBackupStateCounts
	retainBackupStateCounts = map[btypes.ProgressState]int{
		btypes.ProgressStateComplete: 0,
	}
	defer func() {
		retainBackupStateCounts = oldRetainCounts
	}()

	server := &Server{
		backupMap: map[string]*Backup{},
	}

	backup := &Backup{
		Name:       "complete-not-cleaned",
		State:      btypes.ProgressStateComplete,
		terminalAt: time.Now(),
		replica:    &Replica{Name: "replica-a"},
		fragmap:    &Fragmap{},
		executor:   &commonns.Executor{},
	}

	server.trackBackupLocked(backup.Name, backup)

	_, exists := server.backupMap[backup.Name]
	c.Assert(exists, Equals, true)
	c.Assert(backup.replica, NotNil)
	c.Assert(backup.fragmap, NotNil)
	c.Assert(backup.executor, NotNil)
}
