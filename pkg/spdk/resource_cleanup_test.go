package spdk

import (
	"context"
	"fmt"
	"os"
	"time"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonns "github.com/longhorn/go-common-libs/ns"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"

	. "gopkg.in/check.v1"
)

type fakeNVMeInitiator struct {
	endpoint    string
	startCalled bool
	stopCalled  bool
}

func (f *fakeNVMeInitiator) StartNvmeTCPInitiator(_, _ string, _ bool, _ bool) (bool, error) {
	f.startCalled = true
	return false, nil
}

func (f *fakeNVMeInitiator) Stop(_ *spdkclient.Client, _ bool, _ bool, _ bool) (bool, error) {
	f.stopCalled = true
	return false, nil
}

func (f *fakeNVMeInitiator) Endpoint() string {
	return f.endpoint
}

type fakeBackingImageServiceClient struct {
	exposedAddress string
	exposeCalled   bool
	unexposeCalled bool
	closeCalled    bool
}

func (f *fakeBackingImageServiceClient) BackingImageExpose(_, _ string) (string, error) {
	f.exposeCalled = true
	return f.exposedAddress, nil
}

func (f *fakeBackingImageServiceClient) BackingImageUnexpose(_, _ string) error {
	f.unexposeCalled = true
	return nil
}

func (f *fakeBackingImageServiceClient) Close() error {
	f.closeCalled = true
	return nil
}

func (s *TestSuite) TestBackupOpenSnapshotCleansUpOnOpenFileFailure(c *C) {
	c.Log("Testing Backup.OpenSnapshot cleans up initiator and exposure when device open fails")

	origOpenFile := openFile
	origNewNVMeTCPInitiator := newNVMeTCPInitiator
	origBackupNewFragmap := backupNewFragmap
	origBackupExposeSnapshotLvolBdev := backupExposeSnapshotLvolBdev
	origBackupStopExposeBdev := backupStopExposeBdev
	defer func() {
		openFile = origOpenFile
		newNVMeTCPInitiator = origNewNVMeTCPInitiator
		backupNewFragmap = origBackupNewFragmap
		backupExposeSnapshotLvolBdev = origBackupExposeSnapshotLvolBdev
		backupStopExposeBdev = origBackupStopExposeBdev
	}()

	fakeInitiator := &fakeNVMeInitiator{endpoint: "/dev/fake-backup"}
	stopExposeCalled := false
	stopExposeNQN := ""

	openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	newNVMeTCPInitiator = func(string, *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		return fakeInitiator, nil
	}
	backupNewFragmap = func(*Backup) (*Fragmap, error) {
		return &Fragmap{}, nil
	}
	backupExposeSnapshotLvolBdev = func(*spdkclient.Client, string, string, string, int32, *commonns.Executor) (string, string, error) {
		return "subsystem", "controller", nil
	}
	backupStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		stopExposeCalled = true
		stopExposeNQN = nqn
		return nil
	}

	backup := &Backup{
		spdkClient:     nil,
		VolumeName:     "vol-a",
		replica:        &Replica{Name: "replica-a", LvsName: "disk-a"},
		IP:             "10.0.0.1",
		Port:           1000,
		log:            logrus.New(),
		subsystemNQN:   "stale-subsystem",
		controllerName: "stale-controller",
	}

	err := backup.OpenSnapshot("snap-a", "vol-a")
	c.Assert(err, NotNil)
	c.Assert(fakeInitiator.startCalled, Equals, true)
	c.Assert(fakeInitiator.stopCalled, Equals, true)
	c.Assert(stopExposeCalled, Equals, true)
	c.Assert(stopExposeNQN, Equals, helpertypes.GetNQN(GetReplicaSnapshotLvolName("replica-a", "snap-a")))
	c.Assert(backup.initiator, IsNil)
	c.Assert(backup.devFh, IsNil)
	c.Assert(backup.subsystemNQN, Equals, "")
	c.Assert(backup.controllerName, Equals, "")
}

func (s *TestSuite) TestRestoreOpenVolumeDevCleansUpOnOpenFileFailure(c *C) {
	c.Log("Testing Restore.OpenVolumeDev cleans up initiator and exposure when device open fails")

	origOpenFile := openFile
	origNewNVMeTCPInitiator := newNVMeTCPInitiator
	origRestoreExposeSnapshotLvolBdev := restoreExposeSnapshotLvolBdev
	origRestoreStopExposeBdev := restoreStopExposeBdev
	defer func() {
		openFile = origOpenFile
		newNVMeTCPInitiator = origNewNVMeTCPInitiator
		restoreExposeSnapshotLvolBdev = origRestoreExposeSnapshotLvolBdev
		restoreStopExposeBdev = origRestoreStopExposeBdev
	}()

	fakeInitiator := &fakeNVMeInitiator{endpoint: "/dev/fake-restore"}
	stopExposeCalled := false
	stopExposeNQN := ""

	openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	newNVMeTCPInitiator = func(string, *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		return fakeInitiator, nil
	}
	restoreExposeSnapshotLvolBdev = func(*spdkclient.Client, string, string, string, int32, *commonns.Executor) (string, string, error) {
		return "subsystem", "controller", nil
	}
	restoreStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		stopExposeCalled = true
		stopExposeNQN = nqn
		return nil
	}

	replica := &Replica{Name: "replica-a", LvsName: "disk-a", IsExposed: false}
	restore := &Restore{
		spdkClient: nil,
		replica:    replica,
		ip:         "10.0.0.1",
		port:       1000,
		log:        logrus.New(),
	}

	fh, endpoint, err := restore.OpenVolumeDev("unused")
	c.Assert(err, NotNil)
	c.Assert(fh, IsNil)
	c.Assert(endpoint, Equals, "")
	c.Assert(fakeInitiator.startCalled, Equals, true)
	c.Assert(fakeInitiator.stopCalled, Equals, true)
	c.Assert(stopExposeCalled, Equals, true)
	c.Assert(stopExposeNQN, Equals, helpertypes.GetNQN("replica-a"))
	c.Assert(restore.initiator, IsNil)
	c.Assert(replica.IsExposed, Equals, false)
}

func (s *TestSuite) TestRestoreOpenVolumeDevUnexposeFailureKeepsIsExposed(c *C) {
	c.Log("Testing Restore.OpenVolumeDev keeps IsExposed=true when unexpose fails during cleanup")

	origOpenFile := openFile
	origNewNVMeTCPInitiator := newNVMeTCPInitiator
	origRestoreExposeSnapshotLvolBdev := restoreExposeSnapshotLvolBdev
	origRestoreStopExposeBdev := restoreStopExposeBdev
	defer func() {
		openFile = origOpenFile
		newNVMeTCPInitiator = origNewNVMeTCPInitiator
		restoreExposeSnapshotLvolBdev = origRestoreExposeSnapshotLvolBdev
		restoreStopExposeBdev = origRestoreStopExposeBdev
	}()

	fakeInitiator := &fakeNVMeInitiator{endpoint: "/dev/fake-restore"}

	openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	newNVMeTCPInitiator = func(string, *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		return fakeInitiator, nil
	}
	restoreExposeSnapshotLvolBdev = func(*spdkclient.Client, string, string, string, int32, *commonns.Executor) (string, string, error) {
		return "subsystem", "controller", nil
	}
	restoreStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		return fmt.Errorf("unexpose failed")
	}

	replica := &Replica{Name: "replica-a", LvsName: "disk-a", IsExposed: false}
	restore := &Restore{
		spdkClient: nil,
		replica:    replica,
		ip:         "10.0.0.1",
		port:       1000,
		log:        logrus.New(),
	}

	fh, endpoint, err := restore.OpenVolumeDev("unused")
	c.Assert(err, NotNil)
	c.Assert(fh, IsNil)
	c.Assert(endpoint, Equals, "")
	// IsExposed must remain true because unexpose failed
	c.Assert(replica.IsExposed, Equals, true)
}

func (s *TestSuite) TestBackupCloseSnapshotHandlesMissingInitiator(c *C) {
	c.Log("Testing Backup.CloseSnapshot tolerates a missing initiator during cleanup")

	origBackupStopExposeBdev := backupStopExposeBdev
	defer func() {
		backupStopExposeBdev = origBackupStopExposeBdev
	}()

	stopExposeCalled := false
	backupStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		stopExposeCalled = true
		c.Assert(nqn, Equals, helpertypes.GetNQN(GetReplicaSnapshotLvolName("replica-a", "snap-a")))
		return nil
	}

	fh, err := os.CreateTemp(c.MkDir(), "backup-close-*")
	c.Assert(err, IsNil)

	backup := &Backup{
		spdkClient: nil,
		replica:    &Replica{Name: "replica-a"},
		devFh:      fh,
		log:        logrus.New(),
	}

	err = backup.CloseSnapshot("snap-a", "vol-a")
	c.Assert(err, IsNil)
	c.Assert(stopExposeCalled, Equals, true)
	c.Assert(backup.devFh, IsNil)
	c.Assert(backup.initiator, IsNil)
	c.Assert(backup.replica, IsNil)
}

func (s *TestSuite) TestRestoreCloseVolumeDevHandlesMissingInitiator(c *C) {
	c.Log("Testing Restore.CloseVolumeDev tolerates a missing initiator during cleanup")

	origRestoreStopExposeBdev := restoreStopExposeBdev
	defer func() {
		restoreStopExposeBdev = origRestoreStopExposeBdev
	}()

	stopExposeCalled := false
	restoreStopExposeBdev = func(_ *spdkclient.Client, nqn string) error {
		stopExposeCalled = true
		c.Assert(nqn, Equals, helpertypes.GetNQN("replica-a"))
		return nil
	}

	volDev, err := os.CreateTemp(c.MkDir(), "restore-close-*")
	c.Assert(err, IsNil)

	replica := &Replica{Name: "replica-a", IsExposed: true}
	restore := &Restore{
		spdkClient: nil,
		replica:    replica,
		log:        logrus.New(),
	}

	err = restore.CloseVolumeDev(volDev)
	c.Assert(err, IsNil)
	c.Assert(stopExposeCalled, Equals, true)
	c.Assert(replica.IsExposed, Equals, false)
	c.Assert(restore.initiator, IsNil)
}

func (s *TestSuite) TestPrepareBackingImageSnapshotOpenFileFailureStopsInitiator(c *C) {
	c.Log("Testing prepareBackingImageSnapshot stops initiator when device open fails")

	origOpenFile := openFile
	origNewNVMeTCPInitiator := newNVMeTCPInitiator
	origBackingImageGetIPForPod := backingImageGetIPForPod
	origBackingImageNewExecutor := backingImageNewExecutor
	origBackingImageGetLvsNameByUUID := backingImageGetLvsNameByUUID
	origBackingImageBdevLvolCreate := backingImageBdevLvolCreate
	origBackingImageBdevLvolGet := backingImageBdevLvolGet
	origBackingImageExposeSnapshotLvolBdev := backingImageExposeSnapshotLvolBdev
	origBackingImageStopExposeBdev := backingImageStopExposeBdev
	defer func() {
		openFile = origOpenFile
		newNVMeTCPInitiator = origNewNVMeTCPInitiator
		backingImageGetIPForPod = origBackingImageGetIPForPod
		backingImageNewExecutor = origBackingImageNewExecutor
		backingImageGetLvsNameByUUID = origBackingImageGetLvsNameByUUID
		backingImageBdevLvolCreate = origBackingImageBdevLvolCreate
		backingImageBdevLvolGet = origBackingImageBdevLvolGet
		backingImageExposeSnapshotLvolBdev = origBackingImageExposeSnapshotLvolBdev
		backingImageStopExposeBdev = origBackingImageStopExposeBdev
	}()

	fakeInitiator := &fakeNVMeInitiator{endpoint: "/dev/fake-bi-head"}
	stopExposeCalled := false
	openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	newNVMeTCPInitiator = func(string, *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		return fakeInitiator, nil
	}
	backingImageGetIPForPod = func() (string, error) {
		return "10.0.0.1", nil
	}
	backingImageNewExecutor = func(string) (*commonns.Executor, error) {
		return nil, nil
	}
	backingImageGetLvsNameByUUID = func(*spdkclient.Client, string) (string, error) {
		return "disk-a", nil
	}
	backingImageBdevLvolCreate = func(*spdkclient.Client, string, string, string, uint64, spdktypes.BdevLvolClearMethod, bool) (string, error) {
		return "", nil
	}
	backingImageBdevLvolGet = func(*spdkclient.Client, string, uint64) ([]spdktypes.BdevInfo, error) {
		return []spdktypes.BdevInfo{{
			BdevInfoBasic: spdktypes.BdevInfoBasic{Name: "bi-temp-head"},
		}}, nil
	}
	backingImageExposeSnapshotLvolBdev = func(*spdkclient.Client, string, string, string, int32, *commonns.Executor) (string, string, error) {
		return "subsystem", "controller", nil
	}
	backingImageStopExposeBdev = func(*spdkclient.Client, string) error {
		stopExposeCalled = true
		return nil
	}

	portAllocator, err := commonbitmap.NewBitmap(1000, 1000)
	c.Assert(err, IsNil)

	bi := &BackingImage{
		ctx:     context.Background(),
		Name:    "bi-a",
		LvsName: "disk-a",
		LvsUUID: "uuid-a",
		log:     safelog.NewSafeLogger(logrus.New()),
	}

	err = bi.prepareBackingImageSnapshot(nil, portAllocator, "http://example.com/image.raw", "")
	c.Assert(err, NotNil)
	c.Assert(fakeInitiator.startCalled, Equals, true)
	c.Assert(fakeInitiator.stopCalled, Equals, true)
	c.Assert(stopExposeCalled, Equals, true)
}

func (s *TestSuite) TestPrepareFromSyncOpenFileFailureStopsInitiatorAndUnexposes(c *C) {
	c.Log("Testing prepareFromSync stops initiator and unexposes source snapshot when device open fails")

	origOpenFile := openFile
	origNewNVMeTCPInitiator := newNVMeTCPInitiator
	origBackingImageGetServiceClient := backingImageGetServiceClient
	origBackingImageDiscoverAndConnectNVMeTarget := backingImageDiscoverAndConnectNVMeTarget
	defer func() {
		openFile = origOpenFile
		newNVMeTCPInitiator = origNewNVMeTCPInitiator
		backingImageGetServiceClient = origBackingImageGetServiceClient
		backingImageDiscoverAndConnectNVMeTarget = origBackingImageDiscoverAndConnectNVMeTarget
	}()

	fakeInitiator := &fakeNVMeInitiator{endpoint: "/dev/fake-bi-sync"}
	fakeServiceClient := &fakeBackingImageServiceClient{exposedAddress: "10.0.0.1:1000"}

	openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	newNVMeTCPInitiator = func(string, *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		return fakeInitiator, nil
	}
	backingImageGetServiceClient = func(string) (backingImageServiceClient, error) {
		return fakeServiceClient, nil
	}
	backingImageDiscoverAndConnectNVMeTarget = func(string, int32, int, time.Duration) (string, string, error) {
		return "", "", nil
	}

	bi := &BackingImage{
		ctx:  context.Background(),
		Name: "bi-a",
		log:  safelog.NewSafeLogger(logrus.New()),
	}

	err := bi.prepareFromSync(nil, "remote-address", "src-lvs-uuid")
	c.Assert(err, NotNil)
	c.Assert(fakeServiceClient.exposeCalled, Equals, true)
	c.Assert(fakeServiceClient.unexposeCalled, Equals, true)
	c.Assert(fakeServiceClient.closeCalled, Equals, true)
	c.Assert(fakeInitiator.startCalled, Equals, true)
	c.Assert(fakeInitiator.stopCalled, Equals, true)
}
