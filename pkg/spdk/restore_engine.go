package spdk

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	btypes "github.com/longhorn/backupstore/types"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
)

type EngineRestore struct {
	sync.RWMutex

	spdkClient *spdkclient.Client
	engine     *Engine

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// The snapshot file that stores the restored data in the end.
	SnapshotName string

	TargetAddress string

	superiorPortAllocator *commonbitmap.Bitmap

	LastRestored           string
	CurrentRestoringBackup string

	stopChan chan struct{}
	stopOnce sync.Once

	log logrus.FieldLogger
}

var _ backupstore.DeltaRestoreOperations = (*EngineRestore)(nil)

func NewEngineRestore(spdkClient *spdkclient.Client, snapshotName string, backupURL string, backupName string, engine *Engine, superiorPortAllocator *commonbitmap.Bitmap) *EngineRestore {
	log := logrus.WithFields(logrus.Fields{
		"targetAddress": engine.restore.TargetAddress,
		"snapshotName":  snapshotName,
		"backupURL":     backupURL,
		"backupName":    backupName,
	})

	return &EngineRestore{
		spdkClient:             spdkClient,
		engine:                 engine,
		BackupURL:              backupURL,
		CurrentRestoringBackup: backupName,
		SnapshotName:           snapshotName,
		TargetAddress:          engine.restore.TargetAddress,
		superiorPortAllocator:  superiorPortAllocator,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		stopChan:               make(chan struct{}),
		log:                    log,
	}
}

func (r *EngineRestore) StartNewRestore(backupURL string, currentRestoringBackup string, snapshotName string, validLastRestoredBackup bool) {
	r.Lock()
	defer r.Unlock()

	r.SnapshotName = snapshotName

	r.Progress = 0
	r.Error = ""
	r.BackupURL = backupURL
	r.State = btypes.ProgressStateInProgress

	if !validLastRestoredBackup {
		r.LastRestored = ""
	}

	r.CurrentRestoringBackup = currentRestoringBackup
}

func (r *EngineRestore) DeepCopy() *EngineRestore {
	r.RLock()
	defer r.RUnlock()

	return &EngineRestore{
		BackupURL:              r.BackupURL,
		CurrentRestoringBackup: r.CurrentRestoringBackup,
		LastRestored:           r.LastRestored,
		SnapshotName:           r.SnapshotName,
		TargetAddress:          r.TargetAddress,
		superiorPortAllocator:  r.superiorPortAllocator,
		State:                  r.State,
		Error:                  r.Error,
		Progress:               r.Progress,
	}
}

func (r *EngineRestore) OpenVolumeDev(_ string) (*os.File, string, error) {
	r.log.Infof("Setting up NVMe-TCP frontend for backup restore to address %v", r.TargetAddress)
	if err := r.engine.handleNvmeTcpFrontend(r.spdkClient, r.superiorPortAllocator, 1, r.TargetAddress, true, true); err != nil {
		return nil, "", errors.Wrapf(err, "failed to setup NVMe-TCP frontend for backup restore to address %v", r.TargetAddress)
	}

	r.log.Infof("Opening NVMe device %v", r.engine.initiator.Endpoint)
	fh, err := os.OpenFile(r.engine.initiator.Endpoint, os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v", r.engine.initiator.Endpoint)
	}
	return fh, r.engine.initiator.Endpoint, nil
}

func (r *EngineRestore) CloseVolumeDev(volDev *os.File) error {
	if err := volDev.Sync(); err != nil {
		r.log.WithError(err).Errorf("Failed to sync NVMe device %v before close", volDev.Name())
	}

	r.log.Infof("Closing NVMe device %v", volDev.Name())
	closeErr := volDev.Close()

	r.log.Infof("Disconnecting NVMe-TCP target %s after restore", r.TargetAddress)
	if err := r.engine.disconnectTarget(r.TargetAddress); err != nil {
		r.log.WithError(err).Warn("Failed to disconnect NVMe-TCP target after restore")
	}

	if err := r.spdkClient.StopExposeBdev(r.engine.NvmeTcpFrontend.Nqn); err != nil {
		r.log.WithError(err).Warnf("Failed to stop exposing bdev for NVMe-TCP frontend nqn %s after restore", r.engine.NvmeTcpFrontend.Nqn)
	}

	if err := r.engine.releasePorts(r.superiorPortAllocator); err != nil {
		r.log.WithError(err).Warn("Failed to release ports after restore")
	}

	return closeErr
}

func (r *EngineRestore) UpdateRestoreStatus(snapshot string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	r.Progress = progress

	if err != nil {
		if strings.Contains(err.Error(), btypes.ErrorMsgRestoreCancelled) {
			r.State = btypes.ProgressStateCanceled
			r.Error = err.Error()
		} else {
			r.State = btypes.ProgressStateError
			if r.Error != "" {
				r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
			} else {
				r.Error = err.Error()
			}
		}
	}
}

func (r *EngineRestore) FinishRestore() {
	r.Lock()
	defer r.Unlock()

	if r.State != btypes.ProgressStateError && r.State != btypes.ProgressStateCanceled {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}
}

func (r *EngineRestore) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)

		r.Lock()
		defer r.Unlock()
		r.State = btypes.ProgressStateCanceled
		r.Error = btypes.ErrorMsgRestoreCancelled
		r.Progress = 0
	})
}

func (r *EngineRestore) GetStopChan() chan struct{} {
	return r.stopChan
}
