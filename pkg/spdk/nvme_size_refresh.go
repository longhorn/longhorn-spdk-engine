package spdk

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	retrygo "github.com/avast/retry-go/v5"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

// The helpers in this file read back the size of an NVMe-oF target that grew while a
// local bdev_nvme controller was attached to it. The RAID1 engine expansion uses them.
//
// A replica grows its lvol while its NVMe-oF subsystem is not exposed, because
// bdev_lvol_resize can hang with an initiator attached on the same SPDK process. Without
// the subsystem, no namespace attribute changed notice (AER) reaches the local bdev_nvme
// controller, so its nvme bdev keeps the old size. The Longhorn SPDK fork refreshes the
// namespace sizes when a controller reconnects after a reset, and raises
// SPDK_BDEV_EVENT_RESIZE on the nvme bdev when the size changed (longhorn/spdk#72,
// bdev_nvme_reset_create_qpairs_done). So a controller reset reads back the new size
// without detaching the bdev from the RAID bdev above it.
//
// The RAID bdev of a RAID1 engine has explicit_resize on (longhorn/spdk#91), so it ignores
// that event and grows only through bdev_raid_resize, which Expand issues once every
// member reports the new size.

const (
	nvmeSizeRefreshResetRetryInterval = 500 * time.Millisecond
	nvmeSizeRefreshResetTimeout       = 20 * time.Second
)

// resetNvmeControllerForSizeRefresh resets the bdev_nvme controller controllerName so that
// its nvme bdev reads back the current namespace size.
//
// The controller has usually just been disconnected by a restart of the remote export, and
// it can refuse the reset while SPDK is still reconnecting it, so a refused or failed reset
// is retried with a fixed delay until the controller accepts it or the timeout passes. A
// controller that no longer exists was deleted after the controller loss timeout and cannot
// recover, so it is not retried.
func resetNvmeControllerForSizeRefresh(spdkClient *spdkclient.Client, controllerName string, log *safelog.SafeLogger) error {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeSizeRefreshResetTimeout)
	defer cancel()

	err := retrygo.New(
		retrygo.Context(ctx),
		retrygo.UntilSucceeded(),
		retrygo.Delay(nvmeSizeRefreshResetRetryInterval),
		retrygo.DelayType(retrygo.FixedDelay),
		retrygo.RetryIf(func(err error) bool {
			return !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err)
		}),
		retrygo.WrapContextErrorWithLastError(true),
		retrygo.OnRetry(func(_ uint, err error) {
			log.WithError(err).Warnf("Failed to reset nvme controller %s to refresh its size, will retry", controllerName)
		}),
	).Do(func() error {
		success, err := sizeRefreshBdevNvmeResetController(spdkClient, controllerName)
		if err != nil {
			return err
		}
		if !success {
			return fmt.Errorf("reset returned false")
		}
		return nil
	})
	if err == nil {
		return nil
	}
	if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "nvme controller %s no longer exists", controllerName)
	}
	return errors.Wrapf(lastRetryError(err), "failed to reset nvme controller %s", controllerName)
}

// lastRetryError returns the last error of the retried function when retry-go returns it
// together with the context error (WrapContextErrorWithLastError), and err otherwise.
func lastRetryError(err error) error {
	if retryErr, ok := err.(retrygo.Error); ok {
		if lastErr := retryErr.LastError(); lastErr != nil {
			return lastErr
		}
	}
	return err
}

// checkBdevSize checks that the bdev bdevName reports at least size bytes.
func checkBdevSize(spdkClient *spdkclient.Client, bdevName string, size uint64) error {
	bdevs, err := sizeRefreshBdevGetBdevs(spdkClient, bdevName, 0)
	if err != nil {
		return errors.Wrapf(err, "failed to get bdev %s", bdevName)
	}
	if len(bdevs) == 0 {
		return fmt.Errorf("bdev %s not found", bdevName)
	}
	if bdevSize := getBdevSize(&bdevs[0]); bdevSize < size {
		return fmt.Errorf("bdev %s is at size %v, requested %v", bdevName, bdevSize, size)
	}
	return nil
}
