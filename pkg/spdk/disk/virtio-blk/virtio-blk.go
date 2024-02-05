package virtioblk

import (
	"github.com/pkg/errors"

	commonTypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverVirtioBlk struct {
}

const (
	diskDriver = "virtio-blk"
)

func init() {
	driver := &DiskDriverVirtioBlk{}
	if err := disk.RegisterDiskDriver(diskDriver, driver); err != nil {
		panic(err)
	}
}

func (d *DiskDriverVirtioBlk) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, "uio_pci_generic", executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind the disk %v with uio_pci_generic", diskPath)
	}

	bdevs, err := spdkClient.BdevVirtioAttachController(diskName, "pci", diskPath, "blk")
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach the disk %v with virtio-blk", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach the disk %v with virtio-blk", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverVirtioBlk) DiskDelete(spdkClient *spdkclient.Client, diskName string) (deleted bool, err error) {
	return spdkClient.BdevVirtioDetachController(diskName)
}

func (d *DiskDriverVirtioBlk) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return spdkClient.BdevGetBdevs(diskName, timeout)
}
