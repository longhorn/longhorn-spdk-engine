package nvme

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonTypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverNvme struct {
}

const (
	diskDriver = "nvme"
)

func init() {
	driver := &DiskDriverNvme{}
	if err := disk.RegisterDiskDriver(diskDriver, driver); err != nil {
		panic(err)
	}
}

func (d *DiskDriverNvme) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, "uio_pci_generic", executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind the disk %v with uio_pci_generic", diskPath)
	}

	bdevs, err := spdkClient.BdevNvmeAttachController(diskName, "", diskPath, "", "PCIe", "",
		helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec)
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach the disk %v with %v", diskPath, diskDriver)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach the disk %v with %v", diskPath, diskDriver)
	}
	return bdevs[0], nil
}

func (d *DiskDriverNvme) DiskDelete(spdkClient *spdkclient.Client, diskName string) (deleted bool, err error) {
	return spdkClient.BdevVirtioDetachController(diskName)
}

func (d *DiskDriverNvme) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	bdevs, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bdevs")
	}
	foundBdevs := []spdktypes.BdevInfo{}
	for _, bdev := range bdevs {
		if bdev.DriverSpecific == nil {
			continue
		}
		if bdev.DriverSpecific.Nvme == nil {
			continue
		}
		nvmes := *bdev.DriverSpecific.Nvme
		for _, nvme := range nvmes {
			if nvme.PciAddress == diskPath {
				logrus.Infof("Found bdev %v for disk %v", bdev, diskName)
				foundBdevs = append(foundBdevs, bdev)
			}
		}
	}
	return foundBdevs, nil
}
