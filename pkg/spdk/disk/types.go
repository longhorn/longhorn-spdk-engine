package disk

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/pkg/errors"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type BlockDiskDriver string

const (
	BlockDiskDriverAuto       = BlockDiskDriver("auto")
	BlockDiskDriverAio        = BlockDiskDriver("aio")
	BlockDiskDriverNvme       = BlockDiskDriver("nvme")
	BlockDiskDriverVirtio     = BlockDiskDriver("virtio")
	BlockDiskDriverVirtioScsi = BlockDiskDriver("virtio-scsi")
	BlockDiskDriverVirtioBlk  = BlockDiskDriver("virtio-blk")
)

type BlockDiskSubsystem string

const (
	BlockDiskSubsystemVirtio = BlockDiskSubsystem("virtio")
	BlockDiskSubsystemPci    = BlockDiskSubsystem("pci")
	BlockDiskSubsystemNvme   = BlockDiskSubsystem("nvme")
	BlockDiskSubsystemScsi   = BlockDiskSubsystem("scsi")
)

type BlockDiskType string

const (
	BlockDiskTypeDisk = BlockDiskType("disk")
	BlockDiskTypeLoop = BlockDiskType("loop")
)

func GetDiskDriver(diskDriver, diskPath string) (BlockDiskDriver, error) {
	devName, err := util.GetDevNameFromBDF(diskPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get device name from BDF %s", diskPath)
	}

	var exactDiskDriver BlockDiskDriver
	diskType, err := util.GetBlockDiskType(devName)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk type for %s", devName)
	}

	switch BlockDiskType(diskType) {
	case BlockDiskTypeDisk:
		subsystems, err := util.GetBlockDiskSubsystems(devName)
		if err != nil {
			return "", errors.Wrapf(err, "failed to get disk subsystems for %s", devName)
		}
		if slices.Contains(subsystems, string(BlockDiskSubsystemVirtio)) && slices.Contains(subsystems, string(BlockDiskSubsystemPci)) {
			if diskDriver != string(BlockDiskDriverAuto) && diskDriver != string(BlockDiskDriverVirtio) {
				return "", fmt.Errorf("unsupported disk driver %s for disk type %s", diskDriver, diskType)
			}

			exactDiskDriver = BlockDiskDriverVirtioBlk
			if slices.Contains(subsystems, string(BlockDiskSubsystemScsi)) {
				exactDiskDriver = BlockDiskDriverVirtioScsi
			}
		} else if slices.Contains(subsystems, string(BlockDiskSubsystemNvme)) {
			if diskDriver != string(BlockDiskDriverAuto) && diskDriver != string(BlockDiskDriverNvme) {
				return "", fmt.Errorf("unsupported disk driver %s for disk type %s", diskDriver, diskType)
			}

			exactDiskDriver = BlockDiskDriverNvme
		} else {
			return "", fmt.Errorf("unsupported disk subsystems %v for disk type %s", subsystems, diskType)
		}
	case BlockDiskTypeLoop:
		if diskDriver != string(BlockDiskDriverAuto) && diskDriver != string(BlockDiskDriverAio) {
			return "", fmt.Errorf("unsupported disk driver %s for disk type %s", diskDriver, diskType)
		}
		exactDiskDriver = BlockDiskDriverAio
	default:
		return "", fmt.Errorf("unsupported disk type %s", diskType)
	}

	return exactDiskDriver, nil
}

func isBDF(addr string) bool {
	bdfFormat := "[a-f0-9]{4}:[a-f0-9]{2}:[a-f0-9]{2}\\.[a-f0-9]{1}"
	bdfPattern := regexp.MustCompile(bdfFormat)
	return bdfPattern.MatchString(addr)
}
