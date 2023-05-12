package spdk

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

func getDiskPath(path string) string {
	return filepath.Join(hostPrefix, path)
}

func getDiskID(filename string) (string, error) {
	executor := util.NewTimeoutExecutor(util.CmdTimeout)
	dev, err := util.DetectDevice(filename, executor)
	if err != nil {
		return "", errors.Wrap(err, "failed to detect disk device")
	}

	return fmt.Sprintf("%d-%d", dev.Major, dev.Minor), nil
}

func isBlockDevice(fullPath string) (bool, error) {
	var st unix.Stat_t
	err := unix.Stat(fullPath, &st)
	if err != nil {
		return false, err
	}

	return (st.Mode & unix.S_IFMT) == unix.S_IFBLK, nil
}

func getDiskDeviceSize(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open %s", path)
	}
	defer file.Close()

	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to seek %s", path)
	}
	return pos, nil
}

func (s *Server) validateDiskCreateRequest(req *spdkrpc.DiskCreateRequest) error {
	ok, err := isBlockDevice(req.DiskPath)
	if err != nil {
		return errors.Wrap(err, "failed to check if disk is a block device")
	}
	if !ok {
		return errors.Wrapf(err, "disk %v is not a block device", req.DiskPath)
	}

	size, err := getDiskDeviceSize(req.DiskPath)
	if err != nil {
		return errors.Wrap(err, "failed to get disk size")
	}
	if size == 0 {
		return fmt.Errorf("disk %v size is 0", req.DiskPath)
	}

	diskID, err := getDiskID(getDiskPath(req.DiskPath))
	if err != nil {
		return errors.Wrap(err, "failed to get disk device number")
	}

	bdevs, err := s.spdkClient.BdevAioGet("", 0)
	if err != nil {
		return errors.Wrap(err, "failed to get AIO bdevs")
	}

	for _, bdev := range bdevs {
		id, err := getDiskID(bdev.DriverSpecific.Aio.FileName)
		if err != nil {
			return errors.Wrap(err, "failed to get disk device number")
		}

		if id == diskID {
			return fmt.Errorf("disk %v is already used by AIO bdev %v", req.DiskPath, bdev.Name)
		}
	}

	return nil
}

func (s *Server) addBlockDevice(diskName, diskPath string, blockSize uint64) (string, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":  diskName,
		"diskPath":  diskPath,
		"blockSize": blockSize,
	})

	log.Infof("Creating AIO bdev %v with block size %v", diskName, blockSize)
	bdevName, err := s.spdkClient.BdevAioCreate(getDiskPath(diskPath), diskName, blockSize)
	if err != nil {
		resp, parseErr := parseErrorMessage(err.Error())
		if parseErr != nil || !isFileExists(resp.Message) {
			return "", errors.Wrapf(err, "failed to create AIO bdev")
		}
	}

	log.Infof("Creating lvstore %v", diskName)

	// Name of the lvstore is the same as the name of the aio bdev
	lvstoreName := bdevName
	lvstores, err := s.spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return "", errors.Wrapf(err, "failed to get lvstores")
	}

	for _, lvstore := range lvstores {
		if lvstore.BaseBdev != bdevName {
			continue
		}

		log.Infof("Found an existing lvstore %v", lvstore.Name)
		if lvstore.Name == lvstoreName {
			return lvstore.UUID, nil
		}

		log.Infof("Renaming the existing lvstore %v to %v", lvstore.Name, lvstoreName)
		renamed, err := s.spdkClient.BdevLvolRenameLvstore(lvstore.Name, lvstoreName)
		if err != nil {
			return "", errors.Wrapf(err, "failed to rename lvstore from %v to %v", lvstore.Name, lvstoreName)
		}
		if !renamed {
			return "", fmt.Errorf("failed to rename lvstore from %v to %v", lvstore.Name, lvstoreName)
		}
		return lvstore.UUID, nil
	}

	return s.spdkClient.BdevLvolCreateLvstore(lvstoreName, diskName, defaultClusterSize)
}

func (s *Server) lvstoreToDisk(diskPath, lvstoreName, lvstoreUUID string) (*spdkrpc.Disk, error) {
	lvstores, err := s.spdkClient.BdevLvolGetLvstore(lvstoreName, lvstoreUUID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get lvstore with name %v and UUID %v", lvstoreName, lvstoreUUID)
	}
	lvstore := &lvstores[0]

	// A disk does not have a fsid, so we use the device number as the disk ID
	diskID, err := getDiskID(getDiskPath(diskPath))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get disk ID")
	}

	return &spdkrpc.Disk{
		Id:          diskID,
		Uuid:        lvstore.UUID,
		Path:        diskPath,
		Type:        DiskTypeBlock,
		TotalSize:   int64(lvstore.TotalDataClusters * lvstore.ClusterSize),
		FreeSize:    int64(lvstore.FreeClusters * lvstore.ClusterSize),
		TotalBlocks: int64(lvstore.TotalDataClusters * lvstore.ClusterSize / lvstore.BlockSize),
		FreeBlocks:  int64(lvstore.FreeClusters * lvstore.ClusterSize / lvstore.BlockSize),
		BlockSize:   int64(lvstore.BlockSize),
		ClusterSize: int64(lvstore.ClusterSize),
	}, nil
}

var errorMessageRegExp = regexp.MustCompile(`"code": (-?\d+),\n\t"message": "([^"]+)"`)

type ErrorMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func parseErrorMessage(errStr string) (*ErrorMessage, error) {
	match := errorMessageRegExp.FindStringSubmatch(errStr)
	if len(match) == 0 {
		return nil, fmt.Errorf("failed to parse error message")
	}

	code := 0
	if _, err := fmt.Sscanf(match[1], "%d", &code); err != nil {
		return nil, fmt.Errorf("failed to parse error code: %w", err)
	}

	em := &ErrorMessage{
		Code:    code,
		Message: match[2],
	}

	return em, nil
}

func isFileExists(message string) bool {
	return strings.EqualFold(message, syscall.Errno(syscall.EEXIST).Error())
}

func isNoSuchDevice(message string) bool {
	return strings.EqualFold(message, syscall.Errno(syscall.ENODEV).Error())
}
