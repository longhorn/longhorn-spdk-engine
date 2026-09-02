package spdk

import (
	"context"

	. "gopkg.in/check.v1"

	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"
)

// TestDiskCreateRetryKeepsPreviousErrorMsg covers longhorn/longhorn#13893: a retried
// creation restarts from the creating state and can wait behind the disk creation
// lock for several monitoring cycles, so DiskGet has to keep reporting the previous
// reason instead of an empty message.
func (s *TestSuite) TestDiskCreateRetryKeepsPreviousErrorMsg(c *C) {
	const (
		diskName = "test-disk"
		diskPath = "0000:05:00.0"
		errorMsg = "failed to get disk driver: device is not driven by the kernel"
	)

	failed := NewDisk(diskName, "test-uuid", diskPath, "", 4096)
	failed.State = DiskStateError
	failed.ErrorMsg = errorMsg

	server := &Server{diskMap: map[string]*Disk{diskName: failed}}

	_, err := server.DiskCreate(context.Background(), &spdkrpc.DiskCreateRequest{
		DiskName: diskName,
		DiskUuid: "test-uuid",
		DiskPath: diskPath,
	})
	c.Assert(err, IsNil)

	retried := server.diskMap[diskName]
	c.Assert(retried, Not(Equals), failed)
	c.Assert(retried.GetState(), Equals, DiskStateCreating)
	c.Assert(retried.GetErrorMsg(), Equals, errorMsg)

	disk, err := retried.DiskGet(nil, diskName, diskPath, "")
	c.Assert(err, IsNil)
	c.Assert(disk.State, Equals, string(DiskStateCreating))
	c.Assert(disk.Message, Equals, errorMsg)
}
