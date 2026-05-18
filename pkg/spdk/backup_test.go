package spdk

import (
	"github.com/sirupsen/logrus"

	btypes "github.com/longhorn/backupstore/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestUpdateBackupStatusPrefersErrorOverComplete(c *C) {
	b := &Backup{log: logrus.New()}

	err := b.UpdateBackupStatus("snap-1", "vol-1", string(btypes.ProgressStateInProgress), 100, "backup-url", "upload failed")

	c.Assert(err, IsNil)
	c.Assert(b.State, Equals, btypes.ProgressStateError)
	c.Assert(b.Progress, Equals, 100)
	c.Assert(b.Error, Equals, "upload failed")
	c.Assert(b.BackupURL, Equals, "backup-url")
}

func (s *TestSuite) TestUpdateBackupStatusKeepsErrorAsFinalState(c *C) {
	b := &Backup{log: logrus.New()}

	err := b.UpdateBackupStatus("snap-1", "vol-1", string(btypes.ProgressStateInProgress), 42, "", "upload failed")
	c.Assert(err, IsNil)
	c.Assert(b.State, Equals, btypes.ProgressStateError)

	err = b.UpdateBackupStatus("snap-1", "vol-1", string(btypes.ProgressStateInProgress), 100, "backup-url", "")

	c.Assert(err, IsNil)
	c.Assert(b.State, Equals, btypes.ProgressStateError)
	c.Assert(b.Progress, Equals, 42)
	c.Assert(b.Error, Equals, "upload failed")
	c.Assert(b.BackupURL, Equals, "")
}
