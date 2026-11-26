package spdk

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/longhorn/backupstore"
	btypes "github.com/longhorn/backupstore/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"
	"github.com/sirupsen/logrus"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"

	. "gopkg.in/check.v1"
)

func validBackupEncryptionCredential() map[string]string {
	return map[string]string{
		btypes.BackupEncryptionSecret: "backup-enc-secret",
		btypes.BackupEncryptionKey:    base64.StdEncoding.EncodeToString(make([]byte, 32)),
	}
}

func invalidBackupEncryptionCredential() map[string]string {
	return map[string]string{
		btypes.BackupEncryptionSecret: "backup-enc-secret",
	}
}

func (s *TestSuite) TestReplicaBackupRestoreRejectsInvalidEncryptionCredential(c *C) {
	r := &Replica{
		Name: "replica-enc",
		log:  safelog.NewSafeLogger(logrus.WithField("test", "enc")),
	}

	err := r.BackupRestore(nil, "vfs:///tmp/backups?backup=b1&volume=v1", "snap-1", invalidBackupEncryptionCredential(), 1)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "encryption"), Equals, true)
}

func (s *TestSuite) TestReplicaBackupCreateRejectsInvalidEncryptionCredential(c *C) {
	server := &Server{}

	_, err := server.ReplicaBackupCreate(context.Background(), &spdkrpc.BackupCreateRequest{
		BackupName:   "b1",
		BackupTarget: "vfs:///tmp/backups",
		VolumeName:   "v1",
		Credential:   invalidBackupEncryptionCredential(),
	})
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "encryption"), Equals, true)
}

func (s *TestSuite) TestReplicaBackupRestoreHelpersPassEncryptionConfig(c *C) {
	enc, err := backupstore.NewEncryptionConfig(validBackupEncryptionCredential())
	c.Assert(err, IsNil)
	c.Assert(enc, NotNil)

	r := &Replica{
		Name: "replica-enc",
		log:  safelog.NewSafeLogger(logrus.WithField("test", "enc")),
		restore: &Restore{
			State: btypes.ProgressStateInProgress,
		},
	}

	// Invalid backup URL still executes the Encryption field assignment.
	err = r.backupRestore("vfs:///tmp/does-not-exist-enc-test?backup=b1&volume=v1", "lvol-1", 1, enc)
	c.Assert(err, NotNil)

	err = r.backupRestoreIncrementally("vfs:///tmp/does-not-exist-enc-test?backup=b1&volume=v1", "prev", "lvol-1", 1, enc)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestEngineBackupRestoreHelpersPassEncryptionConfig(c *C) {
	enc, err := backupstore.NewEncryptionConfig(validBackupEncryptionCredential())
	c.Assert(err, IsNil)
	c.Assert(enc, NotNil)

	e := NewEngine("engine-enc", "vol-enc", "spdk-tcp-blockdev", 4096, make(chan interface{}, 1), 250, nil)
	e.restore = &EngineRestore{
		State: btypes.ProgressStateInProgress,
	}

	err = e.backupRestore("vfs:///tmp/does-not-exist-enc-test?backup=b1&volume=v1", 1, enc)
	c.Assert(err, NotNil)

	err = e.backupRestoreIncrementally("vfs:///tmp/does-not-exist-enc-test?backup=b1&volume=v1", "prev", 1, enc)
	c.Assert(err, NotNil)
}
