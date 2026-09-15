package spdk

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestReadSnapshotReturnsDataAndReadErrors(c *C) {
	file, err := os.CreateTemp(c.MkDir(), "snapshot")
	c.Assert(err, IsNil)
	defer file.Close()
	payload := []byte("snapshot data")
	_, err = file.Write(payload)
	c.Assert(err, IsNil)

	b := &Backup{
		replica: &Replica{Name: "replica", State: types.InstanceStateRunning},
		devFh:   file,
	}
	data := make([]byte, 4)
	c.Assert(b.ReadSnapshot("snapshot", "volume", 9, data), IsNil)
	c.Assert(data, DeepEquals, []byte("data"))
	c.Assert(b.ReadSnapshot("snapshot", "volume", int64(len(payload)), data), Equals, io.EOF)
}

func (s *TestSuite) TestReadSnapshotStoppedReplicaReturnsError(c *C) {
	b := &Backup{replica: &Replica{Name: "replica", State: types.InstanceStateStopped}}
	c.Assert(b.ReadSnapshot("snapshot", "volume", 0, make([]byte, 4096)), ErrorMatches, "replica replica is in stopped state")
}

func (s *TestSuite) TestReadSnapshotMissingFileReturnsError(c *C) {
	b := &Backup{
		Name:    "backup",
		replica: &Replica{Name: "replica", State: types.InstanceStateRunning},
	}
	c.Assert(b.ReadSnapshot("snapshot", "volume", 0, make([]byte, 4096)), ErrorMatches, "backup backup snapshot is closed")
}

func (s *TestSuite) TestReadSnapshotAfterCleanupReturnsError(c *C) {
	original := backupStopExposeBdev
	defer func() { backupStopExposeBdev = original }()
	backupStopExposeBdev = func(_ *spdkclient.Client, _ string) error { return nil }

	b := &Backup{
		Name:    "backup",
		replica: &Replica{Name: "replica", State: types.InstanceStateRunning},
		log:     logrus.New(),
	}
	c.Assert(b.CloseSnapshot("snapshot", "volume"), IsNil)
	c.Assert(b.replica, IsNil)
	c.Assert(b.ReadSnapshot("snapshot", "volume", 0, make([]byte, 4096)), ErrorMatches, "backup backup snapshot is closed")
}

func (s *TestSuite) TestReadSnapshotConcurrentCleanup(c *C) {
	original := backupStopExposeBdev
	defer func() { backupStopExposeBdev = original }()
	backupStopExposeBdev = func(_ *spdkclient.Client, _ string) error { return nil }

	file, err := os.CreateTemp(c.MkDir(), "snapshot")
	c.Assert(err, IsNil)
	defer file.Close()
	payload := bytes.Repeat([]byte{0x5a}, 4096)
	_, err = file.Write(payload)
	c.Assert(err, IsNil)
	b := &Backup{
		Name:    "backup",
		replica: &Replica{Name: "replica", State: types.InstanceStateRunning},
		devFh:   file,
		log:     logrus.New(),
	}

	const readerCount = 32
	firstReads := make(chan error, readerCount)
	results := make(chan error, readerCount)
	start := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < readerCount; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			data := make([]byte, len(payload))
			read := func() error {
				if err := b.ReadSnapshot("snapshot", "volume", 0, data); err != nil {
					return err
				}
				if !bytes.Equal(data, payload) {
					return fmt.Errorf("snapshot data differs from the original payload")
				}
				return nil
			}
			firstReads <- read()
			<-start
			for j := 0; j < 1000; j++ {
				if err := read(); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	for i := 0; i < readerCount; i++ {
		c.Check(<-firstReads, IsNil)
	}
	close(start)
	c.Check(b.CloseSnapshot("snapshot", "volume"), IsNil)
	readers.Wait()
	for i := 0; i < readerCount; i++ {
		if err := <-results; err != nil {
			c.Check(err, ErrorMatches, "backup backup snapshot is closed")
		}
	}
	c.Assert(b.replica, IsNil)
	c.Assert(b.devFh, IsNil)
	c.Assert(b.ReadSnapshot("snapshot", "volume", 0, make([]byte, 4096)), ErrorMatches, "backup backup snapshot is closed")
}
