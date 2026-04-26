package pkg

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	server "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	. "gopkg.in/check.v1"
)

// linkedCloneTestEnv wraps the runtime monitoring env with additional
// helper state for linked-clone tests.
type linkedCloneTestEnv struct {
	*runtimeMonitoringTestEnv
	ip             string
	serviceAddress string
	srcReplicaName string
	dstReplicaName string
	volumeName     string
	engineName     string
	snapshotName   string
}

func newLinkedCloneTestEnv(env *runtimeMonitoringTestEnv) *linkedCloneTestEnv {
	ip, _ := commonnet.GetAnyExternalIP()
	suffix := strings.ReplaceAll(util.UUID(), "-", "")[:8]

	volumeName := fmt.Sprintf("lc-vol-%s", suffix)
	return &linkedCloneTestEnv{
		runtimeMonitoringTestEnv: env,
		ip:                       ip,
		serviceAddress:           net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)),
		srcReplicaName:           fmt.Sprintf("lc-src-r-%s", suffix),
		dstReplicaName:           fmt.Sprintf("lc-dst-r-%s", suffix),
		volumeName:               volumeName,
		engineName:               fmt.Sprintf("%s-e", volumeName),
		snapshotName:             "snap-1",
	}
}

// setupSrcReplicaWithSnapshot creates a source replica and takes a snapshot
// using the replica's own snapshot API. This avoids the engine frontend
// (which requires NVMe/TCP kernel support not available in all test
// environments). Returns a cleanup function.
func (lce *linkedCloneTestEnv) setupSrcReplicaWithSnapshot(c *C) func() {
	spdkCli := lce.spdkCli

	_, err := spdkCli.ReplicaCreate(lce.srcReplicaName, defaultTestDiskName, lce.disk.Uuid,
		defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)

	err = spdkCli.ReplicaSnapshotCreate(lce.srcReplicaName, lce.snapshotName,
		&api.SnapshotOptions{UserCreated: true})
	c.Assert(err, IsNil)

	srcReplica, err := spdkCli.ReplicaGet(lce.srcReplicaName)
	c.Assert(err, IsNil)
	c.Assert(srcReplica.State, Equals, types.InstanceStateRunning)
	_, hasSnap := srcReplica.Snapshots[lce.snapshotName]
	c.Assert(hasSnap, Equals, true)

	return func() {
		_ = spdkCli.ReplicaDelete(lce.srcReplicaName, true)
	}
}

// createDstReplica creates the destination (clone) replica.
func (lce *linkedCloneTestEnv) createDstReplica(c *C) {
	_, err := lce.spdkCli.ReplicaCreate(lce.dstReplicaName, defaultTestDiskName,
		lce.disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
}

// startLinkedClone performs the linked-clone operation.
func (lce *linkedCloneTestEnv) startLinkedClone(c *C) {
	err := lce.spdkCli.ReplicaSnapshotCloneDstStart(
		lce.dstReplicaName, lce.snapshotName,
		lce.srcReplicaName, lce.serviceAddress,
		spdkrpc.CloneMode_CLONE_MODE_LINKED_CLONE)
	c.Assert(err, IsNil)
}

// ---------------------------------------------------------------------------
// Test Case 1: Linked-clone volume should be readable and writable
// ---------------------------------------------------------------------------

func (s *TestSuite) TestLinkedCloneReplicaReadableWritable(c *C) {
	fmt.Println("Testing linked-clone replica is readable and writable via NVMe/TCP frontend")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		lce.createDstReplica(c)
		defer func() {
			_ = lce.spdkCli.ReplicaDelete(lce.dstReplicaName, true)
		}()

		lce.startLinkedClone(c)

		dstReplica, err := lce.spdkCli.ReplicaGet(lce.dstReplicaName)
		c.Assert(err, IsNil)
		c.Assert(dstReplica.State, Equals, types.InstanceStateRunning)

		replicaAddressMap := map[string]string{
			lce.dstReplicaName: net.JoinHostPort(lce.ip, strconv.Itoa(int(dstReplica.PortStart))),
		}

		engine, err := lce.spdkCli.EngineCreate(lce.engineName, lce.volumeName,
			types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		defer func() {
			_ = lce.spdkCli.EngineDelete(lce.engineName)
		}()

		engineFrontendName := fmt.Sprintf("%s-ef", lce.volumeName)
		engineFrontend, err := lce.spdkCli.EngineFrontendCreate(engineFrontendName, lce.volumeName, lce.engineName,
			types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
			net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
		c.Assert(err, IsNil)
		c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)
		defer func() {
			_ = lce.spdkCli.EngineFrontendDelete(engineFrontendName)
		}()

		endpoint := helperutil.GetLonghornDevicePath(lce.volumeName)
		c.Assert(engineFrontend.Endpoint, Equals, endpoint)

		err = formatBlockDevice(endpoint, "ext4")
		c.Assert(err, IsNil)

		ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
		c.Assert(err, IsNil)

		// Write 1MB of zeros and compute checksum
		_, err = ne.Execute(nil, "dd",
			[]string{
				"if=/dev/zero",
				fmt.Sprintf("of=%s", endpoint),
				"bs=1M", "count=1", "oflag=direct", "conv=notrunc", "status=none",
			},
			defaultTestExecuteTimeout,
		)
		c.Assert(err, IsNil)

		checksumZero, err := ne.Execute(nil, "sh",
			[]string{
				"-c",
				fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct status=none | md5sum", endpoint),
			},
			defaultTestExecuteTimeout,
		)
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(checksumZero), Not(Equals), "")

		// Overwrite with random data
		_, err = ne.Execute(nil, "dd",
			[]string{
				"if=/dev/urandom",
				fmt.Sprintf("of=%s", endpoint),
				"bs=1M", "count=1", "oflag=direct", "conv=notrunc", "status=none",
			},
			defaultTestExecuteTimeout,
		)
		c.Assert(err, IsNil)

		// Read back and verify checksum changed (proves both write and read work)
		checksumRandom, err := ne.Execute(nil, "sh",
			[]string{
				"-c",
				fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct status=none | md5sum", endpoint),
			},
			defaultTestExecuteTimeout,
		)
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(checksumRandom), Not(Equals), "")
		c.Assert(strings.TrimSpace(checksumRandom), Not(Equals), strings.TrimSpace(checksumZero))
	})
}

// ---------------------------------------------------------------------------
// Test Case 2: Source replica and snapshots cannot be cleaned up when
//              there are active clone replicas
// ---------------------------------------------------------------------------

func (s *TestSuite) TestLinkedCloneSourceDeleteBlocked(c *C) {
	fmt.Println("Testing linked-clone source replica and snapshot deletion is blocked by active clones")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)

		lce.createDstReplica(c)

		lce.startLinkedClone(c)

		// Attempt to delete source replica with cleanup should be blocked
		// (the guard fires early, leaving the replica in error state)
		err := lce.spdkCli.ReplicaDelete(lce.srcReplicaName, true)
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "clone entrypoint") || strings.Contains(err.Error(), "active clone replicas"),
			Equals, true, Commentf("expected clone entrypoint blocking error, got: %v", err))

		// Verify source replica is still present (in error state after blocked delete)
		srcReplica, err := lce.spdkCli.ReplicaGet(lce.srcReplicaName)
		c.Assert(err, IsNil)
		c.Assert(srcReplica, NotNil)

		// Now delete the clone replica (with cleanup) to unblock
		err = lce.spdkCli.ReplicaDelete(lce.dstReplicaName, true)
		c.Assert(err, IsNil)

		// Wait for sync to clean up the childless entrypoint on the error-state src replica
		time.Sleep(3 * server.MonitorInterval)

		// Now delete source replica with cleanup should succeed
		// (entrypoint was cleaned up by sync, guard passes even though replica is in error state)
		err = lce.spdkCli.ReplicaDelete(lce.srcReplicaName, true)
		c.Assert(err, IsNil)

		// Override cleanupSrc since we already cleaned up
		cleanupSrc()
	})
}

func (s *TestSuite) TestLinkedCloneSnapshotDeleteBlocked(c *C) {
	fmt.Println("Testing linked-clone snapshot deletion is blocked by clone entrypoint")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		lce.createDstReplica(c)
		defer func() {
			_ = lce.spdkCli.ReplicaDelete(lce.dstReplicaName, true)
		}()

		lce.startLinkedClone(c)

		// Create an engine for the source replica (no frontend needed)
		srcReplica, err := lce.spdkCli.ReplicaGet(lce.srcReplicaName)
		c.Assert(err, IsNil)
		srcAddrMap := map[string]string{
			lce.srcReplicaName: net.JoinHostPort(lce.ip, strconv.Itoa(int(srcReplica.PortStart))),
		}
		_, err = lce.spdkCli.EngineCreate(lce.engineName, lce.volumeName,
			types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, srcAddrMap, 1, false)
		c.Assert(err, IsNil)
		defer func() {
			_ = lce.spdkCli.EngineDelete(lce.engineName)
		}()

		// Attempt to delete the snapshot that has an entrypoint should be blocked
		err = lce.spdkCli.EngineSnapshotDelete(lce.engineName, lce.snapshotName)
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "clone entrypoint") || strings.Contains(err.Error(), "active clone replicas"),
			Equals, true, Commentf("expected snapshot delete blocked by entrypoint, got: %v", err))

		// Verify the snapshot still exists on the engine
		engine, err := lce.spdkCli.EngineGet(lce.engineName)
		c.Assert(err, IsNil)
		_, exists := engine.Snapshots[lce.snapshotName]
		c.Assert(exists, Equals, true)
	})
}

// ---------------------------------------------------------------------------
// Test Case 3: recoverCloneEntrypointInfo and recoverCloneReplicaInfo
// ---------------------------------------------------------------------------

func (s *TestSuite) TestLinkedCloneRecoverAfterRestart(c *C) {
	fmt.Println("Testing linked-clone recovery of entrypoint and clone replica info after restart")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		lce.createDstReplica(c)
		defer func() {
			_ = lce.spdkCli.ReplicaDelete(lce.dstReplicaName, true)
		}()

		lce.startLinkedClone(c)

		// Simulate restart: delete without cleanup (soft stop), then recreate.
		// This forces the replica to reconstruct from SPDK state.
		err := lce.spdkCli.ReplicaDelete(lce.srcReplicaName, false)
		c.Assert(err, IsNil)
		err = lce.spdkCli.ReplicaDelete(lce.dstReplicaName, false)
		c.Assert(err, IsNil)

		// Recreate replicas — they will recover from SPDK state via construct()
		_, err = lce.spdkCli.ReplicaCreate(lce.srcReplicaName, defaultTestDiskName,
			lce.disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		_, err = lce.spdkCli.ReplicaCreate(lce.dstReplicaName, defaultTestDiskName,
			lce.disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)

		// Verify source replica recovered and is running
		srcReplica, err := lce.spdkCli.ReplicaGet(lce.srcReplicaName)
		c.Assert(err, IsNil)
		c.Assert(srcReplica.State, Equals, types.InstanceStateRunning)

		// Verify clone replica recovered and is running
		dstReplica, err := lce.spdkCli.ReplicaGet(lce.dstReplicaName)
		c.Assert(err, IsNil)
		c.Assert(dstReplica.State, Equals, types.InstanceStateRunning)

		// Verify entrypoint still exists in SPDK after restart
		epLvolName := server.GetCloneEntrypointLvolName(lce.srcReplicaName, lce.snapshotName)
		epAlias := spdktypes.GetLvolAlias(lce.disk.Name, epLvolName)
		_, err = lce.rawSPDKCli.BdevLvolGetByName(epAlias, 0)
		c.Assert(err, IsNil)

		// Verify source still blocks cleanup deletion (entrypoint guard preserved across restart)
		err = lce.spdkCli.ReplicaDelete(lce.srcReplicaName, true)
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "clone entrypoint") || strings.Contains(err.Error(), "active clone replicas"),
			Equals, true, Commentf("expected entrypoint blocking after recovery, got: %v", err))
	})
}

// ---------------------------------------------------------------------------
// Test Case 4: syncCloneEntrypoints — tmp head and childless ep cleanup
// ---------------------------------------------------------------------------

func (s *TestSuite) TestLinkedCloneSyncCleanupChildlessEntrypoint(c *C) {
	fmt.Println("Testing linked-clone syncCloneEntrypoints cleans up childless entrypoint")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		lce.createDstReplica(c)

		lce.startLinkedClone(c)

		// Delete the clone replica (with cleanup) — this removes its lvols
		err := lce.spdkCli.ReplicaDelete(lce.dstReplicaName, true)
		c.Assert(err, IsNil)

		// The entrypoint should now be childless.
		// Wait for sync to detect and auto-cleanup the childless entrypoint.
		epLvolName := server.GetCloneEntrypointLvolName(lce.srcReplicaName, lce.snapshotName)
		epAlias := spdktypes.GetLvolAlias(lce.disk.Name, epLvolName)

		err = retry.Do(func() error {
			_, getErr := lce.rawSPDKCli.BdevLvolGetByName(epAlias, 0)
			if getErr != nil {
				// Entrypoint was cleaned up — success
				return nil
			}
			return fmt.Errorf("entrypoint %s still exists, waiting for sync cleanup", epLvolName)
		}, monitoringRetryOpts(lce.ctx, 20)...)
		c.Assert(err, IsNil)

		// Verify source replica can now be deleted with cleanup
		err = lce.spdkCli.ReplicaDelete(lce.srcReplicaName, false)
		c.Assert(err, IsNil)
		_, err = lce.spdkCli.ReplicaCreate(lce.srcReplicaName, defaultTestDiskName,
			lce.disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		err = lce.spdkCli.ReplicaDelete(lce.srcReplicaName, true)
		c.Assert(err, IsNil)
	})
}

func (s *TestSuite) TestLinkedCloneSyncCleanupOrphanedTmpHead(c *C) {
	fmt.Println("Testing linked-clone syncCloneEntrypoints cleans up orphaned tmp-head")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		// Create a fake orphaned tmp-head lvol for the source replica's snapshot.
		// This simulates a crashed entrypoint creation that left behind a tmp-head.
		srcSnapLvolName := server.GetReplicaSnapshotLvolName(lce.srcReplicaName, lce.snapshotName)
		srcSnapAlias := spdktypes.GetLvolAlias(lce.disk.Name, srcSnapLvolName)
		srcSnapBdev, err := lce.rawSPDKCli.BdevLvolGetByName(srcSnapAlias, 0)
		c.Assert(err, IsNil)

		tmpHeadName := server.GetCloneEntrypointTmpHeadLvolName(lce.srcReplicaName, lce.snapshotName)
		_, err = lce.rawSPDKCli.BdevLvolClone(srcSnapBdev.UUID, tmpHeadName)
		c.Assert(err, IsNil)

		tmpHeadAlias := spdktypes.GetLvolAlias(lce.disk.Name, tmpHeadName)

		// Verify the tmp-head exists
		_, err = lce.rawSPDKCli.BdevLvolGetByName(tmpHeadAlias, 0)
		c.Assert(err, IsNil)

		// Wait for sync to detect and clean up the orphaned tmp-head
		err = retry.Do(func() error {
			_, getErr := lce.rawSPDKCli.BdevLvolGetByName(tmpHeadAlias, 0)
			if getErr != nil {
				return nil
			}
			return fmt.Errorf("tmp-head %s still exists, waiting for sync cleanup", tmpHeadName)
		}, monitoringRetryOpts(lce.ctx, 20)...)
		c.Assert(err, IsNil)
	})
}
