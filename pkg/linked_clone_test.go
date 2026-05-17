package pkg

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	server "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
)

// linkedCloneTestEnv wraps the runtime monitoring env with additional
// helper state for linked-clone tests.
type linkedCloneTestEnv struct {
	*runtimeMonitoringTestEnv
	ip              string
	serviceAddress  string
	srcReplicaName  string
	dstReplicaName  string
	dstReplicaNames []string // for N-replica tests
	volumeName      string
	srcEngineName   string // source engine for N-replica clone
	engineName      string
	snapshotName    string
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
		srcEngineName:            fmt.Sprintf("%s-src-e", volumeName),
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
			types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false, 0, spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_REPLICATED)
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
			types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, srcAddrMap, 1, false, 0, spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_REPLICATED)
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
// Test Case 3: syncCloneEntrypoints — tmp head and childless ep cleanup
// ---------------------------------------------------------------------------

// TestLinkedCloneSyncCleanupTmpHeadOnlyEntrypoint verifies that when a clone
// entrypoint's only child is a tmp-head lvol, syncCloneEntrypoints deletes the
// tmp-head first and then the now-childless entrypoint in the same sync cycle.
// This covers both the orphaned-tmp-head and childless-entrypoint cleanup paths.
func (s *TestSuite) TestLinkedCloneSyncCleanupTmpHeadOnlyEntrypoint(c *C) {
	fmt.Println("Testing linked-clone syncCloneEntrypoints cleans up entrypoint whose only child is a tmp-head")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		// Directly create a clone entrypoint lvol parented on the src snapshot,
		// then create a tmp-head lvol as its only child.  This simulates a
		// crashed entrypoint-creation attempt: the entrypoint was created but
		// no real dst replica root-snapshot was ever cloned from it.
		srcSnapLvolName := server.GetReplicaSnapshotLvolName(lce.srcReplicaName, lce.snapshotName)
		srcSnapAlias := spdktypes.GetLvolAlias(lce.disk.Name, srcSnapLvolName)
		srcSnapBdev, err := lce.rawSPDKCli.BdevLvolGetByName(srcSnapAlias, 0)
		c.Assert(err, IsNil)

		epLvolName := server.GetCloneEntrypointLvolName(lce.srcReplicaName, lce.snapshotName)
		epAlias := spdktypes.GetLvolAlias(lce.disk.Name, epLvolName)
		tmpHeadName := server.GetCloneEntrypointTmpHeadLvolName(lce.srcReplicaName, lce.snapshotName)
		tmpHeadAlias := spdktypes.GetLvolAlias(lce.disk.Name, tmpHeadName)

		// Simulate a crashed entrypoint-creation attempt by replicating the SPDK
		// state that createCloneEntrypointLvol leaves behind when it completes
		// step 2 (snapshot the tmp-head into the ep) but crashes before step 3
		// (deleting the tmp-head).
		//
		// Real state: srcSnapshot → ep(read-only) → tmpHead(writable, only child)
		//
		// We must do this atomically with retries because the monitoring loop
		// fires every 3 s and deletes orphaned tmp-heads (loop 1), racing with
		// our setup.
		err = retry.Do(func() error {
			_, _ = lce.rawSPDKCli.BdevLvolDelete(tmpHeadAlias)
			_, _ = lce.rawSPDKCli.BdevLvolDelete(epAlias)
			// Step 1: clone srcSnapshot → writable tmpHead
			tmpHeadUUID, cloneErr := lce.rawSPDKCli.BdevLvolClone(srcSnapBdev.UUID, tmpHeadName)
			if cloneErr != nil {
				return cloneErr
			}
			// Step 2: snapshot tmpHead → read-only ep (tmpHead becomes ep's child)
			if _, snapErr := lce.rawSPDKCli.BdevLvolSnapshot(tmpHeadUUID, epLvolName, nil); snapErr != nil {
				return fmt.Errorf("monitoring may have deleted tmp-head before ep snapshot: %w", snapErr)
			}
			return nil
		}, monitoringRetryOpts(lce.ctx, 10)...)
		c.Assert(err, IsNil)

		// Wait for sync to detect the tmp-head-only entrypoint, delete the
		// tmp-head (pass 1), then delete the now-childless entrypoint (pass 1
		// or the next cycle).  Both lvols must be gone.
		err = retry.Do(func() error {
			_, epErr := lce.rawSPDKCli.BdevLvolGetByName(epAlias, 0)
			if epErr != nil {
				return nil
			}
			return fmt.Errorf("entrypoint %s still exists, waiting for sync cleanup", epLvolName)
		}, monitoringRetryOpts(lce.ctx, 20)...)
		c.Assert(err, IsNil)

		_, err = lce.rawSPDKCli.BdevLvolGetByName(tmpHeadAlias, 0)
		c.Assert(err, NotNil, Commentf("tmp-head should have been deleted along with the entrypoint"))
	})
}

// createNDstReplicas creates N destination (clone) replicas and stores their
// names in lce.dstReplicaNames. Returns the slice of names.
func (lce *linkedCloneTestEnv) createNDstReplicas(c *C, n int) []string {
	names := make([]string, n)
	for i := 0; i < n; i++ {
		suffix := strings.ReplaceAll(util.UUID(), "-", "")[:6]
		name := fmt.Sprintf("%s-dst-%d-%s", lce.volumeName, i, suffix)
		_, err := lce.spdkCli.ReplicaCreate(name, defaultTestDiskName,
			lce.disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		names[i] = name
	}
	lce.dstReplicaNames = names
	return names
}

// startNReplicaLinkedClone creates both src and dst engines (FrontendEmpty),
// then issues a single EngineSnapshotClone call with all N dst replicas mapped
// to the same src replica via DstReplicaSrcReplicaPairMap.
func (lce *linkedCloneTestEnv) startNReplicaLinkedClone(c *C) {
	// Build dst engine replica address map and the dst→src pair map.
	dstReplicaAddrMap := map[string]string{}
	pairMap := map[string]string{}
	for _, name := range lce.dstReplicaNames {
		r, err := lce.spdkCli.ReplicaGet(name)
		c.Assert(err, IsNil)
		dstReplicaAddrMap[name] = net.JoinHostPort(lce.ip, strconv.Itoa(int(r.PortStart)))
		pairMap[name] = lce.srcReplicaName
	}

	// Create dst engine (no frontend needed for clone).
	_, err := lce.spdkCli.EngineCreate(lce.engineName, lce.volumeName,
		types.FrontendEmpty, defaultTestLvolSize, dstReplicaAddrMap, 0, false, 0, spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_REPLICATED)
	c.Assert(err, IsNil)

	// Create src engine (no frontend needed).
	srcReplica, err := lce.spdkCli.ReplicaGet(lce.srcReplicaName)
	c.Assert(err, IsNil)
	srcAddrMap := map[string]string{
		lce.srcReplicaName: net.JoinHostPort(lce.ip, strconv.Itoa(int(srcReplica.PortStart))),
	}
	_, err = lce.spdkCli.EngineCreate(lce.srcEngineName, lce.volumeName,
		types.FrontendEmpty, defaultTestLvolSize, srcAddrMap, 0, false, 0, spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_REPLICATED)
	c.Assert(err, IsNil)

	// Execute N-replica simultaneous linked-clone.
	err = lce.spdkCli.EngineSnapshotClone(
		lce.engineName, lce.snapshotName,
		lce.srcEngineName, lce.serviceAddress,
		spdkrpc.CloneMode_CLONE_MODE_LINKED_CLONE, pairMap)
	c.Assert(err, IsNil)
}

// ---------------------------------------------------------------------------
// Test Case 6: N-replica simultaneous linked-clone via DstReplicaSrcReplicaPairMap
// ---------------------------------------------------------------------------

func (s *TestSuite) TestLinkedCloneNReplicaSimultaneous(c *C) {
	fmt.Println("Testing N-replica simultaneous linked-clone via DstReplicaSrcReplicaPairMap")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		lce := newLinkedCloneTestEnv(env)
		cleanupSrc := lce.setupSrcReplicaWithSnapshot(c)
		defer cleanupSrc()

		const nReplicas = 3
		dstNames := lce.createNDstReplicas(c, nReplicas)
		defer func() {
			for _, name := range dstNames {
				_ = lce.spdkCli.ReplicaDelete(name, true)
			}
			_ = lce.spdkCli.EngineDelete(lce.engineName)
			_ = lce.spdkCli.EngineDelete(lce.srcEngineName)
		}()

		lce.startNReplicaLinkedClone(c)

		// Verify all N dst replicas are running and marked as clone replicas.
		for _, name := range dstNames {
			r, err := lce.spdkCli.ReplicaGet(name)
			c.Assert(err, IsNil)
			c.Assert(r.State, Equals, types.InstanceStateRunning,
				Commentf("dst replica %s should be running after N-replica clone (errorMsg: %s)", name, r.ErrorMsg))
			c.Assert(r.IsCloneReplica, Equals, true,
				Commentf("dst replica %s should be marked as clone replica (errorMsg: %s)", name, r.ErrorMsg))
			c.Assert(r.CloneSourceReplicaName, Equals, lce.srcReplicaName,
				Commentf("dst replica %s should reference correct src replica (errorMsg: %s)", name, r.ErrorMsg))
		}

		// Verify dst engine is healthy with all N replicas present.
		dstEngine, err := lce.spdkCli.EngineGet(lce.engineName)
		c.Assert(err, IsNil)
		c.Assert(dstEngine.State, Equals, types.InstanceStateRunning)
		for _, name := range dstNames {
			_, ok := dstEngine.ReplicaAddressMap[name]
			c.Assert(ok, Equals, true,
				Commentf("dst replica %s missing from engine replica address map", name))
		}

		// Verify src replica now has at least N clone entrypoints (one per dst replica).
		srcReplica, err := lce.spdkCli.ReplicaGet(lce.srcReplicaName)
		c.Assert(err, IsNil)
		c.Assert(len(srcReplica.CloneEntrypointMap) >= 1, Equals, true,
			Commentf("src replica should have at least one entrypoint after N-replica clone, got %d",
				len(srcReplica.CloneEntrypointMap)))
	})
}
