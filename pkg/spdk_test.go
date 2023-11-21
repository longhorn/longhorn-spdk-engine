package pkg

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	helperclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	"github.com/longhorn/go-spdk-helper/pkg/spdk/target"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	server "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"

	. "gopkg.in/check.v1"
)

var (
	defaultTestDiskName = "test-disk"
	defaultTestDiskPath = filepath.Join("/tmp", defaultTestDiskName)

	defaultTestBlockSize     = 4096
	defaultTestDiskSize      = uint64(10240 * helpertypes.MiB)
	defaultTestLvolSizeInMiB = uint64(500)
	defaultTestLvolSize      = defaultTestLvolSizeInMiB * helpertypes.MiB

	defaultTestStartPort        = int32(20000)
	defaultTestEndPort          = int32(30000)
	defaultTestReplicaPortCount = int32(5)
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func GetSPDKDir() string {
	spdkDir := os.Getenv("SPDK_DIR")
	if spdkDir != "" {
		return spdkDir
	}
	return filepath.Join(os.Getenv("GOPATH"), "src/github.com/longhorn/spdk")
}

func LaunchTestSPDKTarget(c *C, execute func(name string, args []string) (string, error)) {
	targetReady := false
	if spdkCli, err := helperclient.NewClient(context.Background()); err == nil {
		if _, err := spdkCli.BdevGetBdevs("", 0); err == nil {
			targetReady = true
		}
	}

	if !targetReady {
		go func() {
			err := target.StartTarget(GetSPDKDir(), []string{"--logflag all", "2>&1 | tee /tmp/spdk_tgt.log"}, execute)
			c.Assert(err, IsNil)
		}()

		for cnt := 0; cnt < 30; cnt++ {
			if spdkCli, err := helperclient.NewClient(context.Background()); err == nil {
				if _, err := spdkCli.BdevGetBdevs("", 0); err == nil {
					targetReady = true
					break
				}
			}
			time.Sleep(time.Second)
		}
	}

	c.Assert(targetReady, Equals, true)
}

func LaunchTestSPDKGRPCServer(c *C, ip string, execute func(name string, args []string) (string, error)) {
	LaunchTestSPDKTarget(c, execute)
	srv, err := server.NewServer(context.Background(), defaultTestStartPort, defaultTestEndPort)
	c.Assert(err, IsNil)

	spdkGRPCListener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	spdkGRPCServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))

	spdkrpc.RegisterSPDKServiceServer(spdkGRPCServer, srv)
	reflection.Register(spdkGRPCServer)

	go func() {
		if err := spdkGRPCServer.Serve(spdkGRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping SPDK gRPC server")
		}
	}()
}

func PrepareDiskFile(c *C) string {
	err := os.RemoveAll(defaultTestDiskPath)
	c.Assert(err, IsNil)

	f, err := os.Create(defaultTestDiskPath)
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)

	err = os.Truncate(defaultTestDiskPath, int64(defaultTestDiskSize))
	c.Assert(err, IsNil)

	output, err := helperutil.Execute("losetup", []string{"-f"})
	c.Assert(err, IsNil)

	loopDevicePath := strings.TrimSpace(output)
	c.Assert(loopDevicePath, Not(Equals), "")

	_, err = helperutil.Execute("losetup", []string{loopDevicePath, defaultTestDiskPath})
	c.Assert(err, IsNil)

	return loopDevicePath
}

func CleanupDiskFile(c *C, loopDevicePath string) {
	defer func() {
		err := os.RemoveAll(defaultTestDiskPath)
		c.Assert(err, IsNil)
	}()

	_, err := helperutil.Execute("losetup", []string{"-d", loopDevicePath})
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestSPDKMultipleThread(c *C) {
	fmt.Println("Testing SPDK basic operations with multiple threads")

	ip, err := util.GetExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(util.EnvPodIP, ip)

	LaunchTestSPDKGRPCServer(c, ip, helperutil.Execute)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid)
		c.Assert(err, IsNil)
	}()

	concurrentCount := 10
	dataCountInMB := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-engine", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)

		go func() {
			defer wg.Done()

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, false, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, false, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)
			c.Assert(replica2.PortStart, Not(Equals), int32(0))

			replicaAddressMap := map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
			}
			replicaModeMap := map[string]types.Mode{
				replica1.Name: types.ModeRW,
				replica2.Name: types.ModeRW,
			}
			endpoint := helperutil.GetLonghornDevicePath(volumeName)
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Endpoint, Equals, endpoint)

			_, err = helperutil.Execute("dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), "seek=0", "status=none"})
			c.Assert(err, IsNil)
			cksumBefore1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore1, Not(Equals), "")

			snapshotName1 := "snap1"
			err = spdkCli.EngineSnapshotCreate(engineName, snapshotName1)
			c.Assert(err, IsNil)

			_, err = helperutil.Execute("dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), "seek=200", "status=none"})
			c.Assert(err, IsNil)
			cksumBefore2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore2, Not(Equals), "")

			snapshotName2 := "snap2"
			err = spdkCli.EngineSnapshotCreate(engineName, snapshotName2)
			c.Assert(err, IsNil)

			// Check both replica snapshot map after the snapshot operations
			checkReplicaSnapshots(c, spdkCli, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName1: {snapshotName2},
					snapshotName2: {"volume-head"},
				})

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName2)
			c.Assert(err, IsNil)

			// Detach and re-attach the volume
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			err = spdkCli.ReplicaDelete(replicaName1, false)
			c.Assert(err, IsNil)
			err = spdkCli.ReplicaDelete(replicaName2, false)
			c.Assert(err, IsNil)

			replica1, err = spdkCli.ReplicaGet(replicaName1)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateStopped)
			c.Assert(replica1.PortStart, Equals, int32(0))
			c.Assert(replica1.PortEnd, Equals, int32(0))

			replica2, err = spdkCli.ReplicaGet(replicaName1)
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.State, Equals, types.InstanceStateStopped)
			c.Assert(replica2.PortStart, Equals, int32(0))
			c.Assert(replica2.PortEnd, Equals, int32(0))

			replica1, err = spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, false, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			replica2, err = spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, false, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)

			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
			}
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Endpoint, Equals, endpoint)

			// Check both replica snapshot map after the snapshot deletion and volume re-attachment
			checkReplicaSnapshots(c, spdkCli, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName1: {"volume-head"},
				})

			// Data keeps intact after the snapshot deletion and volume re-attachment
			cksumAfterSnap1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterSnap1, Equals, cksumBefore1)
			cksumAfterSnap2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterSnap2, Equals, cksumBefore2)

			// Restart the engine with empty frontend
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendEmpty, defaultTestLvolSize, replicaAddressMap, 1)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendEmpty)
			c.Assert(engine.Endpoint, Equals, "")

			// Before testing offline rebuilding
			// Crash replica2 and remove it from the engine
			delete(replicaAddressMap, replicaName2)
			err = spdkCli.ReplicaDelete(replicaName2, true)
			c.Assert(err, IsNil)
			err = spdkCli.EngineReplicaDelete(engineName, replicaName2, net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))))
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW})
			c.Assert(engine.Endpoint, Equals, "")

			// Start testing offline rebuilding
			// Launch a new replica then ask the engine to rebuild it
			replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, false, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			c.Assert(replica3.PortStart, Not(Equals), int32(0))

			err = spdkCli.EngineReplicaAdd(engineName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))))
			c.Assert(err, IsNil)

			// Verify the rebuilding result
			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica3.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))),
			}
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)

			// Restart the engine with the newly rebuilt replica
			delete(replicaAddressMap, replicaName3)
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)

			// The newly rebuilt replica should contain correct data
			cksumAfterRebuilding1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding1, Equals, cksumBefore1)
			cksumAfterRebuilding2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding2, Equals, cksumBefore2)

			// Do cleanup
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			err = spdkCli.ReplicaDelete(replicaName1, true)
			c.Assert(err, IsNil)
			err = spdkCli.ReplicaDelete(replicaName2, true)
			c.Assert(err, IsNil)
			err = spdkCli.ReplicaDelete(replicaName3, true)
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	c.Assert(len(engineList), Equals, 0)
	replicaList, err := spdkCli.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(len(replicaList), Equals, 0)
}

func checkReplicaSnapshots(c *C, spdkCli *client.SPDKClient, replicaList []string, snapshotMap map[string][]string) {
	for _, replicaName := range replicaList {
		replica, err := spdkCli.ReplicaGet(replicaName)
		c.Assert(err, IsNil)
		c.Assert(len(replica.Snapshots), Equals, len(snapshotMap))

		for snapName, childrenList := range snapshotMap {
			snap := replica.Snapshots[snapName]
			c.Assert(snap, NotNil)

			for _, childSnapName := range childrenList {
				if childSnapName == "volume-head" {
					c.Assert(snap.Children[replicaName], Equals, true)
				} else {
					childLvolName := server.GetReplicaSnapshotLvolName(replicaName, childSnapName)
					c.Assert(snap.Children[childLvolName], Equals, true)
					childSnap := replica.Snapshots[childSnapName]
					c.Assert(childSnap, NotNil)
					c.Assert(childSnap.Parent, Equals, server.GetReplicaSnapshotLvolName(replicaName, snapName))
				}
			}
		}
	}
}
