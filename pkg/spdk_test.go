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

	commonNet "github.com/longhorn/go-common-libs/net"
	commonTypes "github.com/longhorn/go-common-libs/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	server "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

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

	defaultTestExecuteTimeout = 10 * time.Second
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

func LaunchTestSPDKGRPCServer(ctx context.Context, c *C, ip string) {
	srv, err := server.NewServer(ctx, defaultTestStartPort, defaultTestEndPort)
	c.Assert(err, IsNil)

	spdkGRPCListener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	spdkGRPCServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))
	go func() {
		<-ctx.Done()
		spdkGRPCServer.Stop()
	}()

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

	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	c.Assert(err, IsNil)
	output, err := ne.Execute(nil, "losetup", []string{"-f"}, defaultTestExecuteTimeout)
	c.Assert(err, IsNil)

	loopDevicePath := strings.TrimSpace(output)
	c.Assert(loopDevicePath, Not(Equals), "")

	_, err = ne.Execute(nil, "losetup", []string{loopDevicePath, defaultTestDiskPath}, defaultTestExecuteTimeout)
	c.Assert(err, IsNil)

	return loopDevicePath
}

func CleanupDiskFile(c *C, loopDevicePath string) {
	defer func() {
		err := os.RemoveAll(defaultTestDiskPath)
		c.Assert(err, IsNil)
	}()

	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	c.Assert(err, IsNil)
	_, err = ne.Execute(nil, "losetup", []string{"-d", loopDevicePath}, time.Second)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestSPDKMultipleThread(c *C) {
	fmt.Println("Testing SPDK basic operations with multiple threads")

	diskDriverName := "aio"

	ip, err := commonNet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonNet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
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
		// replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				// TODO: Check why there is a race here
				// err = spdkCli.EngineDelete(engineName)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName1, true)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName2, true)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName3, true)
				// c.Assert(err, IsNil)

				wg.Done()
			}()

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			c.Assert(replica1.Head, NotNil)
			c.Assert(replica1.Head.CreationTime, Not(Equals), "")
			c.Assert(replica1.Head.Parent, Equals, "")
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)
			c.Assert(replica2.PortStart, Not(Equals), int32(0))
			c.Assert(replica2.Head, NotNil)
			c.Assert(replica2.Head.CreationTime, Not(Equals), "")
			c.Assert(replica2.Head.Parent, Equals, "")

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

			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), "seek=0", "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore1, Not(Equals), "")

			snapshotName1 := "snap1"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName1)
			c.Assert(err, IsNil)

			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), "seek=200", "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore2, Not(Equals), "")

			snapshotName2 := "snap2"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName2)
			c.Assert(err, IsNil)

			// Check both replica snapshot map after the snapshot operations
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName1: {snapshotName2},
					snapshotName2: {types.VolumeHead},
				},
				map[string]api.SnapshotOptions{
					snapshotName1: {UserCreated: true},
					snapshotName2: {UserCreated: true},
				})

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName1)
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

			replica1, err = spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			replica2, err = spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
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
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName2: {types.VolumeHead},
				},
				nil)

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
			// replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			// c.Assert(err, IsNil)
			// c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			// c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			// c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			// c.Assert(replica3.PortStart, Not(Equals), int32(0))
			// c.Assert(replica3.Head, NotNil)
			// c.Assert(replica3.Head.CreationTime, Not(Equals), "")
			// c.Assert(replica3.Head.Parent, Equals, "")

			// err = spdkCli.EngineReplicaAdd(engineName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))))
			// c.Assert(err, IsNil)

			// // Verify the rebuilding result
			// replicaAddressMap = map[string]string{
			// 	replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
			// 	replica3.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))),
			// }
			// engine, err = spdkCli.EngineGet(engineName)
			// c.Assert(err, IsNil)
			// c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)

			// // Restart the engine with the newly rebuilt replica
			// delete(replicaAddressMap, replicaName3)
			// err = spdkCli.EngineDelete(engineName)
			// c.Assert(err, IsNil)
			// engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1)
			// c.Assert(err, IsNil)
			// c.Assert(engine.State, Equals, types.InstanceStateRunning)
			// c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			// c.Assert(engine.Endpoint, Equals, endpoint)

			// // The newly rebuilt replica should contain correct data
			// cksumAfterRebuilding1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfterRebuilding1, Equals, cksumBefore1)
			// cksumAfterRebuilding2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfterRebuilding2, Equals, cksumBefore2)
		}()
	}

	wg.Wait()

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	for _, engine := range engineList {
		err = spdkCli.EngineDelete(engine.Name)
		c.Assert(err, IsNil)
	}
	replicaList, err := spdkCli.ReplicaList()
	c.Assert(err, IsNil)
	for _, replica := range replicaList {
		err = spdkCli.ReplicaDelete(replica.Name, true)
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestSPDKMultipleThreadSnapshot(c *C) {
	fmt.Println("Testing SPDK snapshot operations with multiple threads")
	diskDriverName := "aio"

	ip, err := commonNet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonNet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	concurrentCount := 10
	dataCountInMB := int64(10)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-engine", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		//replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				// TODO: Check why there is a race here
				// err = spdkCli.EngineDelete(engineName)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName1, true)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName2, true)
				// c.Assert(err, IsNil)
				// err = spdkCli.ReplicaDelete(replicaName3, true)
				// c.Assert(err, IsNil)

				wg.Done()
			}()

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
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

			offsetInMB := int64(0)
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore11, Not(Equals), "")
			snapshotName11 := "snap11"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName11)
			c.Assert(err, IsNil)

			offsetInMB = dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore12, Not(Equals), "")
			snapshotName12 := "snap12"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12)
			c.Assert(err, IsNil)

			offsetInMB = 2 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore13, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore13, Not(Equals), "")
			snapshotName13 := "snap13"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName13)
			c.Assert(err, IsNil)

			offsetInMB = 3 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore14, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore14, Not(Equals), "")
			snapshotName14 := "snap14"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName14)
			c.Assert(err, IsNil)

			offsetInMB = 4 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore15, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore15, Not(Equals), "")
			snapshotName15 := "snap15"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName15)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50] -> head[50,60]

			// Write some extra data into the current head before reverting. This part of data will be discarded after revert
			offsetInMB = 5 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			//cksumBefore16, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			//c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14},
					snapshotName14: {snapshotName15},
					snapshotName15: {types.VolumeHead},
				},
				nil)

			// Revert for a new chain (chain 2)
			revertSnapshot(c, spdkCli, snapshotName13, volumeName, engineName, replicaAddressMap)

			// Only the data of snap11, snap12, and snap13 keeps intact after the snapshot deletion and volume re-attachment
			offsetInMB = 0
			cksumAfter11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter12, Equals, cksumBefore12)
			offsetInMB = 2 * dataCountInMB
			cksumAfter13, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter13, Equals, cksumBefore13)
			// The data of snap14 is no longer there after reverting to snap13
			offsetInMB = 3 * dataCountInMB
			cksumAfter14, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter14, Not(Equals), cksumBefore14)

			offsetInMB = 3 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore21, Not(Equals), "")
			snapshotName21 := "snap21"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName21)
			c.Assert(err, IsNil)

			offsetInMB = 4 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore22, Not(Equals), "")
			snapshotName22 := "snap22"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName22)
			c.Assert(err, IsNil)

			offsetInMB = 5 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore23, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore23, Not(Equals), "")
			snapshotName23 := "snap23"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName23)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50]
			// 	                                                                       \
			// 	                                                                        -> snap21[30,40] -> snap22[40,50] -> snap23[50,60] -> head[60,60]

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14, snapshotName21},
					snapshotName14: {snapshotName15},
					snapshotName15: {},
					snapshotName21: {snapshotName22},
					snapshotName22: {snapshotName23},
					snapshotName23: {types.VolumeHead},
				},
				nil)

			// Delete some snapshots
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName21)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName22)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName23)
			c.Assert(strings.Contains(err.Error(), "since it is the parent of volume head"), Equals, true)

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName12)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName14)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName13)
			c.Assert(strings.Contains(err.Error(), "since it contains multiple children"), Equals, true)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                                      \
			// 	                                                       -> snap23[30,60] -> head[60,60]

			// Verify the data for the current snapshot chain (chain 2)
			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter12, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter12, Equals, cksumBefore12)
			offsetInMB = 2 * dataCountInMB
			cksumAfter13, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter13, Equals, cksumBefore13)

			offsetInMB = 3 * dataCountInMB
			cksumAfter21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter21, Equals, cksumBefore21)
			offsetInMB = 4 * dataCountInMB
			cksumAfter22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter22, Equals, cksumBefore22)
			offsetInMB = 5 * dataCountInMB
			cksumAfter23, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter23, Equals, cksumBefore23)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {types.VolumeHead},
				},
				nil)

			// TODO: Add replica rebuilding related test step

			// Revert for a new chain (chain 3)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, replicaAddressMap)

			// Create and delete some snapshots for the new chain (chain 3)
			offsetInMB = dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore31, Not(Equals), "")
			snapshotName31 := "snap31"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName31)
			c.Assert(err, IsNil)

			offsetInMB = 2 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore32, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore32, Not(Equals), "")
			snapshotName32 := "snap32"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName32)
			c.Assert(err, IsNil)

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName31)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                     \                \
			// 	                                      \                -> snap23[30,60]
			// 	                                       \
			// 	                                        -> snap32[10,30] -> head[30,30]

			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)

			offsetInMB = dataCountInMB
			cksumAfter31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter31, Equals, cksumBefore31)
			offsetInMB = 2 * dataCountInMB
			cksumAfter32, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter32, Equals, cksumBefore32)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName32: {types.VolumeHead},
				},
				nil)

			// Revert for a new chain (chain 4)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, replicaAddressMap)

			// Create some snapshots for the new chain (chain 4)
			offsetInMB = dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore41, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore41, Not(Equals), "")
			snapshotName41 := "snap41"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName41)
			c.Assert(err, IsNil)
			offsetInMB = 2 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore42, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore42, Not(Equals), "")
			snapshotName42 := "snap42"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName42)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30] -> head[30,30]

			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName32: {},
					snapshotName41: {snapshotName42},
					snapshotName42: {types.VolumeHead},
				},
				nil)

			// // Before testing offline rebuilding
			// // Crash replica2 and remove it from the engine
			// err = spdkCli.EngineDelete(engineName)
			// c.Assert(err, IsNil)
			// engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendEmpty, defaultTestLvolSize, replicaAddressMap, 1)
			// c.Assert(err, IsNil)
			// c.Assert(engine.State, Equals, types.InstanceStateRunning)
			// c.Assert(engine.Frontend, Equals, types.FrontendEmpty)
			// c.Assert(engine.Endpoint, Equals, "")

			// delete(replicaAddressMap, replicaName2)
			// err = spdkCli.ReplicaDelete(replicaName2, true)
			// c.Assert(err, IsNil)
			// err = spdkCli.EngineReplicaDelete(engineName, replicaName2, net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))))
			// c.Assert(err, IsNil)
			// engine, err = spdkCli.EngineGet(engineName)
			// c.Assert(err, IsNil)
			// c.Assert(engine.State, Equals, types.InstanceStateRunning)
			// c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			// c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW})

			// // Start testing offline rebuilding
			// // Launch a new replica then ask the engine to rebuild it
			// replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, true, defaultTestReplicaPortCount)
			// c.Assert(err, IsNil)
			// c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			// c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			// c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			// c.Assert(replica3.PortStart, Not(Equals), int32(0))

			// err = spdkCli.EngineReplicaAdd(engineName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))))
			// c.Assert(err, IsNil)

			// // Verify the rebuilding result
			// replicaAddressMap = map[string]string{
			// 	replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
			// 	replica3.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))),
			// }
			// engine, err = spdkCli.EngineGet(engineName)
			// c.Assert(err, IsNil)
			// c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)

			// // Restart the engine with the newly rebuilt replica
			// delete(replicaAddressMap, replicaName3)
			// err = spdkCli.EngineDelete(engineName)
			// c.Assert(err, IsNil)
			// engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1)
			// c.Assert(err, IsNil)
			// c.Assert(engine.State, Equals, types.InstanceStateRunning)
			// c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			// c.Assert(engine.Endpoint, Equals, endpoint)

			// // The newly rebuilt replica should contain correct/unchanged data
			// // Verify chain1
			// revertSnapshot(c, spdkCli, snapshotName15, volumeName, engineName, replicaAddressMap)
			// offsetInMB = 0
			// cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter11, Equals, cksumBefore11)
			// offsetInMB = dataCountInMB
			// cksumAfter12, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter12, Equals, cksumBefore12)
			// offsetInMB = 2 * dataCountInMB
			// cksumAfter13, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter13, Equals, cksumBefore13)
			// offsetInMB = 3 * dataCountInMB
			// cksumAfter14, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter14, Equals, cksumBefore14)
			// offsetInMB = 4 * dataCountInMB
			// cksumAfter15, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter15, Equals, cksumBefore15)
			// // Notice that the head before the first revert is discarded
			// offsetInMB = 5 * dataCountInMB
			// cksumAfter16, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter16, Not(Equals), cksumBefore16)
			// // Verify chain2
			// revertSnapshot(c, spdkCli, snapshotName23, volumeName, engineName, replicaAddressMap)
			// offsetInMB = 0
			// cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter11, Equals, cksumBefore11)
			// offsetInMB = dataCountInMB
			// cksumAfter12, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter12, Equals, cksumBefore12)
			// offsetInMB = 2 * dataCountInMB
			// cksumAfter13, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter13, Equals, cksumBefore13)
			// offsetInMB = 3 * dataCountInMB
			// cksumAfter21, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter21, Equals, cksumBefore21)
			// offsetInMB = 4 * dataCountInMB
			// cksumAfter22, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter22, Equals, cksumBefore22)
			// offsetInMB = 5 * dataCountInMB
			// cksumAfter23, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter23, Equals, cksumBefore23)
			// // Verify chain3
			// revertSnapshot(c, spdkCli, snapshotName32, volumeName, engineName, replicaAddressMap)
			// offsetInMB = 0
			// cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter11, Equals, cksumBefore11)
			// offsetInMB = dataCountInMB
			// cksumAfter31, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter31, Equals, cksumBefore31)
			// offsetInMB = 2 * dataCountInMB
			// cksumAfter32, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter32, Equals, cksumBefore32)
			// // Verify chain4
			// revertSnapshot(c, spdkCli, snapshotName42, volumeName, engineName, replicaAddressMap)
			// offsetInMB = 0
			// cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter11, Equals, cksumBefore11)
			// offsetInMB = dataCountInMB
			// cksumAfter41, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter41, Equals, cksumBefore41)
			// offsetInMB = 2 * dataCountInMB
			// cksumAfter42, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			// c.Assert(err, IsNil)
			// c.Assert(cksumAfter42, Equals, cksumBefore42)

			// // Rebuilding would lead to a snapshot creation (with random name)
			// // Current snapshot tree (with backing image):
			// // 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50] -> rebuilding-snap1[50,50] -> head[50,50]
			// // 	                                    |\                \
			// // 	                                    | \                -> snap23[30,60]
			// // 	                                    |  \
			// // 	                                    \   -> snap32[10,30]
			// // 	                                     \
			// // 	                                      -> snap41[10,20] -> snap42[20,30]
			// snapshotMap := map[string][]string{
			// 	snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
			// 	snapshotName13: {snapshotName15, snapshotName23},
			// 	snapshotName15: {},
			// 	snapshotName23: {},
			// 	snapshotName32: {},
			// 	snapshotName41: {snapshotName42},
			// }
			// for replicaName := range replicaAddressMap {
			// 	replica, err := spdkCli.ReplicaGet(replicaName)
			// 	c.Assert(err, IsNil)
			// 	for snapName, snapLvol := range replica.Snapshots {
			// 		if strings.HasPrefix(snapName, fmt.Sprintf("%s-%s-", replicaName, server.RebuildingSnapshotNamePrefix)) {
			// 			c.Assert(snapLvol.Children[types.VolumeHead], Equals, true)
			// 			c.Assert(snapLvol.Parent, Equals, snapshotName15)
			// 			continue
			// 		}
			// 		for _, childSnapName := range snapshotMap[snapName] {
			// 			c.Assert(snapLvol.Children[childSnapName], Equals, true)
			// 		}
			// 	}
			// }
		}()
	}

	wg.Wait()

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	for _, engine := range engineList {
		err = spdkCli.EngineDelete(engine.Name)
		c.Assert(err, IsNil)
	}
	replicaList, err := spdkCli.ReplicaList()
	c.Assert(err, IsNil)
	for _, replica := range replicaList {
		err = spdkCli.ReplicaDelete(replica.Name, true)
		c.Assert(err, IsNil)
	}
}

func checkReplicaSnapshots(c *C, spdkCli *client.SPDKClient, engineName string, replicaList []string, snapshotMap map[string][]string, snapshotOpts map[string]api.SnapshotOptions) {
	engine, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)

	for _, replicaName := range replicaList {
		replica, err := spdkCli.ReplicaGet(replicaName)
		c.Assert(err, IsNil)
		c.Assert(len(replica.Snapshots), Equals, len(snapshotMap))

		for snapName, childrenList := range snapshotMap {
			snap := replica.Snapshots[snapName]
			c.Assert(snap, NotNil)
			c.Assert(engine.Snapshots[snapName], NotNil)
			c.Assert(engine.Snapshots[snapName].Children, DeepEquals, snap.Children)

			for _, childSnapName := range childrenList {
				c.Assert(snap.Children[childSnapName], Equals, true)
				if childSnapName != types.VolumeHead {
					childSnap := replica.Snapshots[childSnapName]
					c.Assert(childSnap, NotNil)
					c.Assert(childSnap.Parent, Equals, snapName)
				}
			}
		}

		for snapName, opts := range snapshotOpts {
			snap := replica.Snapshots[snapName]
			c.Assert(snap, NotNil)
			c.Assert(engine.Snapshots[snapName].UserCreated, Equals, opts.UserCreated)
		}

	}
}

func revertSnapshot(c *C, spdkCli *client.SPDKClient, snapshotName, volumeName, engineName string, replicaAddressMap map[string]string) {
	engine, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)

	if engine.State != types.InstanceStateRunning {
		return
	}

	prevFrontend := engine.Frontend
	prevEndpoint := engine.Endpoint
	if prevFrontend != types.FrontendEmpty {
		// Restart the engine without the frontend
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)
		engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendEmpty, defaultTestLvolSize, replicaAddressMap, 1)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
		c.Assert(engine.Port, Equals, int32(0))
		c.Assert(engine.Endpoint, Equals, "")
	}

	err = spdkCli.EngineSnapshotRevert(engineName, snapshotName)
	c.Assert(err, IsNil)

	if prevFrontend != types.FrontendEmpty {
		// Restart the engine with the previous frontend
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)
		engine, err = spdkCli.EngineCreate(engineName, volumeName, prevFrontend, defaultTestLvolSize, replicaAddressMap, 1)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
		c.Assert(engine.Port, Not(Equals), int32(0))
		c.Assert(engine.Endpoint, Equals, prevEndpoint)
	}
}
