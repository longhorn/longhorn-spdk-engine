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

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	commontypes "github.com/longhorn/go-common-libs/types"
	helperclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	server "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"

	. "gopkg.in/check.v1"
)

var (
	defaultTestDiskName = "test-disk"
	defaultTestDiskPath = filepath.Join("/tmp", defaultTestDiskName)

	defaultTestBlockSize          = 4096
	defaultTestDiskSize           = uint64(20480 * helpertypes.MiB)
	defaultTestLvolSizeInMiB      = uint64(500)
	defaultTestLvolSize           = defaultTestLvolSizeInMiB * helpertypes.MiB
	defaultTestLargeLvolSizeInMiB = uint64(2000)
	defaultTestLargeLvolSize      = defaultTestLargeLvolSizeInMiB * helpertypes.MiB

	defaultTestBackingImageName        = "parrot"
	defaultTestBackingImageUUID        = "12345"
	defaultTestBackingImageChecksum    = "304f3ed30ca6878e9056ee6f1b02b328239f0d0c2c1272840998212f9734b196371560b3b939037e4f4c2884ce457c2cbc9f0621f4f5d1ca983983c8cdf8cd9a"
	defaultTestBackingImageDownloadURL = "https://longhorn-backing-image.s3-us-west-1.amazonaws.com/parrot.raw"
	defaultTestBackingImageSizeInMiB   = uint64(32)
	defaultTestBackingImageSize        = defaultTestBackingImageSizeInMiB * helpertypes.MiB

	defaultTestStartPort        = int32(20000)
	defaultTestEndPort          = int32(30000)
	defaultTestReplicaPortCount = int32(5)

	defaultTestExecuteTimeout = 10 * time.Second

	defaultTestRebuildingWaitInterval   = 3 * time.Second
	defaultTestRebuildingWaitCount      = 60
	defaultTestSnapChecksumWaitInterval = 1 * time.Second
	defaultTestSnapChecksumWaitCount    = 60

	maxBackingImageGetRetries = 300
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

func startTarget(spdkDir string, args []string, execute func(envs []string, binary string, args []string, timeout time.Duration) (string, error)) (err error) {
	if spdkCli, err := helperclient.NewClient(context.Background()); err == nil {
		if _, err := spdkCli.BdevGetBdevs("", 0); err == nil {
			logrus.Info("Detected running spdk_tgt, skipped the target starting")
			return nil
		}
	}

	argsInStr := ""
	for _, arg := range args {
		argsInStr = fmt.Sprintf("%s %s", argsInStr, arg)
	}
	tgtOpts := []string{
		"-c",
		fmt.Sprintf("%s %s", filepath.Join(spdkDir, "build/bin/spdk_tgt"), argsInStr),
	}

	_, err = execute(nil, "sh", tgtOpts, 1*time.Minute)
	return err
}

func LaunchTestSPDKTarget(c *C, execute func(envs []string, name string, args []string, timeout time.Duration) (string, error)) {
	targetReady := false
	if spdkCli, err := helperclient.NewClient(context.Background()); err == nil {
		if _, err := spdkCli.BdevGetBdevs("", 0); err == nil {
			targetReady = true
		}
	}

	if !targetReady {
		go func() {
			err := startTarget(GetSPDKDir(), []string{"--logflag all", "2>&1 | tee /tmp/spdk_tgt.log"}, execute)
			c.Assert(err, IsNil)
		}()

		for cnt := 0; cnt < 300; cnt++ {
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

func LaunchTestSPDKGRPCServer(ctx context.Context, c *C, ip string, execute func(envs []string, name string, args []string, timeout time.Duration) (string, error)) {
	LaunchTestSPDKTarget(c, execute)
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
		logrus.Info("Stopping SPDK gRPC server")
		// Stop the SPDK tgt daemon
		// TODO: The error "no such child process" will be emitted when the process is already stopped. Need to improve the error handling, but we can ignore it for now.
		_ = util.ForceStopSPDKTgtDaemon(5 * time.Second)
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

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
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

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	_, err = ne.Execute(nil, "losetup", []string{"-d", loopDevicePath}, time.Second)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestSPDKMultipleThread(c *C) {
	fmt.Println("Testing SPDK basic operations with multiple threads")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonnet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer spdkCli.Close()

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
		spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
		c.Assert(err, IsNil)
		defer spdkCli.Close()

		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-engine", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				err = spdkCli.EngineDelete(engineName)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName1, true)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName2, true)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName3, true)
				c.Assert(err, IsNil)

				wg.Done()
			}()

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			c.Assert(replica1.Head, NotNil)
			c.Assert(replica1.Head.CreationTime, Not(Equals), "")
			c.Assert(replica1.Head.Parent, Equals, "")
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
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
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, ip, ip, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
			c.Assert(engine.Endpoint, Equals, endpoint)
			// initiator and target are on the same node
			c.Assert(engine.IP, Not(Equals), "")
			c.Assert(engine.TargetIP, Not(Equals), "")
			c.Assert(engine.IP, Equals, engine.TargetIP)
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.TargetPort, Not(Equals), int32(0))
			c.Assert(engine.Port, Equals, engine.TargetPort)

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

			replica1, err = spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			replica2, err = spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)

			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
			}
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, ip, ip, false)
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

			// Before testing online rebuilding
			// Crash replica2 and remove it from the engine
			delete(replicaAddressMap, replicaName2)
			err = spdkCli.ReplicaDelete(replicaName2, true)
			c.Assert(err, IsNil)
			err = spdkCli.EngineReplicaDelete(engineName, replicaName2, net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))))
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW})

			// Start testing online rebuilding
			// Launch a new replica then ask the engine to rebuild it
			replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			c.Assert(replica3.PortStart, Not(Equals), int32(0))
			c.Assert(replica3.Head, NotNil)
			c.Assert(replica3.Head.CreationTime, Not(Equals), "")
			c.Assert(replica3.Head.Parent, Equals, "")

			err = spdkCli.EngineReplicaAdd(engineName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))))
			c.Assert(err, IsNil)

			WaitForReplicaRebuildingComplete(c, spdkCli, engineName, replicaName3)

			// Verify the rebuilding result
			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica3.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))),
			}
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW, replicaName3: types.ModeRW})

			// The newly rebuilt replica should contain correct data
			cksumAfterRebuilding1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding1, Equals, cksumBefore1)
			cksumAfterRebuilding2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding2, Equals, cksumBefore2)
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

func (s *TestSuite) TestSPDKMultipleThreadSnapshotOpsAndRebuilding(c *C) {
	fmt.Println("Testing SPDK snapshot operations with multiple threads")
	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonnet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer spdkCli.Close()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	bi, err := spdkCli.BackingImageCreate(defaultTestBackingImageName, defaultTestBackingImageUUID, disk.Uuid, defaultTestBackingImageSize, defaultTestBackingImageChecksum, defaultTestBackingImageDownloadURL, "")
	c.Assert(err, IsNil)
	c.Assert(bi, NotNil)
	defer func() {
		err := spdkCli.BackingImageDelete(defaultTestBackingImageName, disk.Uuid)
		c.Assert(err, IsNil)
	}()

	// check if bi.State is "ready" in 300 seconds
	for i := 0; i < maxBackingImageGetRetries; i++ {
		bi, err = spdkCli.BackingImageGet(defaultTestBackingImageName, disk.Uuid)
		c.Assert(err, IsNil)

		if bi.State == string(types.BackingImageStateReady) {
			break
		}

		time.Sleep(1 * time.Second)

		if i == maxBackingImageGetRetries-1 {
			c.Assert(bi.State, Equals, string(types.BackingImageStateReady))
		}
	}

	concurrentCount := 10
	dataCountInMB := int64(10)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
		c.Assert(err, IsNil)
		defer spdkCli.Close()

		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-engine", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)
		replicaName4 := fmt.Sprintf("%s-replica-4", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				err = spdkCli.EngineDelete(engineName)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName1, true)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName2, true)
				c.Assert(err, IsNil)
				err = spdkCli.ReplicaDelete(replicaName3, true)
				c.Assert(err, IsNil)

				wg.Done()
			}()

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)
			c.Assert(replica2.PortStart, Not(Equals), int32(0))

			_, err = spdkCli.ReplicaGet(replicaName1)
			c.Assert(err, IsNil)
			_, err = spdkCli.ReplicaGet(replicaName2)
			c.Assert(err, IsNil)

			replicaAddressMap := map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
			}
			replicaModeMap := map[string]types.Mode{
				replica1.Name: types.ModeRW,
				replica2.Name: types.ModeRW,
			}
			endpoint := helperutil.GetLonghornDevicePath(volumeName)
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, ip, ip, false)
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
			cksumBefore16, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)

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

			// Revert back to chain 1
			revertSnapshot(c, spdkCli, snapshotName15, volumeName, engineName, replicaAddressMap)

			// Test online rebuilding twice

			// Write some data to the head before the 1st rebuilding
			offsetInMB = 6 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBeforeRebuild11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBeforeRebuild11, Not(Equals), "")

			// Crash replica1
			delete(replicaAddressMap, replicaName1)
			err = spdkCli.ReplicaDelete(replicaName1, true)
			c.Assert(err, IsNil)
			err = spdkCli.EngineReplicaDelete(engineName, replicaName1, net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))))
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW})
			// Launch the 1st rebuilding replica as the replacement of the crashed replica1
			replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			c.Assert(replica3.PortStart, Not(Equals), int32(0))
			// Start the 1st rebuilding and wait for the completion
			err = spdkCli.EngineReplicaAdd(engineName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))))
			c.Assert(err, IsNil)
			WaitForReplicaRebuildingComplete(c, spdkCli, engineName, replicaName3)
			// While the volume head data written before rebuilding remains
			offsetInMB = 6 * dataCountInMB
			cksumAfterRebuild11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuild11, Equals, cksumBeforeRebuild11)
			// Figure out the 1st rebuilding snapshot name
			replicaAddressMap[replica3.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart)))
			snapshotNameRebuild1 := ""
			for replicaName := range replicaAddressMap {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				for snapName, snapLvol := range replica.Snapshots {
					if strings.HasPrefix(snapName, server.RebuildingSnapshotNamePrefix) {
						c.Assert(snapLvol.Children[types.VolumeHead], Equals, true)
						c.Assert(snapLvol.Parent, Equals, snapshotName15)
						if snapshotNameRebuild1 == "" {
							snapshotNameRebuild1 = snapName
						} else {
							c.Assert(snapName, Equals, snapshotNameRebuild1)
						}
						break
					}
				}
			}
			c.Assert(snapshotNameRebuild1, Not(Equals), "")

			// Verify the 1st rebuilding result
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW, replicaName3: types.ModeRW})

			// Rebuilding once leads to 1 snapshot creations (with random name)
			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50] -> rebuild11[60,70] -> head[70,70]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]

			snapshotMap := map[string][]string{
				snapshotName11:       {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13:       {snapshotName15, snapshotName23},
				snapshotName15:       {snapshotNameRebuild1},
				snapshotNameRebuild1: {types.VolumeHead},
				snapshotName23:       {},
				snapshotName32:       {},
				snapshotName41:       {snapshotName42},
				snapshotName42:       {},
			}
			snapshotOpts := map[string]api.SnapshotOptions{}
			for snapName := range snapshotMap {
				snapshotOpts[snapName] = api.SnapshotOptions{UserCreated: true}
			}
			snapshotOpts[snapshotNameRebuild1] = api.SnapshotOptions{UserCreated: false}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName2, replicaName3}, snapshotMap, snapshotOpts)

			// Write more data to the head before the 2nd rebuilding
			offsetInMB = 7 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBeforeRebuild12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBeforeRebuild12, Not(Equals), "")

			// Crash replica2
			delete(replicaAddressMap, replicaName2)
			err = spdkCli.ReplicaDelete(replicaName2, true)
			c.Assert(err, IsNil)
			err = spdkCli.EngineReplicaDelete(engineName, replicaName2, net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))))
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName3: types.ModeRW})
			// Launch the 2nd rebuilding replica as the replacement of the crashed replica2
			replica4, err := spdkCli.ReplicaCreate(replicaName4, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica4.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica4.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica4.State, Equals, types.InstanceStateRunning)
			c.Assert(replica4.PortStart, Not(Equals), int32(0))
			// Start the 2nd rebuilding and wait for the completion
			err = spdkCli.EngineReplicaAdd(engineName, replicaName4, net.JoinHostPort(ip, strconv.Itoa(int(replica4.PortStart))))
			c.Assert(err, IsNil)
			WaitForReplicaRebuildingComplete(c, spdkCli, engineName, replicaName4)
			// While the volume head data written before rebuilding remains
			offsetInMB = 6 * dataCountInMB
			cksumAfterRebuild11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuild11, Equals, cksumBeforeRebuild11)
			offsetInMB = 7 * dataCountInMB
			cksumAfterRebuild12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuild12, Equals, cksumBeforeRebuild12)
			// Figure out the 2nd rebuilding snapshot name
			replicaAddressMap[replica4.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica4.PortStart)))
			snapshotNameRebuild2 := ""
			for replicaName := range replicaAddressMap {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				for snapName, snapLvol := range replica.Snapshots {
					if snapName != snapshotNameRebuild1 && strings.HasPrefix(snapName, server.RebuildingSnapshotNamePrefix) {
						c.Assert(snapLvol.Children[types.VolumeHead], Equals, true)
						c.Assert(snapLvol.Parent, Equals, snapshotNameRebuild1)
						if snapshotNameRebuild2 == "" {
							snapshotNameRebuild2 = snapName
						} else {
							c.Assert(snapName, Equals, snapshotNameRebuild2)
						}
						break
					}
				}
			}
			c.Assert(snapshotNameRebuild2, Not(Equals), "")

			// Verify the 2nd rebuilding result
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName3: types.ModeRW, replicaName4: types.ModeRW})

			// Rebuilding twice leads to 2 snapshot creations (with random name)
			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50] -> rebuild11[60,70] -> rebuild12[70,80] -> head[80,80]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]
			snapshotMap = map[string][]string{
				snapshotName11:       {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13:       {snapshotName15, snapshotName23},
				snapshotName15:       {snapshotNameRebuild1},
				snapshotNameRebuild1: {snapshotNameRebuild2},
				snapshotNameRebuild2: {types.VolumeHead},
				snapshotName23:       {},
				snapshotName32:       {},
				snapshotName41:       {snapshotName42},
				snapshotName42:       {},
			}
			snapshotOpts[snapshotNameRebuild2] = api.SnapshotOptions{UserCreated: false}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts)

			// Purging snapshots would lead to rebuild11 cleanup
			err = spdkCli.EngineSnapshotPurge(engineName)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50] -> rebuild12[60,80] -> head[80,80]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]
			snapshotMap = map[string][]string{
				snapshotName11:       {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13:       {snapshotName15, snapshotName23},
				snapshotName15:       {snapshotNameRebuild2},
				snapshotNameRebuild2: {types.VolumeHead},
				snapshotName23:       {},
				snapshotName32:       {},
				snapshotName41:       {snapshotName42},
				snapshotName42:       {},
			}
			delete(snapshotOpts, snapshotNameRebuild1)
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts)
			for _, replicaName := range []string{replicaName3, replicaName4} {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				c.Assert(replica.Head.Parent, Equals, snapshotNameRebuild2)
				c.Assert(replica.Head.ActualSize, Equals, uint64(0))
			}

			// The newly rebuilt replicas should contain correct/unchanged data
			// Verify chain1
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
			cksumAfter14, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter14, Equals, cksumBefore14)
			offsetInMB = 4 * dataCountInMB
			cksumAfter15, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter15, Equals, cksumBefore15)
			// Notice that the head before the first revert is discarded
			offsetInMB = 5 * dataCountInMB
			cksumAfter16, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter16, Not(Equals), cksumBefore16)
			// While the volume head data after rebuilding and purge still remains
			offsetInMB = 6 * dataCountInMB
			cksumAfterRebuild11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuild11, Equals, cksumBeforeRebuild11)
			offsetInMB = 7 * dataCountInMB
			cksumAfterRebuild12, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuild12, Equals, cksumBeforeRebuild12)
			// Verify chain2
			revertSnapshot(c, spdkCli, snapshotName23, volumeName, engineName, replicaAddressMap)
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
			cksumAfter21, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter21, Equals, cksumBefore21)
			offsetInMB = 4 * dataCountInMB
			cksumAfter22, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter22, Equals, cksumBefore22)
			offsetInMB = 5 * dataCountInMB
			cksumAfter23, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter23, Equals, cksumBefore23)
			// Verify chain3
			revertSnapshot(c, spdkCli, snapshotName32, volumeName, engineName, replicaAddressMap)
			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter31, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter31, Equals, cksumBefore31)
			offsetInMB = 2 * dataCountInMB
			cksumAfter32, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter32, Equals, cksumBefore32)
			// Verify chain4
			revertSnapshot(c, spdkCli, snapshotName42, volumeName, engineName, replicaAddressMap)
			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter41, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter41, Equals, cksumBefore41)
			offsetInMB = 2 * dataCountInMB
			cksumAfter42, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter42, Equals, cksumBefore42)
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

func (s *TestSuite) TestSPDKMultipleThreadFastRebuilding(c *C) {
	fmt.Println("Testing SPDK fast rebuilding with multiple threads")
	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonnet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute)

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

	bi, err := spdkCli.BackingImageCreate(defaultTestBackingImageName, defaultTestBackingImageUUID, disk.Uuid, defaultTestBackingImageSize, defaultTestBackingImageChecksum, defaultTestBackingImageDownloadURL, "")
	c.Assert(err, IsNil)
	c.Assert(bi, NotNil)
	defer func() {
		err := spdkCli.BackingImageDelete(defaultTestBackingImageName, disk.Uuid)
		c.Assert(err, IsNil)
	}()

	// check if bi.State is "ready" in 300 seconds
	for i := 0; i < maxBackingImageGetRetries; i++ {
		bi, err = spdkCli.BackingImageGet(defaultTestBackingImageName, disk.Uuid)
		c.Assert(err, IsNil)

		if bi.State == string(types.BackingImageStateReady) {
			break
		}

		time.Sleep(1 * time.Second)

		if i == maxBackingImageGetRetries-1 {
			c.Assert(bi.State, Equals, string(types.BackingImageStateReady))
		}
	}

	concurrentCount := 10
	dataCountInMB := int64(100)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-engine", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)

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

			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, bi.Name)
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
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, replicaAddressMap, 1, ip, ip, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Endpoint, Equals, endpoint)

			// Construct a snapshot tree with enough data before testing rebuilding

			// Build the first chain (chain 1)
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

			// Revert for a new chain (chain 2)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, replicaAddressMap)
			offsetInMB = 1 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore21, Not(Equals), "")
			snapshotName21 := "snap21"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName21)
			c.Assert(err, IsNil)

			offsetInMB = 2 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore22, Not(Equals), "")
			snapshotName22 := "snap22"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName22)
			c.Assert(err, IsNil)

			// Revert for a new chain (chain 3)
			revertSnapshot(c, spdkCli, snapshotName21, volumeName, engineName, replicaAddressMap)

			offsetInMB = 2 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBefore31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore31, Not(Equals), "")
			snapshotName31 := "snap31"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName31)
			c.Assert(err, IsNil)

			// Finally, write some data to the head before the rebuilding
			offsetInMB = 3 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			cksumBeforeRebuild11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBeforeRebuild11, Not(Equals), "")

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] -> snap13[2*DATA,3*DATA]
			// 	                                      \
			// 	                                       -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			// 	                                                          \
			// 	                                                           -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {types.VolumeHead},
				},
				nil)

			waitReplicaSnapshotChecksum(c, spdkCli, replicaName1, "")
			waitReplicaSnapshotChecksum(c, spdkCli, replicaName2, "")

			// Test online rebuilding

			// Crash replica1
			err = spdkCli.ReplicaDelete(replicaName1, false)
			c.Assert(err, IsNil)
			// TODO: Make replica1 Mode ERR
			//engine, err = spdkCli.EngineGet(engineName)
			//c.Assert(err, IsNil)
			//c.Assert(engine.State, Equals, types.InstanceStateRunning)
			//c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			//c.Assert(engine.Endpoint, Equals, endpoint)
			//c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			//c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeERR, replicaName2: types.ModeRW})

			delete(replicaAddressMap, replicaName1)
			err = spdkCli.EngineReplicaDelete(engineName, replicaName1, net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))))
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW})

			replica1, err = spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, bi.Name)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))

			// Before reusing the crashed replica1 for rebuilding, mess up some snapshots and see if the fast rebuilding mechanism can handle it
			replicaTmpAddressMap := map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
			}
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, replicaTmpAddressMap, 1, ip, ip, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaTmpAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replica1.Name: types.ModeRW})
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Endpoint, Equals, endpoint)

			// Mess up snapshots in chain 1 by re-creating snap12 with other random data
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, replicaTmpAddressMap)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName13)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName12)
			c.Assert(err, IsNil)
			// Write 2 * dataCountInMB data to the invalid snap12
			offsetInMB = 1 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", 2*dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12)
			c.Assert(err, IsNil)
			// Then create an extra snapshot and leave an invalid head
			offsetInMB = 3 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			snapshotName14 := "snap14"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName14)
			c.Assert(err, IsNil)
			offsetInMB = 4 * dataCountInMB
			_, err = ne.Execute(nil, "dd", []string{"if=/dev/urandom", fmt.Sprintf("of=%s", endpoint), "bs=1M", fmt.Sprintf("count=%d", dataCountInMB), fmt.Sprintf("seek=%d", offsetInMB), "status=none"}, defaultTestExecuteTimeout)
			c.Assert(err, IsNil)
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)

			// Relaunch engine with the correct replica
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, replicaAddressMap, 1, ip, ip, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replica2.Name: types.ModeRW})
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Endpoint, Equals, endpoint)

			// And start the 1st rebuilding and wait for the completion
			err = spdkCli.EngineReplicaAdd(engineName, replicaName1, net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))))
			c.Assert(err, IsNil)
			// The rebuilding should be pretty fast since all existing snapshots and the previous head are there
			WaitForReplicaRebuildingCompleteTimeout(c, spdkCli, engineName, replicaName1, 30)
			// Figure out the 1st rebuilding snapshot name
			replicaAddressMap[replica1.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart)))
			snapshotNameRebuild1 := ""
			for replicaName := range replicaAddressMap {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				for snapName, snapLvol := range replica.Snapshots {
					if strings.HasPrefix(snapName, server.RebuildingSnapshotNamePrefix) {
						c.Assert(snapLvol.Children[types.VolumeHead], Equals, true)
						c.Assert(snapLvol.Parent, Equals, snapshotName31)
						if snapshotNameRebuild1 == "" {
							snapshotNameRebuild1 = snapName
						} else {
							c.Assert(snapName, Equals, snapshotNameRebuild1)
						}
						break
					}
				}
			}
			c.Assert(snapshotNameRebuild1, Not(Equals), "")

			// Verify the 1st rebuilding result
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.Frontend, Equals, types.FrontendSPDKTCPBlockdev)
			c.Assert(engine.Endpoint, Equals, endpoint)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW, replicaName2: types.ModeRW})

			// Rebuilding once leads to 1 snapshot creations (with random name)
			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] (messed up in replica1, which has invalid data [1*DATA,3*DATA]) (-> replica1 snap14[3*DATA,4*DATA] -> replica 1 invalid head[4*DATA,5*DATA])
			// 	                                      \
			// 	                                       -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			// 	                                                          \
			// 	                                                           -> snap31[2*DATA,3*DATA] -> rebuild11[3*DATA,4*DATA] -> head[4*DATA,4*DATA]

			snapshotMap := map[string][]string{
				snapshotName11:       {snapshotName12, snapshotName21},
				snapshotName12:       {snapshotName13},
				snapshotName13:       {},
				snapshotName21:       {snapshotName22, snapshotName31},
				snapshotName22:       {},
				snapshotName31:       {snapshotNameRebuild1},
				snapshotNameRebuild1: {types.VolumeHead},
			}
			snapshotOpts := map[string]api.SnapshotOptions{}
			for snapName := range snapshotMap {
				snapshotOpts[snapName] = api.SnapshotOptions{UserCreated: true}
			}
			snapshotOpts[snapshotNameRebuild1] = api.SnapshotOptions{UserCreated: false}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2}, snapshotMap, snapshotOpts)

			// The newly rebuilt replicas should contain correct/unchanged data
			// Verify chain3
			offsetInMB = 0
			cksumAfter11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter21, Equals, cksumBefore21)
			offsetInMB = 2 * dataCountInMB
			cksumAfter31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter31, Equals, cksumBefore31)
			// Verify chain2
			revertSnapshot(c, spdkCli, snapshotName22, volumeName, engineName, replicaAddressMap)
			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter11, Equals, cksumBefore11)
			offsetInMB = dataCountInMB
			cksumAfter21, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter21, Equals, cksumBefore21)
			offsetInMB = 2 * dataCountInMB
			cksumAfter22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfter22, Equals, cksumBefore22)
			// Verify chain1
			revertSnapshot(c, spdkCli, snapshotName13, volumeName, engineName, replicaAddressMap)
			offsetInMB = 0
			cksumAfter11, err = util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
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

func waitReplicaSnapshotChecksum(c *C, spdkCli *client.SPDKClient, replicaName, targetSnapName string) {
	waitReplicaSnapshotChecksumTimeout(c, spdkCli, replicaName, targetSnapName, defaultTestSnapChecksumWaitCount)
}

func waitReplicaSnapshotChecksumTimeout(c *C, spdkCli *client.SPDKClient, replicaName, targetSnapName string, timeoutInSecond int) {
	ticker := time.NewTicker(defaultTestSnapChecksumWaitInterval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Duration(timeoutInSecond) * time.Second)
	defer timer.Stop()

	hasChecksum := true
	for {
		hasChecksum = true
		select {
		case <-timer.C:
			c.Assert(hasChecksum, Equals, true)
			return
		case <-ticker.C:
			replica, err := spdkCli.ReplicaGet(replicaName)
			c.Assert(err, IsNil)
			if targetSnapName == "" || replica.Snapshots[targetSnapName] != nil {
				for snapName, snap := range replica.Snapshots {
					if targetSnapName == "" || snapName == targetSnapName {
						if snap.SnapshotChecksum == "" {
							hasChecksum = false
							break
						}
					}
				}
			}
		}
		if hasChecksum {
			break
		}
	}

	c.Assert(hasChecksum, Equals, true)
}

func revertSnapshot(c *C, spdkCli *client.SPDKClient, snapshotName, volumeName, engineName string, replicaAddressMap map[string]string) {
	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonnet.EnvPodIP, ip)

	engine, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)

	if engine.State != types.InstanceStateRunning {
		return
	}
	volumeSize := engine.SpecSize

	prevFrontend := engine.Frontend
	prevEndpoint := engine.Endpoint
	if prevFrontend != types.FrontendEmpty {
		// Restart the engine without the frontend
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)
		engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendEmpty, volumeSize, replicaAddressMap, 1, ip, ip, false)
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
		engine, err = spdkCli.EngineCreate(engineName, volumeName, prevFrontend, volumeSize, replicaAddressMap, 1, ip, ip, false)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
		c.Assert(engine.Port, Not(Equals), int32(0))
		c.Assert(engine.Endpoint, Equals, prevEndpoint)
	}
}

func WaitForReplicaRebuildingComplete(c *C, spdkCli *client.SPDKClient, engineName, replicaName string) {
	WaitForReplicaRebuildingCompleteTimeout(c, spdkCli, engineName, replicaName, defaultTestRebuildingWaitCount)
}

func WaitForReplicaRebuildingCompleteTimeout(c *C, spdkCli *client.SPDKClient, engineName, replicaName string, timeoutInSecond int) {
	complete := false

	for cnt := 0; cnt < timeoutInSecond; cnt++ {
		rebuildingStatus, err := spdkCli.ReplicaRebuildingDstShallowCopyCheck(replicaName)
		c.Assert(err, IsNil)
		c.Assert(rebuildingStatus.Error, Equals, "")
		switch rebuildingStatus.State {
		case "":
			c.Assert(rebuildingStatus.SnapshotName, Equals, "")
			c.Assert(rebuildingStatus.TotalState, Equals, "")
			c.Assert(rebuildingStatus.Progress, Equals, uint32(0))
			c.Assert(rebuildingStatus.TotalProgress, Equals, uint32(0))
			c.Assert(rebuildingStatus.TotalState, Equals, "")
		case types.ProgressStateStarting:
			c.Assert(rebuildingStatus.SnapshotName, Equals, "")
			c.Assert(rebuildingStatus.TotalState, Equals, "")
			c.Assert(rebuildingStatus.Progress, Equals, uint32(0))
			c.Assert(rebuildingStatus.TotalProgress, Equals, uint32(0))
			c.Assert(rebuildingStatus.TotalState, Equals, types.ProgressStateInProgress)
		case types.ProgressStateInProgress:
			c.Assert(rebuildingStatus.SnapshotName, Not(Equals), "")
			c.Assert(rebuildingStatus.TotalState, Equals, types.ProgressStateInProgress)
			c.Assert(rebuildingStatus.Progress <= 100, Equals, true)
			c.Assert(rebuildingStatus.TotalProgress < 100, Equals, true)
		case types.ProgressStateComplete:
			c.Assert(rebuildingStatus.Progress, Equals, uint32(100))
			if rebuildingStatus.TotalState == types.ProgressStateInProgress {
				c.Assert(rebuildingStatus.TotalProgress <= 100, Equals, true)
			} else {
				c.Assert(rebuildingStatus.TotalState, Equals, types.ProgressStateComplete)
				c.Assert(rebuildingStatus.TotalProgress, Equals, uint32(100))
			}
		default:
			c.Fatalf("Unexpected rebuilding state %v", rebuildingStatus.State)
		}

		if rebuildingStatus.TotalState == types.ProgressStateComplete {
			engine, err := spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			if engine.ReplicaModeMap[replicaName] == types.ModeRW {
				complete = true
				break
			}
		}

		time.Sleep(defaultTestRebuildingWaitInterval)
	}

	c.Assert(complete, Equals, true)
}

func (s *TestSuite) TestSPDKEngineOnlyWithTarget(c *C) {
	fmt.Println("Testing SPDK basic operations with engine only with target")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	os.Setenv(commonnet.EnvPodIP, ip)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer spdkCli.Close()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	volumeName := "test-vol"
	engineName := fmt.Sprintf("%s-engine", volumeName)
	replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
	replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)

	replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)

	replicaAddressMap := map[string]string{
		replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
		replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
	}

	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, "127.0.0.1", ip, false)
	c.Assert(err, IsNil)

	c.Assert(engine.Endpoint, Equals, "")
	// Initiator is not created, so the IP and Port should be empty
	c.Assert(engine.IP, Equals, "")
	c.Assert(engine.Port, Equals, int32(0))
	// Target is created and exposed
	c.Assert(engine.TargetIP, Equals, ip)
	c.Assert(engine.TargetPort, Not(Equals), int32(0))

	// Detach and re-attach the volume
	// EngineDeleteTarget will delete engine instance if engine doesn't have an initiator
	err = spdkCli.EngineDeleteTarget(engineName)
	c.Assert(err, IsNil)

	_, err = spdkCli.EngineGet(engineName)
	c.Assert(strings.Contains(err.Error(), "cannot find engine"), Equals, true)

	err = spdkCli.ReplicaDelete(replicaName1, false)
	c.Assert(err, IsNil)
	err = spdkCli.ReplicaDelete(replicaName2, false)
	c.Assert(err, IsNil)
}
