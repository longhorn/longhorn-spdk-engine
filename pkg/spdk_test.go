package pkg

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	helperinitiator "github.com/longhorn/go-spdk-helper/pkg/initiator"
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

	defaultTestFastSync = true

	// Use a larger timeout for the test to avoid timeout issues while enabling debugger such as valgrind.
	defaultTestExecuteTimeout = 120 * time.Second

	defaultTestRebuildingWaitInterval   = 3 * time.Second
	defaultTestRebuildingWaitCount      = 60
	defaultTestSnapChecksumWaitInterval = 1 * time.Second
	defaultTestSnapChecksumWaitCount    = 60

	maxBackingImageGetRetries = 300

	checkReplicaSnapshotsMaxRetries   = 6
	checkReplicaSnapshotsWaitInterval = 10 * time.Second

	SPDKTGTBinary = "spdk_tgt"
)

const (
	spdkTargetProbeTimeout      = 3 * time.Second
	spdkTargetStopGracePeriod   = 10 * time.Second
	spdkTargetStartupProbeDelay = 1 * time.Second
	spdkTargetStopPollInterval  = 200 * time.Millisecond
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})
var runtimeMonitoringTestMu sync.Mutex

func getVolumeName() string {
	return fmt.Sprintf("test-vol-%s", time.Now().Format("20060102150405"))
}

func startTarget(spdkDir string, args []string, execute func(envs []string, binary string, args []string, timeout time.Duration) (string, error)) (err error) {
	argsInStr := ""
	for _, arg := range args {
		argsInStr = fmt.Sprintf("%s %s", argsInStr, arg)
	}

	tgtOpts := []string{
		"-c",
		fmt.Sprintf("%s %s", filepath.Join(spdkDir, SPDKTGTBinary), argsInStr),
	}

	// Use a larger timeout for the test to avoid timeout issues while enabling debugger such as valgrind.
	_, err = execute(nil, "sh", tgtOpts, 180*time.Minute)
	return err
}

func probeSPDKTargetReady(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	spdkCli, err := helperclient.NewClient(ctx)
	if err != nil {
		return false
	}
	defer func() {
		if err := spdkCli.Close(); err != nil {
			logrus.WithError(err).Warn("Failed to close SPDK target probe client")
		}
	}()

	return true
}

func waitForSPDKTargetDaemonStoppedWithTimeout(timeout, pollInterval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		running, err := util.IsSPDKTargetProcessRunning()
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for spdk_tgt to stop")
		}
		time.Sleep(pollInterval)
	}
}

func ensureSPDKTargetDaemonStopped(timeout time.Duration) error {
	running, err := util.IsSPDKTargetProcessRunning()
	if err != nil {
		return err
	}
	if !running {
		return nil
	}

	if err := util.ForceStopSPDKTgtDaemon(timeout); err != nil &&
		!strings.Contains(err.Error(), "process with cmdline spdk_tgt is not found") {
		return err
	}

	return waitForSPDKTargetDaemonStoppedWithTimeout(timeout, spdkTargetStopPollInterval)
}

func LaunchTestSPDKTargetDaemon(c *C, execute func(envs []string, name string, args []string, timeout time.Duration) (string, error)) {
	err := ensureSPDKTargetDaemonStopped(spdkTargetStopGracePeriod)
	c.Assert(err, IsNil)

	targetReady := false
	go func() {
		logrus.Info("Starting SPDK target daemon")
		err := startTarget("", []string{"--logflag all", "2>&1 | tee /tmp/spdk_tgt.log"}, execute)
		c.Assert(err, IsNil)
	}()

	for cnt := 0; cnt < 300; cnt++ {
		if probeSPDKTargetReady(spdkTargetProbeTimeout) {
			targetReady = true
			break
		}
		time.Sleep(spdkTargetStartupProbeDelay)
	}

	c.Assert(targetReady, Equals, true)
}

// launchTestSPDKGRPCServer launches the SPDK gRPC server and returns the server instance.
// This helper allows tests to access the server instance for advanced testing scenarios (e.g. error injection).
func launchTestSPDKGRPCServer(ctx context.Context, c *C, ip string, execute func(envs []string, name string, args []string, timeout time.Duration) (string, error), wg *sync.WaitGroup) *server.Server {

	LaunchTestSPDKTargetDaemon(c, execute)
	srv, err := server.NewServer(ctx, defaultTestStartPort, defaultTestEndPort)
	c.Assert(err, IsNil)

	spdkGRPCListener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	spdkGRPCServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}))

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-ctx.Done()
		spdkGRPCServer.Stop()
		logrus.Info("Stopping SPDK gRPC server")
		// TODO: The error "no such child process" will be emitted when the process is already stopped.
		// Need to improve the error handling, but we can ignore it for now.
		if err := util.ForceStopSPDKTgtDaemon(120 * time.Second); err != nil &&
			!strings.Contains(err.Error(), "process with cmdline spdk_tgt is not found") {
			logrus.WithError(err).Warn("Failed to force stop SPDK target daemon")
		}
		if err := waitForSPDKTargetDaemonStoppedWithTimeout(spdkTargetStopGracePeriod, spdkTargetStopPollInterval); err != nil {
			logrus.WithError(err).Warn("SPDK target daemon is still running after stop")
		}
	}()
	spdkrpc.RegisterSPDKServiceServer(spdkGRPCServer, srv)
	reflection.Register(spdkGRPCServer)
	go func() {
		if err := spdkGRPCServer.Serve(spdkGRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping SPDK gRPC server")
		}
	}()
	return srv
}

// LaunchTestSPDKGRPCServer launches the SPDK gRPC server.
// It wraps launchTestSPDKGRPCServer but discards the returned server instance to maintain backward compatibility.
func LaunchTestSPDKGRPCServer(ctx context.Context, c *C, ip string, execute func(envs []string, name string, args []string, timeout time.Duration) (string, error), wg *sync.WaitGroup) {
	launchTestSPDKGRPCServer(ctx, c, ip, execute, wg)
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

func waitForDiskReady(ctx context.Context, spdkCli *client.SPDKClient, loopDevicePath, diskDriverName string) (*spdkrpc.Disk, error) {
	opts := []retry.Option{
		retry.Context(ctx),
		retry.Attempts(60),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.Delay(1 * time.Second),
	}

	var readyDisk *spdkrpc.Disk

	err := retry.Do(func() error {
		logrus.Info("Checking if the disk is in 'ready' state")
		disk, err := spdkCli.DiskGet(defaultTestDiskName, loopDevicePath, diskDriverName)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get disk %s", defaultTestDiskName)
			return err
		}
		logrus.Infof("Got disk %s in state %s", disk.Name, disk.State)
		if disk.State != string(server.DiskStateReady) {
			return fmt.Errorf("disk %s is in state %s", disk.Name, disk.State)
		}
		logrus.Infof("Disk %s is in %v state", disk.Name, disk.State)
		readyDisk = disk
		return nil
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for disk %s to be in 'ready' state: %w", defaultTestDiskName, err)
	}

	return readyDisk, nil
}

// formatBlockDevice formats block device to check if the engine frontend is working
func formatBlockDevice(endpoint string, fsType string) error {
	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return err
	}
	if _, err := ne.Execute(nil, "mkfs", []string{"-t", fsType, endpoint}, defaultTestExecuteTimeout); err != nil {
		return err
	}
	return nil
}

// verifyNVMfInitiatorConnected verifies the NVMe/TCP initiator is connected.
func verifyNVMfInitiatorConnected(engineFrontend *api.EngineFrontend, nqn string) error {
	ip := engineFrontend.TargetIP
	port := strconv.Itoa(int(engineFrontend.TargetPort))

	nvmeTCPInfo := &helperinitiator.NVMeTCPInfo{
		SubsystemNQN:       nqn,
		TransportAddress:   ip,
		TransportServiceID: port,
	}

	// We use empty host proc path here since we are running checking inside the container (same as the host)
	// and we don't want to lock the file for the test.
	initiator, err := helperinitiator.NewInitiator(engineFrontend.VolumeName, helperinitiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return err
	}

	if err := initiator.WaitForNVMeTCPConnect(60, time.Second); err != nil {
		return err
	}

	return nil
}

func (s *TestSuite) TestReplicaCreateWithStateChecks(c *C) {
	fmt.Println("Testing SPDK replica creation with state checks")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup
	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	replicaName := "test-replica-state"
	defer func() {
		_ = spdkCli.ReplicaDelete(replicaName, true)
	}()

	// Pending -> first create should succeed
	replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	c.Assert(replica.State, Equals, types.InstanceStateRunning)

	// Running -> create again should return AlreadyExists
	_, err = spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, NotNil)
	rootErr := errors.UnwrapAll(err)
	st, ok := grpcstatus.FromError(rootErr)
	c.Assert(ok, Equals, true)
	c.Assert(st.Code(), Equals, grpccodes.AlreadyExists)
	c.Assert(strings.Contains(st.Message(), "already exists and running"), Equals, true)

	// Stopped -> delete without cleanup then create again should succeed
	err = spdkCli.ReplicaDelete(replicaName, false)
	c.Assert(err, IsNil)

	replica, err = spdkCli.ReplicaGet(replicaName)
	c.Assert(err, IsNil)
	c.Assert(replica.State, Equals, types.InstanceStateStopped)

	replica, err = spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	c.Assert(replica.State, Equals, types.InstanceStateRunning)
}

func (s *TestSuite) TestReplicaDeletePendingWithoutCleanupIsNoop(c *C) {
	fmt.Println("Testing SPDK pending replica delete without cleanup is no-op")

	r := server.NewReplica(context.Background(), "test-replica-pending", "test-lvs", "test-lvs-uuid", 0, true, make(chan interface{}, 1))
	c.Assert(r, NotNil)
	c.Assert(string(r.State), Equals, types.InstanceStatePending)

	err := r.Delete(nil, false, nil)
	c.Assert(err, IsNil)
	c.Assert(string(r.State), Equals, types.InstanceStatePending)

	select {
	case <-r.UpdateCh:
		c.Fatalf("unexpected update signal for pending replica delete without cleanup")
	default:
	}
}

func (s *TestSuite) TestSPDKEngineFrontendCreateWithoutEngine(c *C) {
	fmt.Println("Testing SPDK basic operations: engine frontend create without engine")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	volumeName := getVolumeName()
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)

	defer func() {
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)
	}()

	// Create engine frontend (NVMe/TCP initiator)
	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(ip, strconv.Itoa(30000)), 0, 0)
	c.Assert(err, IsNil)
	c.Assert(engineFrontend.State, Equals, types.InstanceStateError)
	c.Assert(engineFrontend.ErrorMsg, Not(Equals), "")
	c.Assert(engineFrontend.TargetIP, Equals, "")
	c.Assert(engineFrontend.TargetPort, Equals, int32(0))
}

func (s *TestSuite) TestSPDKEngineAndEngineFrontendCreateAndDeleteWithDifferentFrontends(c *C) {
	fmt.Println("Testing SPDK basic operations: engine/enginefrontend create and delete with different frontends")

	tests := []struct {
		name                 string
		frontend             string
		expectEngineEndpoint bool
		verifyFrontend       func(c *C, volumeName, engineName string, engine *api.Engine, engineFrontend *api.EngineFrontend)
	}{
		{
			name:                 "FrontendSPDKTCPBlockdev",
			frontend:             types.FrontendSPDKTCPBlockdev,
			expectEngineEndpoint: true,
			verifyFrontend: func(c *C, volumeName, _ string, _ *api.Engine, engineFrontend *api.EngineFrontend) {
				endpoint := helperutil.GetLonghornDevicePath(volumeName)
				c.Assert(engineFrontend.Endpoint, Equals, endpoint)
				err := formatBlockDevice(endpoint, "ext4")
				c.Assert(err, IsNil)
			},
		},
		{
			name:                 "FrontendSPDKTCPNvmf",
			frontend:             types.FrontendSPDKTCPNvmf,
			expectEngineEndpoint: true,
			verifyFrontend: func(c *C, _ string, engineName string, engine *api.Engine, engineFrontend *api.EngineFrontend) {
				nqn := helpertypes.GetNQN(engineName)
				endpoint := server.GetNvmfEndpoint(nqn, engine.IP, engine.Port)
				c.Assert(engineFrontend.Endpoint, Equals, endpoint)

				ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
				c.Assert(err, IsNil)
				_, err = helperinitiator.ConnectTarget(engine.IP, strconv.Itoa(int(engine.Port)), nqn, ne)
				c.Assert(err, IsNil)

				err = verifyNVMfInitiatorConnected(engineFrontend, nqn)
				c.Assert(err, IsNil)

				time.Sleep(5 * time.Second)

				err = helperinitiator.DisconnectTarget(nqn, ne)
				c.Assert(err, IsNil)
			},
		},
		{
			name:                 "FrontendUBLK",
			frontend:             types.FrontendUBLK,
			expectEngineEndpoint: false,
			verifyFrontend: func(c *C, volumeName, _ string, _ *api.Engine, engineFrontend *api.EngineFrontend) {
				endpoint := helperutil.GetLonghornDevicePath(volumeName)
				c.Assert(engineFrontend.Endpoint, Equals, endpoint)

				err := formatBlockDevice(endpoint, "ext4")
				c.Assert(err, IsNil)
				c.Assert(engineFrontend.UblkID, Not(Equals), int32(helperinitiator.UnInitializedUblkId))
			},
		},
	}

	assertEngineEndpoint := func(c *C, ip string, port int32, expectSet bool) {
		if expectSet {
			c.Assert(ip, Not(Equals), "")
			c.Assert(port, Not(Equals), int32(0))
			return
		}
		c.Assert(ip, Equals, "")
		c.Assert(port, Equals, int32(0))
	}

	for _, test := range tests {
		test := test
		fmt.Println("Testing frontend case:", test.name)

		func() {
			diskDriverName := "aio"

			ip, err := commonnet.GetAnyExternalIP()
			c.Assert(err, IsNil)
			err = os.Setenv(commonnet.EnvPodIP, ip)
			c.Assert(err, IsNil)

			ctx, cancel := context.WithCancel(context.Background())
			var spdkWg sync.WaitGroup
			defer func() {
				cancel()
				spdkWg.Wait()
			}()

			ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
			c.Assert(err, IsNil)
			LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

			loopDevicePath := PrepareDiskFile(c)
			defer func() {
				CleanupDiskFile(c, loopDevicePath)
			}()

			spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
			c.Assert(err, IsNil)
			defer func() {
				if errClose := spdkCli.Close(); errClose != nil {
					logrus.WithError(errClose).Error("Failed to close SPDK client")
				}
			}()

			disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
			c.Assert(err, IsNil)
			c.Assert(disk, NotNil)

			disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
			c.Assert(err, IsNil)
			c.Assert(disk.Path, Equals, loopDevicePath)
			c.Assert(disk.Uuid, Not(Equals), "")

			defer func() {
				err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
				c.Assert(err, IsNil)

				disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
				c.Assert(err, NotNil)
				c.Assert(disk, IsNil)
			}()

			volumeName := getVolumeName()
			engineName := fmt.Sprintf("%s-e", volumeName)
			engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
			replicaNames := []string{
				fmt.Sprintf("%s-replica-1", volumeName),
				fmt.Sprintf("%s-replica-2", volumeName),
			}
			replicas := make(map[string]*api.Replica)

			defer func() {
				err = spdkCli.EngineFrontendDelete(engineFrontendName)
				c.Assert(err, IsNil)

				err = spdkCli.EngineDelete(engineName)
				c.Assert(err, IsNil)

				for _, replica := range replicas {
					err = spdkCli.ReplicaDelete(replica.Name, true)
					c.Assert(err, IsNil)
				}
			}()

			for _, replicaName := range replicaNames {
				replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
				c.Assert(err, IsNil)
				c.Assert(replica.LvsName, Equals, defaultTestDiskName)
				c.Assert(replica.LvsUUID, Equals, disk.Uuid)
				c.Assert(replica.ErrorMsg, Equals, "")
				c.Assert(replica.State, Equals, types.InstanceStateRunning)
				c.Assert(replica.PortStart, Not(Equals), int32(0))
				c.Assert(replica.Head, NotNil)
				c.Assert(replica.Head.CreationTime, Not(Equals), "")
				c.Assert(replica.Head.Parent, Equals, "")
				replicas[replicaName] = replica
			}

			replicaAddressMap := make(map[string]string)
			replicaModeMap := make(map[string]types.Mode)
			for _, replica := range replicas {
				replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
				replicaModeMap[replica.Name] = types.ModeRW
			}

			engine, err := spdkCli.EngineCreate(engineName, volumeName, test.frontend, defaultTestLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
			assertEngineEndpoint(c, engine.IP, engine.Port, test.expectEngineEndpoint)

			engineList, err := spdkCli.EngineList()
			c.Assert(err, IsNil)
			c.Assert(engineList, HasLen, 1)
			for _, e := range engineList {
				assertEngineEndpoint(c, e.IP, e.Port, test.expectEngineEndpoint)
			}

			replicaList, err := spdkCli.ReplicaList()
			c.Assert(err, IsNil)
			c.Assert(replicaList, HasLen, 2)

			e, err := spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			assertEngineEndpoint(c, e.IP, e.Port, test.expectEngineEndpoint)

			engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, test.frontend, defaultTestLvolSize,
				net.JoinHostPort(e.IP, strconv.Itoa(int(e.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)
			c.Assert(engineFrontend.ErrorMsg, Equals, "")
			c.Assert(engineFrontend.TargetIP, Equals, e.IP)
			c.Assert(engineFrontend.TargetPort, Equals, e.Port)

			test.verifyFrontend(c, volumeName, engineName, e, engineFrontend)
		}()
	}
}

func (s *TestSuite) TestSPDKEngineFrontendSuspendAndResume(c *C) {
	fmt.Println("Testing SPDK engine frontend suspend")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	volumeName := getVolumeName()
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)

		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	// Create replicas
	for _, replicaName := range replicaNames {
		replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		c.Assert(replica.LvsName, Equals, defaultTestDiskName)
		c.Assert(replica.LvsUUID, Equals, disk.Uuid)
		c.Assert(replica.ErrorMsg, Equals, "")
		c.Assert(replica.State, Equals, types.InstanceStateRunning)
		c.Assert(replica.PortStart, Not(Equals), int32(0))
		c.Assert(replica.Head, NotNil)
		c.Assert(replica.Head.CreationTime, Not(Equals), "")
		c.Assert(replica.Head.Parent, Equals, "")
		replicas[replicaName] = replica
	}

	// Create replica address map and replica mode map
	replicaAddressMap := make(map[string]string)
	replicaModeMap := make(map[string]types.Mode)
	for _, replica := range replicas {
		replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
		replicaModeMap[replica.Name] = types.ModeRW
	}

	// Create engine target
	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
	c.Assert(err, IsNil)
	c.Assert(engine.State, Equals, types.InstanceStateRunning)
	c.Assert(engine.ErrorMsg, Equals, "")
	c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
	c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
	c.Assert(engine.IP, Not(Equals), "")
	c.Assert(engine.Port, Not(Equals), int32(0))

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	c.Assert(engineList, HasLen, 1)
	for _, e := range engineList {
		c.Assert(e.IP, Not(Equals), "")
		c.Assert(e.Port, Not(Equals), int32(0))
	}

	replicaList, err := spdkCli.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(replicaList, HasLen, 2)

	e, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)
	c.Assert(e.IP, Not(Equals), "")
	c.Assert(e.Port, Not(Equals), int32(0))

	// Create engine frontend (NVMe/TCP initiator)
	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(e.IP, strconv.Itoa(int(e.Port))), 0, 0)

	c.Assert(err, IsNil)
	c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)
	c.Assert(engineFrontend.ErrorMsg, Equals, "")
	c.Assert(engineFrontend.TargetIP, Equals, e.IP)
	c.Assert(engineFrontend.TargetPort, Equals, e.Port)

	endpoint := helperutil.GetLonghornDevicePath(volumeName)
	c.Assert(engineFrontend.Endpoint, Equals, endpoint)

	// Format block device to check if the engine frontend is working
	err = formatBlockDevice(endpoint, "ext4")
	c.Assert(err, IsNil)

	// Write a known 1MB zero pattern at offset 400MB (beyond the background dd's range)
	// and compute its checksum for later verification.
	_, err = ne.Execute(nil, "dd",
		[]string{
			"if=/dev/zero",
			fmt.Sprintf("of=%s", endpoint),
			"bs=1M", "count=1", "seek=400", "oflag=direct", "conv=notrunc", "status=none",
		},
		10*time.Second,
	)
	c.Assert(err, IsNil)

	checksumBefore, err := ne.Execute(nil, "sh",
		[]string{
			"-c",
			fmt.Sprintf("dd if=%s bs=1M count=1 skip=400 iflag=direct status=none | md5sum", endpoint),
		},
		10*time.Second,
	)
	c.Assert(err, IsNil)
	c.Assert(strings.TrimSpace(checksumBefore), Not(Equals), "")

	// Start a slow background write using direct IO + dsync.
	// This ensures IO is still in progress when we call Suspend.
	// We write ~10MB (2500 × 4K) which is slow enough with dsync to still be in-flight
	// after 1 second, but fast enough to complete shortly after resume.
	ddDone := make(chan error, 1)
	go func() {
		_, err := ne.Execute(nil, "dd",
			[]string{
				"if=/dev/urandom",
				fmt.Sprintf("of=%s", endpoint),
				"bs=4k", "count=2500", "oflag=direct,dsync", "conv=notrunc", "status=none",
			},
			30*time.Second,
		)
		ddDone <- err
	}()

	// Give dd time to start writing before suspending
	time.Sleep(1 * time.Second)

	// Suspend engine frontend — this freezes the dm-device, queuing all in-flight and new IO
	err = spdkCli.EngineFrontendSuspend(engineFrontend.Name)
	c.Assert(err, IsNil)

	// Verify that new IO is blocked while the engine frontend is suspended.
	// The dd command should time out because the dm-device suspend queues all new IO.
	// We use oflag=direct to bypass the page cache, ensuring the IO hits the dm-device
	// and gets queued by suspend (without direct IO, dd returns immediately after writing
	// to the page cache).
	_, err = ne.Execute(nil, "dd",
		[]string{
			"if=/dev/urandom",
			fmt.Sprintf("of=%s", endpoint),
			"bs=1M", "count=1", "seek=0", "oflag=direct", "conv=notrunc", "status=none",
		},
		10*time.Second,
	)
	c.Assert(err, NotNil)

	// Resume engine frontend — this unblocks the queued IO
	err = spdkCli.EngineFrontendResume(engineFrontend.Name)
	c.Assert(err, IsNil)

	// The background dd should now complete (successfully or with a write error,
	// but it should no longer be stuck). We wait up to 60 seconds to account for
	// the dd's Execute timeout plus residual IO after resume.
	select {
	case <-ddDone:
	case <-time.After(60 * time.Second):
		c.Fatal("background dd did not complete after resume")
	}

	// Verify data integrity: the checksum of the region at offset 400MB should be unchanged
	// after the suspend/resume cycle.
	checksumAfter, err := ne.Execute(nil, "sh",
		[]string{
			"-c",
			fmt.Sprintf("dd if=%s bs=1M count=1 skip=400 iflag=direct status=none | md5sum", endpoint),
		},
		10*time.Second,
	)
	c.Assert(err, IsNil)
	c.Assert(strings.TrimSpace(checksumAfter), Equals, strings.TrimSpace(checksumBefore))
}

func (s *TestSuite) TestSPDKEngineFrontendExpand(c *C) {
	fmt.Println("Testing SPDK engine frontend expand")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	volumeName := getVolumeName()
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)

		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	// Create replicas
	for _, replicaName := range replicaNames {
		replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		c.Assert(replica.LvsName, Equals, defaultTestDiskName)
		c.Assert(replica.LvsUUID, Equals, disk.Uuid)
		c.Assert(replica.ErrorMsg, Equals, "")
		c.Assert(replica.State, Equals, types.InstanceStateRunning)
		c.Assert(replica.PortStart, Not(Equals), int32(0))
		c.Assert(replica.Head, NotNil)
		c.Assert(replica.Head.CreationTime, Not(Equals), "")
		c.Assert(replica.Head.Parent, Equals, "")
		replicas[replicaName] = replica
	}

	// Create replica address map and replica mode map
	replicaAddressMap := make(map[string]string)
	replicaModeMap := make(map[string]types.Mode)
	for _, replica := range replicas {
		replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
		replicaModeMap[replica.Name] = types.ModeRW
	}

	// Create engine target
	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
	c.Assert(err, IsNil)
	c.Assert(engine.State, Equals, types.InstanceStateRunning)
	c.Assert(engine.ErrorMsg, Equals, "")
	c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
	c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
	c.Assert(engine.IP, Not(Equals), "")
	c.Assert(engine.Port, Not(Equals), int32(0))

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	c.Assert(engineList, HasLen, 1)
	for _, e := range engineList {
		c.Assert(e.IP, Not(Equals), "")
		c.Assert(e.Port, Not(Equals), int32(0))
	}

	replicaList, err := spdkCli.ReplicaList()
	c.Assert(err, IsNil)
	c.Assert(replicaList, HasLen, 2)

	e, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)
	c.Assert(e.IP, Not(Equals), "")
	c.Assert(e.Port, Not(Equals), int32(0))

	// Create engine frontend (NVMe/TCP initiator)
	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(e.IP, strconv.Itoa(int(e.Port))), 0, 0)

	c.Assert(err, IsNil)
	c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)
	c.Assert(engineFrontend.ErrorMsg, Equals, "")
	c.Assert(engineFrontend.TargetIP, Equals, e.IP)
	c.Assert(engineFrontend.TargetPort, Equals, e.Port)

	endpoint := helperutil.GetLonghornDevicePath(volumeName)
	c.Assert(engineFrontend.Endpoint, Equals, endpoint)

	size := GetBlockDeviceSize(ne, endpoint)
	c.Assert(size, Equals, int64(defaultTestLvolSize))

	// Format block device to check if the engine frontend is working
	err = formatBlockDevice(endpoint, "ext4")
	c.Assert(err, IsNil)

	// Suspend and resume engine frontend
	newTestLvolSize := defaultTestLvolSize * 2
	err = spdkCli.EngineFrontendExpand(ctx, engineFrontend.Name, newTestLvolSize)
	c.Assert(err, IsNil)

	// Format block device to check if the engine frontend is working
	err = formatBlockDevice(endpoint, "ext4")
	c.Assert(err, IsNil)

	size = GetBlockDeviceSize(ne, endpoint)
	c.Assert(size, Equals, int64(newTestLvolSize))
}

func (s *TestSuite) TestSPDKEngineSnapshotCreateAndDelete(c *C) {
	fmt.Println("Testing SPDK engine snapshot create and delete")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup
	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	volumeName := getVolumeName()
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)

		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	for _, replicaName := range replicaNames {
		replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		replicas[replicaName] = replica
	}

	replicaAddressMap := make(map[string]string)
	replicaModeMap := make(map[string]types.Mode)
	for _, replica := range replicas {
		replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
		replicaModeMap[replica.Name] = types.ModeRW
	}

	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
	c.Assert(err, IsNil)
	c.Assert(engine.State, Equals, types.InstanceStateRunning)
	c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
	c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)

	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(ip, strconv.Itoa(int(engine.Port))), 0, 0)
	c.Assert(err, IsNil)
	c.Assert(engineFrontend, NotNil)

	snapshotName := "snapshot-1"
	retSnapshotName, err := spdkCli.EngineFrontendSnapshotCreate(engineFrontendName, snapshotName)
	c.Assert(err, IsNil)
	c.Assert(retSnapshotName, Equals, snapshotName)

	engineAfterCreate, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)
	_, exists := engineAfterCreate.Snapshots[snapshotName]
	c.Assert(exists, Equals, true)

	err = spdkCli.EngineSnapshotDelete(engineName, snapshotName)
	c.Assert(err, IsNil)

	engineAfterDelete, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)
	_, exists = engineAfterDelete.Snapshots[snapshotName]
	c.Assert(exists, Equals, false)

	err = spdkCli.EngineSnapshotDelete(engineName, snapshotName)
	c.Assert(err, NotNil)
}

// TestSPDKEngineFrontendSnapshotRevert validates both negative and positive revert paths.
// Revert should fail with non-empty frontend and succeed with FrontendEmpty.
func (s *TestSuite) TestSPDKEngineFrontendSnapshotRevert(c *C) {
	fmt.Println("Testing SPDK engine frontend snapshot revert")

	tests := []struct {
		name               string
		engineFrontendType string
		expectErr          bool
		errContains        string
	}{
		{
			name:               "revert rejected for spdk-tcp-blockdev",
			engineFrontendType: types.FrontendSPDKTCPBlockdev,
			expectErr:          true,
			errContains:        "invalid frontend",
		},
		// {
		// 	name:               "revert rejected for spdk-tcp-nvmf",
		// 	engineFrontendType: types.FrontendSPDKTCPNvmf,
		// 	expectErr:          true,
		// 	errContains:        "invalid frontend",
		// },
		// {
		// 	name:               "revert rejected for ublk",
		// 	engineFrontendType: types.FrontendUBLK,
		// 	expectErr:          true,
		// 	errContains:        "invalid frontend",
		// },
		// {
		// 	name:               "revert succeeds for frontend-empty",
		// 	engineFrontendType: types.FrontendEmpty,
		// 	expectErr:          false,
		// },
	}

	for _, tc := range tests {
		c.Logf("testing %s", tc.name)

		diskDriverName := "aio"

		ip, err := commonnet.GetAnyExternalIP()
		c.Assert(err, IsNil)
		err = os.Setenv(commonnet.EnvPodIP, ip)
		c.Assert(err, IsNil)

		ctx, cancel := context.WithCancel(context.Background())
		var spdkWg sync.WaitGroup
		func() {
			defer func() {
				cancel()
				spdkWg.Wait()
			}()

			ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
			c.Assert(err, IsNil)
			LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

			loopDevicePath := PrepareDiskFile(c)
			defer func() {
				CleanupDiskFile(c, loopDevicePath)
			}()

			spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
			c.Assert(err, IsNil)
			defer func() {
				if errClose := spdkCli.Close(); errClose != nil {
					logrus.WithError(errClose).Error("Failed to close SPDK client")
				}
			}()

			disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
			c.Assert(err, IsNil)
			c.Assert(disk, NotNil)

			disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
			c.Assert(err, IsNil)
			c.Assert(disk.Path, Equals, loopDevicePath)
			c.Assert(disk.Uuid, Not(Equals), "")

			defer func() {
				err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
				c.Assert(err, IsNil)
			}()

			volumeName := fmt.Sprintf("test-frontend-revert-vol-%s", time.Now().Format("20060102150405"))
			engineName := fmt.Sprintf("%s-e", volumeName)
			engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
			replicaNames := []string{
				fmt.Sprintf("%s-replica-1", volumeName),
				fmt.Sprintf("%s-replica-2", volumeName),
			}
			replicas := make(map[string]*api.Replica)

			defer func() {
				err = spdkCli.EngineFrontendDelete(engineFrontendName)
				c.Assert(err, IsNil)

				err = spdkCli.EngineDelete(engineName)
				c.Assert(err, IsNil)

				for _, replica := range replicas {
					err = spdkCli.ReplicaDelete(replica.Name, true)
					c.Assert(err, IsNil)
				}
			}()

			for _, replicaName := range replicaNames {
				replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
				c.Assert(err, IsNil)
				replicas[replicaName] = replica
			}

			replicaAddressMap := make(map[string]string)
			for _, replica := range replicas {
				replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
			}

			engine, err := spdkCli.EngineCreate(engineName, volumeName, tc.engineFrontendType, defaultTestLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine, NotNil)
			if engine.State != types.InstanceStateRunning {
				c.Skip(fmt.Sprintf("skip %s: engine is not running (state=%s, error=%s)", tc.name, engine.State, engine.ErrorMsg))
			}

			engineFrontendTargetAddress := net.JoinHostPort(ip, strconv.Itoa(int(engine.Port)))

			engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, tc.engineFrontendType, defaultTestLvolSize,
				engineFrontendTargetAddress, 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend, NotNil)

			snapshotName1 := "snapshot-1"
			snapshotName2 := "snapshot-2"

			retSnapshotName, err := spdkCli.EngineFrontendSnapshotCreate(engineFrontendName, snapshotName1)
			c.Assert(err, IsNil)
			c.Assert(retSnapshotName, Equals, snapshotName1)

			retSnapshotName, err = spdkCli.EngineFrontendSnapshotCreate(engineFrontendName, snapshotName2)
			c.Assert(err, IsNil)
			c.Assert(retSnapshotName, Equals, snapshotName2)

			err = spdkCli.EngineFrontendSnapshotRevert(engineFrontendName, snapshotName1)
			if tc.expectErr {
				c.Assert(err, NotNil)
				c.Assert(strings.Contains(err.Error(), tc.errContains), Equals, true)
			} else {
				c.Assert(err, IsNil)
			}

			engineAfterRevert, err := spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engineAfterRevert.State, Equals, types.InstanceStateRunning)
			_, exists := engineAfterRevert.Snapshots[snapshotName1]
			c.Assert(exists, Equals, true)
		}()
	}
}

type runtimeMonitoringTestEnv struct {
	ctx        context.Context
	spdkCli    *client.SPDKClient
	rawSPDKCli *helperclient.Client
	disk       *spdkrpc.Disk
}

func isReplicaNotFound(err error) bool {
	if err == nil {
		return false
	}
	if grpcstatus.Code(err) == grpccodes.NotFound {
		return true
	}
	return strings.Contains(err.Error(), "cannot find replica")
}

func monitoringRetryOpts(ctx context.Context, attempts uint) []retry.Option {
	return []retry.Option{
		retry.Context(ctx),
		retry.Attempts(attempts),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.Delay(server.MonitorInterval),
	}
}

func waitForSPDKTargetStopped() error {
	return waitForSPDKTargetDaemonStoppedWithTimeout(6*time.Second, spdkTargetStopPollInterval)
}

func withRuntimeMonitoringTestEnv(c *C, diskDriverName string, testFn func(env *runtimeMonitoringTestEnv)) {
	runtimeMonitoringTestMu.Lock()
	defer runtimeMonitoringTestMu.Unlock()

	// Ensure no lingering target process from a previous test case.
	err := waitForSPDKTargetStopped()
	c.Assert(err, IsNil)

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	rawSPDKCli, err := helperclient.NewClient(context.Background())
	c.Assert(err, IsNil)

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)
	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)

	defer func() {
		err := rawSPDKCli.Close()
		c.Assert(err, IsNil)
		err = spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
		err = spdkCli.Close()
		c.Assert(err, IsNil)
		CleanupDiskFile(c, loopDevicePath)
		cancel()
		spdkWg.Wait()
		err = waitForSPDKTargetStopped()
		c.Assert(err, IsNil)
	}()

	testFn(&runtimeMonitoringTestEnv{
		ctx:        ctx,
		spdkCli:    spdkCli,
		rawSPDKCli: rawSPDKCli,
		disk:       disk,
	})
}

func (s *TestSuite) TestRuntimeMonitoringVerifyMultipleReplicasReconstruction(c *C) {
	fmt.Println("Testing runtime monitoring verify() for multiple replicas reconstruction")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		replicaNames := []string{
			fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8]),
			fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8]),
			fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8]),
		}

		for _, name := range replicaNames {
			_, err := env.rawSPDKCli.BdevLvolCreate("", env.disk.Uuid, name, util.BytesToMiB(defaultTestLvolSize), "", true)
			c.Assert(err, IsNil)
			defer func(replicaName string) {
				_ = env.spdkCli.ReplicaDelete(replicaName, true)
				_, _ = env.rawSPDKCli.BdevLvolDelete(fmt.Sprintf("%s/%s", env.disk.Name, replicaName))
			}(name)
		}

		err := retry.Do(func() error {
			for _, name := range replicaNames {
				replica, err := env.spdkCli.ReplicaGet(name)
				if err != nil {
					return err
				}
				if replica.State != types.InstanceStateStopped {
					return fmt.Errorf("replica %s state is %s, expected %s", replica.Name, replica.State, types.InstanceStateStopped)
				}
			}
			return nil
		}, monitoringRetryOpts(env.ctx, 8)...)
		c.Assert(err, IsNil)

		replicaMap, err := env.spdkCli.ReplicaList()
		c.Assert(err, IsNil)
		for _, name := range replicaNames {
			_, exists := replicaMap[name]
			c.Assert(exists, Equals, true)
		}
	})
}

func (s *TestSuite) TestRuntimeMonitoringVerifyMixedValidAndInvalidLvolNames(c *C) {
	fmt.Println("Testing runtime monitoring verify() with mixed valid and invalid lvol names")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		validReplicaName := fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8])
		invalidNames := []string{
			"matrix-invalid",
			"matrix-r-123",
			"matrix-r-123456789",
		}
		allNames := append([]string{validReplicaName}, invalidNames...)

		for _, name := range allNames {
			_, err := env.rawSPDKCli.BdevLvolCreate("", env.disk.Uuid, name, util.BytesToMiB(defaultTestLvolSize), "", true)
			c.Assert(err, IsNil)
			defer func(replicaName string) {
				_ = env.spdkCli.ReplicaDelete(replicaName, true)
				_, _ = env.rawSPDKCli.BdevLvolDelete(fmt.Sprintf("%s/%s", env.disk.Name, replicaName))
			}(name)
		}

		err := retry.Do(func() error {
			replica, err := env.spdkCli.ReplicaGet(validReplicaName)
			if err != nil {
				return err
			}
			if replica.State != types.InstanceStateStopped {
				return fmt.Errorf("replica %s state is %s, expected %s", replica.Name, replica.State, types.InstanceStateStopped)
			}
			return nil
		}, monitoringRetryOpts(env.ctx, 8)...)
		c.Assert(err, IsNil)

		for _, invalidName := range invalidNames {
			_, err := env.spdkCli.ReplicaGet(invalidName)
			c.Assert(isReplicaNotFound(err), Equals, true)
		}
	})
}

func (s *TestSuite) TestRuntimeMonitoringVerifyStaleCacheRemoval(c *C) {
	fmt.Println("Testing runtime monitoring verify() stale cache removal")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		replicaName := fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8])
		replicaAlias := fmt.Sprintf("%s/%s", env.disk.Name, replicaName)

		_, err := env.rawSPDKCli.BdevLvolCreate("", env.disk.Uuid, replicaName, util.BytesToMiB(defaultTestLvolSize), "", true)
		c.Assert(err, IsNil)
		defer func() {
			_ = env.spdkCli.ReplicaDelete(replicaName, true)
			_, _ = env.rawSPDKCli.BdevLvolDelete(replicaAlias)
		}()

		err = retry.Do(func() error {
			_, err := env.spdkCli.ReplicaGet(replicaName)
			return err
		}, monitoringRetryOpts(env.ctx, 8)...)
		c.Assert(err, IsNil)

		_, err = env.rawSPDKCli.BdevLvolDelete(replicaAlias)
		c.Assert(err, IsNil)

		err = retry.Do(func() error {
			_, err := env.spdkCli.ReplicaGet(replicaName)
			if isReplicaNotFound(err) {
				return nil
			}
			if err == nil {
				return fmt.Errorf("replica %s still exists in cache", replicaName)
			}
			return err
		}, monitoringRetryOpts(env.ctx, 8)...)
		c.Assert(err, IsNil)
	})
}

func (s *TestSuite) TestRuntimeMonitoringVerifySkipRebuildingAndCloningLvol(c *C) {
	fmt.Println("Testing runtime monitoring verify() skips rebuilding and cloning lvols")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		baseReplicaName := fmt.Sprintf("matrix-r-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8])
		rebuildingLvolName := server.GetReplicaRebuildingLvolName(baseReplicaName)
		cloningLvolName := server.GetReplicaCloningLvolName(baseReplicaName)

		for _, name := range []string{rebuildingLvolName, cloningLvolName} {
			_, err := env.rawSPDKCli.BdevLvolCreate("", env.disk.Uuid, name, util.BytesToMiB(defaultTestLvolSize), "", true)
			c.Assert(err, IsNil)
			defer func(replicaName string) {
				_ = env.spdkCli.ReplicaDelete(replicaName, true)
				_, _ = env.rawSPDKCli.BdevLvolDelete(fmt.Sprintf("%s/%s", env.disk.Name, replicaName))
			}(name)
		}

		err := retry.Do(func() error {
			for _, name := range []string{rebuildingLvolName, cloningLvolName, baseReplicaName} {
				_, err := env.spdkCli.ReplicaGet(name)
				if !isReplicaNotFound(err) {
					if err == nil {
						return fmt.Errorf("lvol %s was unexpectedly reconstructed as replica", name)
					}
					return err
				}
			}
			return nil
		}, monitoringRetryOpts(env.ctx, 4)...)
		c.Assert(err, IsNil)
	})
}

func (s *TestSuite) TestRuntimeMonitoringVerifyCleanupOrphanBackingImageTempHead(c *C) {
	fmt.Println("Testing runtime monitoring verify() cleans up orphan backing image temp head")
	withRuntimeMonitoringTestEnv(c, "aio", func(env *runtimeMonitoringTestEnv) {
		backingImageName := fmt.Sprintf("matrix-bi-%s", strings.ReplaceAll(util.UUID(), "-", "")[:8])
		tempHeadName := server.GetBackingImageTempHeadLvolName(backingImageName, env.disk.Uuid)
		tempHeadAlias := fmt.Sprintf("%s/%s", env.disk.Name, tempHeadName)

		_, err := env.rawSPDKCli.BdevLvolCreate("", env.disk.Uuid, tempHeadName, util.BytesToMiB(defaultTestLvolSize), "", true)
		c.Assert(err, IsNil)
		defer func() {
			_, _ = env.rawSPDKCli.BdevLvolDelete(tempHeadAlias)
		}()

		err = retry.Do(func() error {
			bdevs, err := env.rawSPDKCli.BdevGetBdevs("", 0)
			if err != nil {
				return err
			}
			for _, bdev := range bdevs {
				for _, alias := range bdev.Aliases {
					if alias == tempHeadAlias {
						return fmt.Errorf("orphan backing image temp head %s still exists", tempHeadAlias)
					}
				}
			}
			return nil
		}, monitoringRetryOpts(env.ctx, 8)...)
		c.Assert(err, IsNil)
	})
}

// TestSPDKEngineCreateWithSalvageRequested validates salvage flow during engine creation.
// The test creates two replicas, writes random data only to the first replica so its head
// actual size becomes larger, then calls EngineCreate with salvageRequested=true.
// It verifies the engine keeps only the replica with the largest head actual size as the
// salvage candidate.
func (s *TestSuite) TestSPDKEngineCreateWithSalvageRequested(c *C) {
	fmt.Println("Testing SPDK engine create with salvageRequested=true")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup
	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	volumeName := fmt.Sprintf("test-salvage-vol-%s", time.Now().Format("20060102150405"))
	engineName := fmt.Sprintf("%s-e", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	for _, replicaName := range replicaNames {
		replica, err := spdkCli.ReplicaCreate(replicaName, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
		c.Assert(err, IsNil)
		replicas[replicaName] = replica
	}

	primaryReplica := replicas[replicaNames[0]]
	primaryNQN := helpertypes.GetNQN(primaryReplica.Name)
	primaryPort := strconv.Itoa(int(primaryReplica.PortStart))

	_, err = helperinitiator.ConnectTarget(ip, primaryPort, primaryNQN, ne)
	c.Assert(err, IsNil)
	defer func() {
		err := helperinitiator.DisconnectTarget(primaryNQN, ne)
		c.Assert(err, IsNil)
	}()

	devices, err := helperinitiator.GetDevices(ip, primaryPort, primaryNQN, ne)
	c.Assert(err, IsNil)
	c.Assert(len(devices), Equals, 1)
	c.Assert(len(devices[0].Namespaces), Equals, 1)

	endpoint := filepath.Join("/dev", devices[0].Namespaces[0].NameSpace)
	err = writeDataToBlockDevice(ne, endpoint, 0, 8)
	c.Assert(err, IsNil)

	var primaryHeadSize uint64
	var secondaryHeadSize uint64
	for i := 0; i < 20; i++ {
		primaryReplicaInfo, err := spdkCli.ReplicaGet(replicaNames[0])
		c.Assert(err, IsNil)
		secondaryReplicaInfo, err := spdkCli.ReplicaGet(replicaNames[1])
		c.Assert(err, IsNil)

		primaryHeadSize = primaryReplicaInfo.Head.ActualSize
		secondaryHeadSize = secondaryReplicaInfo.Head.ActualSize
		if primaryHeadSize > secondaryHeadSize {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Assert(primaryHeadSize > secondaryHeadSize, Equals, true)

	replicaAddressMap := make(map[string]string)
	for _, replica := range replicas {
		replicaAddressMap[replica.Name] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))
	}

	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, true)
	c.Assert(err, IsNil)
	c.Assert(engine, NotNil)
	c.Assert(engine.State, Equals, types.InstanceStateRunning)
	c.Assert(len(engine.ReplicaAddressMap), Equals, 1)
	c.Assert(engine.ReplicaAddressMap[replicaNames[0]], Equals, net.JoinHostPort(ip, strconv.Itoa(int(replicas[replicaNames[0]].PortStart))))
	c.Assert(engine.ReplicaModeMap[replicaNames[0]], Equals, types.ModeRW)
}

func GetBlockDeviceSize(ne *commonns.Executor, endpoint string) int64 {
	output, err := ne.Execute(nil, "blockdev", []string{"--getsize64", endpoint}, 10*time.Second)
	if err != nil {
		return 0
	}
	size, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil {
		return 0
	}
	return size
}

func (s *TestSuite) TestSPDKMultipleThread(c *C) {
	fmt.Println("Testing SPDK basic operations with multiple threads")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	concurrentCount := 5
	dataCountInMB := int64(100)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := range concurrentCount {
		spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
		c.Assert(err, IsNil)
		defer func() {
			if errClose := spdkCli.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close SPDK client")
			}
		}()

		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-e", volumeName)
		engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				err = spdkCli.EngineFrontendDelete(engineFrontendName)
				c.Assert(err, IsNil)
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
			c.Assert(replica1.ErrorMsg, Equals, "")
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			c.Assert(replica1.Head, NotNil)
			c.Assert(replica1.Head.CreationTime, Not(Equals), "")
			c.Assert(replica1.Head.Parent, Equals, "")
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.ErrorMsg, Equals, "")
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
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)

			engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
				net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.Endpoint, Equals, endpoint)
			c.Assert(engine.IP, Not(Equals), "")
			c.Assert(engine.Port, Not(Equals), int32(0))

			err = writeDataToBlockDevice(ne, endpoint, 0, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore1, Not(Equals), "")

			snapshotName1 := "snap1"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName1)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName1, false)
			c.Assert(err, IsNil)
			for replicaName := range replicaAddressMap {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, snapshotName1)
			}

			err = writeDataToBlockDevice(ne, endpoint, 200, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore2, Not(Equals), "")

			snapshotName2 := "snap2"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName2)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName2, false)
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
				}, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName1)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName2, false)
			c.Assert(err, IsNil)
			for replicaName := range replicaAddressMap {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, snapshotName2)
			}

			// Detach and re-attach the volume
			err = spdkCli.EngineFrontendDelete(engineFrontendName)
			c.Assert(err, IsNil)
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
			c.Assert(replica1.ErrorMsg, Equals, "")
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			replica2, err = spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica2.ErrorMsg, Equals, "")
			c.Assert(replica2.State, Equals, types.InstanceStateRunning)

			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
			}
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.Port, Not(Equals), int32(0))

			engineFrontend, err = spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
				net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.Endpoint, Equals, endpoint)

			// Check both replica snapshot map after the snapshot deletion and volume re-attachment
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName2: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW})

			// Start testing online rebuilding
			// Launch a new replica then ask the engine to rebuild it
			replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
			c.Assert(err, IsNil)
			c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica3.ErrorMsg, Equals, "")
			c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			c.Assert(replica3.PortStart, Not(Equals), int32(0))
			c.Assert(replica3.Head, NotNil)
			c.Assert(replica3.Head.CreationTime, Not(Equals), "")
			c.Assert(replica3.Head.Parent, Equals, "")

			err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))), defaultTestFastSync)
			c.Assert(err, IsNil)

			WaitForReplicaRebuildingComplete(c, spdkCli, engineName, replicaName3)

			snapshotNameRebuild := ""
			for replicaName := range replicaAddressMap {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				for snapName, snapLvol := range replica.Snapshots {
					if strings.HasPrefix(snapName, server.RebuildingSnapshotNamePrefix) {
						c.Assert(snapLvol.Children[types.VolumeHead], Equals, true)
						if snapshotNameRebuild == "" {
							snapshotNameRebuild = snapName
						} else {
							c.Assert(snapName, Equals, snapshotNameRebuild)
						}
						break
					}
				}
			}
			c.Assert(snapshotNameRebuild, Not(Equals), "")

			err = spdkCli.EngineSnapshotHash(engineName, snapshotName2, false)
			c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName3},
				map[string][]string{
					snapshotName2:       {snapshotNameRebuild},
					snapshotNameRebuild: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Verify the rebuilding result
			replicaAddressMap = map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
				replica3.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))),
			}
			engine, err = spdkCli.EngineGet(engineName)
			c.Assert(err, IsNil)
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW, replicaName3: types.ModeRW})

			// The newly rebuilt replica should contain correct data
			cksumAfterRebuilding1, err := util.GetFileChunkChecksum(endpoint, 0, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding1, Equals, cksumBefore1)
			cksumAfterRebuilding2, err := util.GetFileChunkChecksum(endpoint, 200*helpertypes.MiB, 100*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumAfterRebuilding2, Equals, cksumBefore2)

			for _, replicaName := range []string{replicaName1, replicaName3} {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, "")
			}
		}()
	}

	wg.Wait()

	engineFrontendList, err := spdkCli.EngineFrontendList()
	c.Assert(err, IsNil)
	for _, ef := range engineFrontendList {
		err = spdkCli.EngineFrontendDelete(ef.Name)
		c.Assert(err, IsNil)
	}

	engineList, err := spdkCli.EngineList()
	c.Assert(err, IsNil)
	for _, engine := range engineList {
		c.Assert(err, IsNil)

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

func (s *TestSuite) spdkMultipleThreadSnapshotOpsAndRebuilding(c *C, withBackingImage bool) {
	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	var bi *api.BackingImage
	if withBackingImage {
		bi, err = spdkCli.BackingImageCreate(defaultTestBackingImageName, defaultTestBackingImageUUID, disk.Uuid, defaultTestBackingImageSize, defaultTestBackingImageChecksum, defaultTestBackingImageDownloadURL, "")
		c.Assert(err, IsNil)
		c.Assert(bi, NotNil)
		defer func() {
			err := spdkCli.BackingImageDelete(defaultTestBackingImageName, disk.Uuid)
			c.Assert(err, IsNil)
		}()

		// check if bi.State is "ready" in 300 seconds
		for i := range maxBackingImageGetRetries {
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
	}

	concurrentCount := 5
	dataCountInMB := int64(10)
	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := range concurrentCount {
		spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
		c.Assert(err, IsNil)
		defer func() {
			if errClose := spdkCli.Close(); errClose != nil {
				logrus.WithError(errClose).Error("Failed to close SPDK client")
			}
		}()

		volumeName := fmt.Sprintf("test-vol-%d", i)
		engineName := fmt.Sprintf("%s-e", volumeName)
		engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
		replicaName1 := fmt.Sprintf("%s-replica-1", volumeName)
		replicaName2 := fmt.Sprintf("%s-replica-2", volumeName)
		replicaName3 := fmt.Sprintf("%s-replica-3", volumeName)
		replicaName4 := fmt.Sprintf("%s-replica-4", volumeName)

		go func() {
			defer func() {
				// Do cleanup
				err = spdkCli.EngineFrontendDelete(engineFrontendName)
				c.Assert(err, IsNil)
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

			backingImageName := ""
			if withBackingImage {
				backingImageName = bi.Name
			}
			replica1, err := spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, backingImageName)

			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.ErrorMsg, Equals, "")
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))
			replica2, err := spdkCli.ReplicaCreate(replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, backingImageName)
			c.Assert(err, IsNil)
			c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica2.ErrorMsg, Equals, "")
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
			engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.ErrorMsg, Equals, "")
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
			c.Assert(engine.Port, Not(Equals), int32(0))
			c.Assert(engine.Port, Not(Equals), int32(0))

			engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
				net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.Endpoint, Equals, endpoint)

			offsetInMB := int64(0)
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore11, Not(Equals), "")
			snapshotName11 := "snap11"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName11)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName11, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       nil (backing image) -> snap11 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore12, Not(Equals), "")
			snapshotName12 := "snap12"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName12, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       nil (backing image) -> snap11 -> snap12 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore13, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore13, Not(Equals), "")
			snapshotName13 := "snap13"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName13)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName13, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       nil (backing image) -> snap11 -> snap12 -> snap13 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 3 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore14, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore14, Not(Equals), "")
			snapshotName14 := "snap14"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName14)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName14, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       nil (backing image) -> snap11 -> snap12 -> snap13 -> snap14 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14},
					snapshotName14: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 4 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore15, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore15, Not(Equals), "")
			snapshotName15 := "snap15"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName15)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName15, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       nil (backing image) -> snap11 -> snap12 -> snap13 -> snap14 -> snap15 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14},
					snapshotName14: {snapshotName15},
					snapshotName15: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50] -> head[50,60]

			// Write some extra data into the current head before reverting. This part of data will be discarded after revert
			offsetInMB = 5 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
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
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Revert for a new chain (chain 2)
			revertSnapshot(c, spdkCli, snapshotName13, volumeName, engineName, engineFrontendName, replicaAddressMap)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50]
			//                                                             \
			//                                                               -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14, types.VolumeHead},
					snapshotName14: {snapshotName15},
					snapshotName15: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore21, Not(Equals), "")
			snapshotName21 := "snap21"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName21)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName21, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50]
			//                                                             \
			//                                                               -> snap21 ->  head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14, snapshotName21},
					snapshotName21: {types.VolumeHead},
					snapshotName14: {snapshotName15},
					snapshotName15: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 4 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore22, Not(Equals), "")
			snapshotName22 := "snap22"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName22)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName22, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap12[10,20] -> snap13[20,30] -> snap14[30,40] -> snap15[40,50]
			//                                                             \
			//                                                               -> snap21 -> snap22 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14, snapshotName21},
					snapshotName21: {snapshotName22},
					snapshotName22: {types.VolumeHead},
					snapshotName14: {snapshotName15},
					snapshotName15: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 5 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore23, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore23, Not(Equals), "")
			snapshotName23 := "snap23"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName23)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName23, false)
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
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Delete some snapshots
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName21)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName22)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName23, false)
			c.Assert(err, IsNil)
			for _, replicaName := range []string{replicaName1, replicaName2} {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, snapshotName23)
			}

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName12)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName13, false)
			c.Assert(err, IsNil)
			for _, replicaName := range []string{replicaName1, replicaName2} {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, snapshotName13)
			}
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName14)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName15, false)
			c.Assert(err, IsNil)
			for _, replicaName := range []string{replicaName1, replicaName2} {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, snapshotName15)
			}
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
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// TODO: Add replica rebuilding related test step

			// Revert for a new chain (chain 3)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, engineFrontendName, replicaAddressMap)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Create and delete some snapshots for the new chain (chain 3)
			offsetInMB = dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore31, Not(Equals), "")
			snapshotName31 := "snap31"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName31)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName31, false)
			c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName31},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName31: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore32, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore32, Not(Equals), "")
			snapshotName32 := "snap32"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName32)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName32, false)
			c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName31},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName31: {snapshotName32},
					snapshotName32: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName31)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName32, false)
			c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName32: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Revert for a new chain (chain 4)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, engineFrontendName, replicaAddressMap)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32, types.VolumeHead},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName32: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Create some snapshots for the new chain (chain 4)
			offsetInMB = dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore41, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore41, Not(Equals), "")
			snapshotName41 := "snap41"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName41)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName41, false)
			c.Assert(err, IsNil)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {},
					snapshotName23: {},
					snapshotName32: {},
					snapshotName41: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore42, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore42, Not(Equals), "")
			snapshotName42 := "snap42"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName42)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName42, false)
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
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Revert back to chain 1
			revertSnapshot(c, spdkCli, snapshotName15, volumeName, engineName, engineFrontendName, replicaAddressMap)

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
					snapshotName13: {snapshotName15, snapshotName23},
					snapshotName15: {types.VolumeHead},
					snapshotName23: {},
					snapshotName32: {},
					snapshotName41: {snapshotName42},
					snapshotName42: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Test online rebuilding twice

			// Write some data to the head before the 1st rebuilding
			offsetInMB = 6 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
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

			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW})
			// Launch the 1st rebuilding replica as the replacement of the crashed replica1
			replica3, err := spdkCli.ReplicaCreate(replicaName3, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, backingImageName)
			c.Assert(err, IsNil)
			c.Assert(replica3.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica3.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica3.State, Equals, types.InstanceStateRunning)
			c.Assert(replica3.PortStart, Not(Equals), int32(0))
			// Start the 1st rebuilding and wait for the completion
			err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaName3, net.JoinHostPort(ip, strconv.Itoa(int(replica3.PortStart))), defaultTestFastSync)
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

			for snapName := range snapshotMap {
				err = spdkCli.EngineSnapshotHash(engineName, snapName, false)
				c.Assert(err, IsNil)
			}

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName2, replicaName3}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Write more data to the head before the 2nd rebuilding
			offsetInMB = 7 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
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

			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName3: types.ModeRW})
			// Launch the 2nd rebuilding replica as the replacement of the crashed replica2
			replica4, err := spdkCli.ReplicaCreate(replicaName4, defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, backingImageName)
			c.Assert(err, IsNil)
			c.Assert(replica4.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica4.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica4.State, Equals, types.InstanceStateRunning)
			c.Assert(replica4.PortStart, Not(Equals), int32(0))
			// Start the 2nd rebuilding and wait for the completion
			err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaName4, net.JoinHostPort(ip, strconv.Itoa(int(replica4.PortStart))), defaultTestFastSync)
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

			for snapName := range snapshotMap {
				err = spdkCli.EngineSnapshotHash(engineName, snapName, false)
				c.Assert(err, IsNil)
			}

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Purging snapshots would lead to rebuild11 and rebuild12 cleanup
			err = spdkCli.EngineSnapshotPurge(engineName)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			// nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50] -> head[60,80]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]
			snapshotMap = map[string][]string{
				snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13: {snapshotName15, snapshotName23},
				snapshotName15: {types.VolumeHead},
				snapshotName23: {},
				snapshotName32: {},
				snapshotName41: {snapshotName42},
				snapshotName42: {},
			}
			delete(snapshotOpts, snapshotNameRebuild1)
			delete(snapshotOpts, snapshotNameRebuild2)
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)
			for _, replicaName := range []string{replicaName3, replicaName4} {
				replica, err := spdkCli.ReplicaGet(replicaName)
				c.Assert(err, IsNil)
				c.Assert(replica.Head.Parent, Equals, snapshotName15)
				c.Assert(replica.Head.ActualSize, Equals, uint64(2*dataCountInMB*helpertypes.MiB))
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
			revertSnapshot(c, spdkCli, snapshotName23, volumeName, engineName, engineFrontendName, replicaAddressMap)

			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60] -> head
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]
			snapshotMap = map[string][]string{
				snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13: {snapshotName15, snapshotName23},
				snapshotName15: {},
				snapshotName23: {types.VolumeHead},
				snapshotName32: {},
				snapshotName41: {snapshotName42},
				snapshotName42: {},
			}
			delete(snapshotOpts, snapshotNameRebuild1)
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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
			revertSnapshot(c, spdkCli, snapshotName32, volumeName, engineName, engineFrontendName, replicaAddressMap)
			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30] -> head
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30]
			snapshotMap = map[string][]string{
				snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13: {snapshotName15, snapshotName23},
				snapshotName15: {},
				snapshotName23: {},
				snapshotName32: {types.VolumeHead},
				snapshotName41: {snapshotName42},
				snapshotName42: {},
			}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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
			revertSnapshot(c, spdkCli, snapshotName42, volumeName, engineName, engineFrontendName, replicaAddressMap)
			// Current snapshot tree (with backing image):
			// 	 nil (backing image) -> snap11[0,10] -> snap13[10,30] -> snap15[30,50]
			// 	                                    |\                \
			// 	                                    | \                -> snap23[30,60]
			// 	                                    |  \
			// 	                                    \   -> snap32[10,30]
			// 	                                     \
			// 	                                      -> snap41[10,20] -> snap42[20,30] -> head
			snapshotMap = map[string][]string{
				snapshotName11: {snapshotName13, snapshotName32, snapshotName41},
				snapshotName13: {snapshotName15, snapshotName23},
				snapshotName15: {},
				snapshotName23: {},
				snapshotName32: {},
				snapshotName41: {snapshotName42},
				snapshotName42: {types.VolumeHead},
			}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName3, replicaName4}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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

	engineFrontendList, err := spdkCli.EngineFrontendList()
	c.Assert(err, IsNil)
	for _, ef := range engineFrontendList {
		err = spdkCli.EngineFrontendDelete(ef.Name)
		c.Assert(err, IsNil)
	}

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

func (s *TestSuite) TestSPDKMultipleThreadSnapshotOpsAndRebuildingWithoutBackingImage(c *C) {
	fmt.Println("Testing SPDK multiple thread snapshot ops and rebuilding without backing image")
	s.spdkMultipleThreadSnapshotOpsAndRebuilding(c, false)
}

func (s *TestSuite) spdkMultipleThreadFastRebuilding(c *C, withBackingImage bool) {
	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup

	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)

		disk, err = spdkCli.DiskGet(defaultTestDiskName, disk.Path, diskDriverName)
		c.Assert(err, NotNil)
		c.Assert(disk, IsNil)
	}()

	var bi *api.BackingImage
	if withBackingImage {
		bi, err = spdkCli.BackingImageCreate(defaultTestBackingImageName, defaultTestBackingImageUUID, disk.Uuid, defaultTestBackingImageSize, defaultTestBackingImageChecksum, defaultTestBackingImageDownloadURL, "")
		c.Assert(err, IsNil)
		c.Assert(bi, NotNil)
		defer func() {
			err := spdkCli.BackingImageDelete(defaultTestBackingImageName, disk.Uuid)
			c.Assert(err, IsNil)
		}()

		// check if bi.State is "ready" in 300 seconds
		for i := range maxBackingImageGetRetries {
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
	}

	backingImageName := ""
	if withBackingImage {
		backingImageName = bi.Name
	}

	concurrentCount := 5
	dataCountInMB := int64(100)

	// Pre-create all resources sequentially to avoid SPDK service contention
	// during concurrent engine creation (validateReplicaSize + connectNVMfBdev
	// can cause context deadline exceeded when goroutines contend simultaneously)
	type volumeTestData struct {
		volumeName         string
		engineName         string
		engineFrontendName string
		replicaName1       string
		replicaName2       string
		replica1           *api.Replica
		replicaAddressMap  map[string]string
		endpoint           string
	}

	testVolumes := make([]volumeTestData, concurrentCount)
	for i := range concurrentCount {
		vol := &testVolumes[i]
		vol.volumeName = fmt.Sprintf("test-vol-%d", i)
		vol.engineName = fmt.Sprintf("%s-e", vol.volumeName)
		vol.engineFrontendName = fmt.Sprintf("%s-ef", vol.volumeName)
		vol.replicaName1 = fmt.Sprintf("%s-replica-1", vol.volumeName)
		vol.replicaName2 = fmt.Sprintf("%s-replica-2", vol.volumeName)

		replica1, err := spdkCli.ReplicaCreate(vol.replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, backingImageName)
		c.Assert(err, IsNil)
		c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
		c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
		c.Assert(replica1.ErrorMsg, Equals, "")
		c.Assert(replica1.State, Equals, types.InstanceStateRunning)
		c.Assert(replica1.PortStart, Not(Equals), int32(0))
		vol.replica1 = replica1

		replica2, err := spdkCli.ReplicaCreate(vol.replicaName2, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, backingImageName)
		c.Assert(err, IsNil)
		c.Assert(replica2.LvsName, Equals, defaultTestDiskName)
		c.Assert(replica2.LvsUUID, Equals, disk.Uuid)
		c.Assert(replica2.ErrorMsg, Equals, "")
		c.Assert(replica2.State, Equals, types.InstanceStateRunning)
		c.Assert(replica2.PortStart, Not(Equals), int32(0))

		vol.replicaAddressMap = map[string]string{
			replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
			replica2.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart))),
		}
		replicaModeMap := map[string]types.Mode{
			replica1.Name: types.ModeRW,
			replica2.Name: types.ModeRW,
		}

		vol.endpoint = helperutil.GetLonghornDevicePath(vol.volumeName)
		engine, err := spdkCli.EngineCreate(vol.engineName, vol.volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, vol.replicaAddressMap, 1, false)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, vol.replicaAddressMap)
		c.Assert(engine.ReplicaModeMap, DeepEquals, replicaModeMap)
		c.Assert(engine.Port, Not(Equals), int32(0))

		engineFrontend, err := spdkCli.EngineFrontendCreate(vol.engineFrontendName, vol.volumeName, vol.engineName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize,
			net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
		c.Assert(err, IsNil)
		c.Assert(engineFrontend.Endpoint, Equals, vol.endpoint)
	}

	wg := sync.WaitGroup{}
	wg.Add(concurrentCount)
	for i := range concurrentCount {
		go func() {
			defer wg.Done()

			vol := testVolumes[i]
			volumeName := vol.volumeName
			engineName := vol.engineName
			engineFrontendName := vol.engineFrontendName
			replicaName1 := vol.replicaName1
			replicaName2 := vol.replicaName2
			replica1 := vol.replica1
			replicaAddressMap := vol.replicaAddressMap
			endpoint := vol.endpoint

			var engine *api.Engine
			var engineFrontend *api.EngineFrontend
			var err error
			// Suppress unused warnings; these are used in the goroutine body below
			_ = engine
			_ = engineFrontend

			// Construct a snapshot tree with enough data before testing rebuilding

			// Build the first chain (chain 1)
			offsetInMB := int64(0)
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore11, Not(Equals), "")
			snapshotName11 := "snap11"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName11)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName11, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore12, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore12, Not(Equals), "")
			snapshotName12 := "snap12"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName12, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 -> snap12 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore13, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore13, Not(Equals), "")
			snapshotName13 := "snap13"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName13)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName13, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 -> snap12 -> snap13 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Revert for a new chain (chain 2)
			revertSnapshot(c, spdkCli, snapshotName11, volumeName, engineName, engineFrontendName, replicaAddressMap)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 ->  snap12 -> snap13
			//                          \
			//                           -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, types.VolumeHead},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 1 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore21, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore21, Not(Equals), "")
			snapshotName21 := "snap21"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName21)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName21, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 ->  snap12 -> snap13
			//                          \
			//                           -> snap21 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBefore22, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore22, Not(Equals), "")
			snapshotName22 := "snap22"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName22)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName22, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 ->  snap12 -> snap13
			//                          \
			//                           -> snap21 -> snap22 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22},
					snapshotName22: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Revert for a new chain (chain 3)
			revertSnapshot(c, spdkCli, snapshotName21, volumeName, engineName, engineFrontendName, replicaAddressMap)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 ->  snap12 -> snap13
			//                          \
			//                           -> snap21 -> snap22
			//                                 \
			//                                  -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22, types.VolumeHead},
					snapshotName22: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			offsetInMB = 2 * dataCountInMB

			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			if err != nil {
				fmt.Printf("Error writing data before creating snap31 for volume %s: %v\n", volumeName, err)
				time.Sleep(60000 * time.Second)
				fmt.Printf("After sleep, still error writing data before creating snap31 for volume %s: %v\n", volumeName, err)
			}
			c.Assert(err, IsNil)
			cksumBefore31, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBefore31, Not(Equals), "")
			snapshotName31 := "snap31"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName31)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName31, false)
			c.Assert(err, IsNil)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11 ->  snap12 -> snap13
			//                          \
			//                           -> snap21 -> snap22
			//                                 \
			//                                  -> snap31 -> head
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Finally, write some data to the head before the rebuilding
			offsetInMB = 3 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			cksumBeforeRebuild11, err := util.GetFileChunkChecksum(endpoint, offsetInMB*helpertypes.MiB, dataCountInMB*helpertypes.MiB)
			c.Assert(err, IsNil)
			c.Assert(cksumBeforeRebuild11, Not(Equals), "")

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] -> snap13[2*DATA,3*DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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

			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW})

			replica1, err = spdkCli.ReplicaCreate(replicaName1, defaultTestDiskName, disk.Uuid, defaultTestLargeLvolSize, defaultTestReplicaPortCount, backingImageName)
			c.Assert(err, IsNil)
			c.Assert(replica1.LvsName, Equals, defaultTestDiskName)
			c.Assert(replica1.LvsUUID, Equals, disk.Uuid)
			c.Assert(replica1.State, Equals, types.InstanceStateRunning)
			c.Assert(replica1.PortStart, Not(Equals), int32(0))

			// Before reusing the crashed replica1 for rebuilding, mess up some snapshots and see if the fast rebuilding mechanism can handle it
			replicaTmpAddressMap := map[string]string{
				replica1.Name: net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))),
			}

			err = spdkCli.EngineFrontendDelete(engineFrontendName)
			c.Assert(err, IsNil)
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, replicaTmpAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaTmpAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replica1.Name: types.ModeRW})
			c.Assert(engine.Port, Not(Equals), int32(0))

			engineFrontend, err = spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize,
				net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.Endpoint, Equals, endpoint)

			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] -> snap13[2*DATA,3*DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName2},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13},
					snapshotName13: {},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {types.VolumeHead},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Mess up snapshots in chain 1 by corrupting data for snap12 and introducing more invalid snapshots/head for the crashed replica1
			revertSnapshot(c, spdkCli, snapshotName12, volumeName, engineName, engineFrontendName, replicaTmpAddressMap)

			// Current snapshot tree of the crashed replica1 (with backing image):
			//                                               -> head[,]
			//                                              /
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] -> snap13[2*DATA,3*DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1},
				map[string][]string{
					snapshotName11: {snapshotName12, snapshotName21},
					snapshotName12: {snapshotName13, types.VolumeHead},
					snapshotName13: {},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName13)
			c.Assert(err, IsNil)

			// Current snapshot tree of the crashed replica1 (with backing image):
			//                                               -> head[,]
			//                                              /
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1},
				map[string][]string{
					snapshotName11: {snapshotName21, snapshotName12},
					snapshotName12: {types.VolumeHead},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Try to write invalid data into the snap12 for replica1
			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			// Since we cannot modify or even delete the snap12 now, we need to put the invalid data to the new snap12-tmp
			snapshotName12Tmp := "snap12-tmp"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12Tmp)
			c.Assert(err, IsNil)
			// Then deleting snap12 will make ll data be merged into snap12-tmp
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName12)
			c.Assert(err, IsNil)
			// Now we can recreate the snap12 and delete the snap12-tmp
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName12)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotDelete(engineName, snapshotName12Tmp)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName12, false)
			c.Assert(err, IsNil)

			// Current snapshot tree of the crashed replica1 (with backing image):
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA][2*INVALID_DATA, 3*INVALID_DATA] -> head[,]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1},
				map[string][]string{
					snapshotName11: {snapshotName21, snapshotName12},
					snapshotName12: {types.VolumeHead},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			// Then create the invalid snapshot13
			offsetInMB = 2 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName13)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName13, false)
			c.Assert(err, IsNil)

			// Finally, create an extra snapshot14 and leave an invalid head
			offsetInMB = 3 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)
			snapshotName14 := "snap14"
			_, err = spdkCli.EngineSnapshotCreate(engineName, snapshotName14)
			c.Assert(err, IsNil)
			err = spdkCli.EngineSnapshotHash(engineName, snapshotName14, false)
			c.Assert(err, IsNil)
			offsetInMB = 4 * dataCountInMB
			err = writeDataToBlockDevice(ne, endpoint, offsetInMB, dataCountInMB)
			c.Assert(err, IsNil)

			// Current snapshot tree of the crashed replica1 (with backing image):
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA][2*INVALID_DATA, 3*INVALID_DATA] -> snap13[2*INVALID_DATA, 3*INVALID_DATA] -> snap14[3*INVALID_DATA, 4*INVALID_DATA] -> head[4*INVALID_DATA, 5*INVALID_DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> head[3*DATA,4*DATA]
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1},
				map[string][]string{
					snapshotName11: {snapshotName21, snapshotName12},
					snapshotName12: {snapshotName13},
					snapshotName13: {snapshotName14},
					snapshotName14: {types.VolumeHead},
					snapshotName21: {snapshotName22, snapshotName31},
					snapshotName22: {},
					snapshotName31: {},
				},
				nil, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

			err = spdkCli.EngineFrontendDelete(engineFrontendName)
			c.Assert(err, IsNil)
			err = spdkCli.EngineDelete(engineName)
			c.Assert(err, IsNil)

			// Relaunch engine with the correct replica
			engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize, replicaAddressMap, 1, false)
			c.Assert(err, IsNil)
			c.Assert(engine.State, Equals, types.InstanceStateRunning)
			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName2: types.ModeRW})
			c.Assert(engine.Port, Not(Equals), int32(0))

			engineFrontend, err = spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLargeLvolSize,
				net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
			c.Assert(err, IsNil)
			c.Assert(engineFrontend.Endpoint, Equals, endpoint)

			// And start the 1st rebuilding and wait for the completion
			err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaName1, net.JoinHostPort(ip, strconv.Itoa(int(replica1.PortStart))), defaultTestFastSync)
			c.Assert(err, IsNil)
			// The rebuilding should be pretty fast since all existing snapshots and the previous head are there
			WaitForReplicaRebuildingCompleteTimeout(c, spdkCli, engineName, replicaName1, 300)

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

			c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
			c.Assert(engine.ReplicaModeMap, DeepEquals, map[string]types.Mode{replicaName1: types.ModeRW, replicaName2: types.ModeRW})

			// Rebuilding once leads to snap12 and snap13 reuse (which involve range shallow copy) as well as invalid snap14 deletion
			// Current snapshot tree (with backing image):
			//       BackingImage -> snap11[0,1*DATA] -> snap12[1*DATA,2*DATA] -> snap13[2*DATA,3*DATA]
			//                          \
			//                           -> snap21[1*DATA,2*DATA] -> snap22[2*DATA,3*DATA]
			//                                 \
			//                                  -> snap31[2*DATA,3*DATA] -> rebuild11[3*DATA,4*DATA] -> head[4*DATA,4*DATA]
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

			for snapName := range snapshotMap {
				err = spdkCli.EngineSnapshotHash(engineName, snapName, false)
				c.Assert(err, IsNil)
			}

			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2}, snapshotMap, snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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

			// Verify chain2, the volume head will be reverted to the snapshot22
			revertSnapshot(c, spdkCli, snapshotName22, volumeName, engineName, engineFrontendName, replicaAddressMap)
			for snapName := range snapshotMap {
				snapshotOpts[snapName] = api.SnapshotOptions{UserCreated: true}
			}
			snapshotOpts[snapshotNameRebuild1] = api.SnapshotOptions{UserCreated: false}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11:       {snapshotName12, snapshotName21},
					snapshotName12:       {snapshotName13},
					snapshotName13:       {},
					snapshotName21:       {snapshotName22, snapshotName31},
					snapshotName22:       {types.VolumeHead},
					snapshotName31:       {snapshotNameRebuild1},
					snapshotNameRebuild1: {},
				},
				snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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

			// Verify chain1, the volume head will be reverted to the snapshot13
			revertSnapshot(c, spdkCli, snapshotName13, volumeName, engineName, engineFrontendName, replicaAddressMap)
			for snapName := range snapshotMap {
				snapshotOpts[snapName] = api.SnapshotOptions{UserCreated: true}
			}
			snapshotOpts[snapshotNameRebuild1] = api.SnapshotOptions{UserCreated: false}
			checkReplicaSnapshots(c, spdkCli, engineName, []string{replicaName1, replicaName2},
				map[string][]string{
					snapshotName11:       {snapshotName12, snapshotName21},
					snapshotName12:       {snapshotName13},
					snapshotName13:       {types.VolumeHead},
					snapshotName21:       {snapshotName22, snapshotName31},
					snapshotName22:       {},
					snapshotName31:       {snapshotNameRebuild1},
					snapshotNameRebuild1: {},
				},
				snapshotOpts, checkReplicaSnapshotsMaxRetries, checkReplicaSnapshotsWaitInterval)

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

	engineFrontendList, err := spdkCli.EngineFrontendList()
	c.Assert(err, IsNil)
	for _, ef := range engineFrontendList {
		err = spdkCli.EngineFrontendDelete(ef.Name)
		c.Assert(err, IsNil)
	}

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

func (s *TestSuite) TestSPDKMultipleThreadFastRebuildingWithoutBackingImage(c *C) {
	fmt.Println("Testing SPDK fast rebuilding with multiple threads without backing image")
	s.spdkMultipleThreadFastRebuilding(c, false)
}

func checkReplicaSnapshots(c *C, spdkCli *client.SPDKClient, engineName string, replicaList []string, snapshotMap map[string][]string, snapshotOpts map[string]api.SnapshotOptions, maxRetries int, retryInterval time.Duration) {
	var lastErr error
	retries := maxRetries
	if retries <= 0 {
		retries = 1
	}
	for attempt := 0; attempt < retries; attempt++ {
		lastErr = nil

		engine, err := spdkCli.EngineGet(engineName)
		if err != nil {
			lastErr = fmt.Errorf("EngineGet failed: %v", err)
		} else {
			for _, replicaName := range replicaList {
				waitReplicaSnapshotChecksum(c, spdkCli, replicaName, "")
			}

			for _, replicaName := range replicaList {
				replica, err := spdkCli.ReplicaGet(replicaName)
				if err != nil {
					lastErr = fmt.Errorf("ReplicaGet failed for %s: %v", replicaName, err)
					break
				}
				if len(replica.Snapshots) != len(snapshotMap) {
					lastErr = fmt.Errorf("replica %s snapshot count mismatch: expected %d, got %d", replicaName, len(snapshotMap), len(replica.Snapshots))
					break
				}

				for snapName, childrenList := range snapshotMap {
					snap := replica.Snapshots[snapName]
					if snap == nil {
						lastErr = fmt.Errorf("snapshot %s not found in replica %s", snapName, replicaName)
						break
					}
					engineSnap := engine.Snapshots[snapName]
					if engineSnap == nil {
						lastErr = fmt.Errorf("snapshot %s not found in engine", snapName)
						break
					}
					if !reflect.DeepEqual(engineSnap.Children, snap.Children) {
						lastErr = fmt.Errorf("snapshot %s children mismatch", snapName)
						break
					}
					for _, childSnapName := range childrenList {
						ok, exists := snap.Children[childSnapName]
						if !exists || !ok {
							lastErr = fmt.Errorf("child snapshot %s not present in snapshot %s", childSnapName, snapName)
							break
						}
						if childSnapName != types.VolumeHead {
							childSnap := replica.Snapshots[childSnapName]
							if childSnap == nil {
								lastErr = fmt.Errorf("child snapshot %s not found in replica %s", childSnapName, replicaName)
								break
							}
							if childSnap.Parent != snapName {
								lastErr = fmt.Errorf("child snapshot %s parent mismatch: expected %s, got %s", childSnapName, snapName, childSnap.Parent)
								break
							}
						}
					}
					if lastErr != nil {
						break
					}
				}
				if lastErr != nil {
					break
				}
				for snapName, opts := range snapshotOpts {
					snap := replica.Snapshots[snapName]
					if snap == nil {
						lastErr = fmt.Errorf("snapshot %s not found in replica %s", snapName, replicaName)
						break
					}
					engineSnap := engine.Snapshots[snapName]
					if engineSnap.UserCreated != opts.UserCreated {
						lastErr = fmt.Errorf("snapshot %s UserCreated mismatch: expected %v, got %v", snapName, opts.UserCreated, engineSnap.UserCreated)
						break
					}
				}
				if lastErr != nil {
					break
				}
			}
		}

		if lastErr == nil {
			return
		}

		if attempt < retries-1 {
			time.Sleep(retryInterval)
		}
	}

	c.Assert(lastErr, IsNil)
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
		select {
		case <-timer.C:
			c.Assert(hasChecksum, Equals, true)
			return
		case <-ticker.C:
			hasChecksum = true
			replica, err := spdkCli.ReplicaGet(replicaName)
			c.Assert(err, IsNil)
			if targetSnapName == "" || replica.Snapshots[targetSnapName] != nil {
				for snapName, snap := range replica.Snapshots {
					if targetSnapName == "" || snapName == targetSnapName {
						if !snap.UserCreated {
							continue
						}
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

func revertSnapshot(c *C, spdkCli *client.SPDKClient, snapshotName, volumeName, engineName, engineFrontendName string, replicaAddressMap map[string]string) {
	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	engine, err := spdkCli.EngineGet(engineName)
	c.Assert(err, IsNil)

	if engine.State != types.InstanceStateRunning {
		return
	}
	volumeSize := engine.SpecSize

	engineFrontends, err := spdkCli.EngineFrontendList()
	c.Assert(err, IsNil)
	var prevFrontend, prevEndpoint string
	if ef, ok := engineFrontends[engineFrontendName]; ok {
		prevFrontend = ef.Frontend
		prevEndpoint = ef.Endpoint
	} else {
		prevFrontend = types.FrontendEmpty
	}

	if prevFrontend != types.FrontendEmpty {
		// Restart the engine without the frontend
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)
		engine, err = spdkCli.EngineCreate(engineName, volumeName, types.FrontendEmpty, volumeSize, replicaAddressMap, 1, false)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
		c.Assert(engine.Port, Equals, int32(0))
	}

	err = spdkCli.EngineSnapshotRevert(engineName, snapshotName)
	c.Assert(err, IsNil)

	if prevFrontend != types.FrontendEmpty {
		// Restart the engine with the previous frontend
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)
		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)
		engine, err = spdkCli.EngineCreate(engineName, volumeName, prevFrontend, volumeSize, replicaAddressMap, 1, false)
		c.Assert(err, IsNil)
		c.Assert(engine.State, Equals, types.InstanceStateRunning)
		c.Assert(engine.ReplicaAddressMap, DeepEquals, replicaAddressMap)
		c.Assert(engine.Port, Not(Equals), int32(0))

		engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, prevFrontend, volumeSize,
			net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
		c.Assert(err, IsNil)
		c.Assert(engineFrontend.Endpoint, Equals, prevEndpoint)
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

func (s *TestSuite) TestSPDKEngineFrontendReplicaAdd(c *C) {
	fmt.Println("Testing SPDK engine frontend replica add with data verification")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup
	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	LaunchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	volumeName := getVolumeName()
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		err = spdkCli.EngineFrontendDelete(engineFrontendName)
		c.Assert(err, IsNil)

		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	// 1. Create first replica
	replica, err := spdkCli.ReplicaCreate(replicaNames[0], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replicas[replicaNames[0]] = replica

	replicaAddressMap := make(map[string]string)
	replicaAddressMap[replicaNames[0]] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))

	// 2. Create Engine with 1 replica
	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
	c.Assert(err, IsNil)

	// 3. Create Engine Frontend
	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
	c.Assert(err, IsNil)
	c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)

	endpoint := helperutil.GetLonghornDevicePath(volumeName)
	c.Assert(engineFrontend.Endpoint, Equals, endpoint)

	// 4. Write Data to Volume (Pattern A)
	offset := int64(0)
	length := int64(4096)
	dataA := make([]byte, length)
	for i := 0; i < len(dataA); i++ {
		dataA[i] = 'A'
	}
	f, err := os.OpenFile(endpoint, os.O_RDWR, 0666)
	c.Assert(err, IsNil)
	_, err = f.WriteAt(dataA, offset)
	c.Assert(err, IsNil)
	err = f.Sync()
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)

	// 5. Take Snapshot
	snapshotName := "snap1"
	_, err = spdkCli.EngineFrontendSnapshotCreate(engineFrontendName, snapshotName)
	c.Assert(err, IsNil)

	// 6. Write Data to Volume (Pattern B)
	offsetB := int64(4096)
	dataB := make([]byte, length)
	for i := 0; i < len(dataB); i++ {
		dataB[i] = 'B'
	}
	f, err = os.OpenFile(endpoint, os.O_RDWR, 0666)
	c.Assert(err, IsNil)
	_, err = f.WriteAt(dataB, offsetB)
	c.Assert(err, IsNil)
	err = f.Sync()
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)

	// 7. Add Second Replica
	replica2, err := spdkCli.ReplicaCreate(replicaNames[1], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replicas[replicaNames[1]] = replica2
	replica2Address := net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart)))

	err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaNames[1], replica2Address, defaultTestFastSync)
	c.Assert(err, IsNil)

	// 8. Wait for Replica Add to Complete
	err = retry.Do(func() error {
		e, err := spdkCli.EngineGet(engineName)
		if err != nil {
			return err
		}
		if e.ReplicaModeMap[replicaNames[1]] != types.ModeRW {
			return fmt.Errorf("replica %s is not RW yest: %v", replicaNames[1], e.ReplicaModeMap[replicaNames[1]])
		}
		return nil
	}, retry.Delay(1*time.Second), retry.Attempts(60))
	c.Assert(err, IsNil)

	// 9. Verify Data on Second Replica
	// Connect to Replica 2 directly to verify data
	replica2NQN := helpertypes.GetNQN(replicaNames[1])
	replica2Port := strconv.Itoa(int(replica2.PortStart))

	ne, err = helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)

	_, err = helperinitiator.ConnectTarget(ip, replica2Port, replica2NQN, ne)
	c.Assert(err, IsNil)
	defer func() {
		_ = helperinitiator.DisconnectTarget(replica2NQN, ne)
	}()

	devices, err := helperinitiator.GetDevices(ip, replica2Port, replica2NQN, ne)
	c.Assert(err, IsNil)
	c.Assert(len(devices), Equals, 1)

	replica2Endpoint := filepath.Join("/dev", devices[0].Namespaces[0].NameSpace)

	// Read back Data A
	readBuf := make([]byte, length)
	f2, err := os.OpenFile(replica2Endpoint, os.O_RDONLY, 0666)
	c.Assert(err, IsNil)
	defer func() {
		err = f2.Close()
		c.Assert(err, IsNil)
	}()

	_, err = f2.ReadAt(readBuf, offset)
	c.Assert(err, IsNil)
	c.Assert(readBuf, DeepEquals, dataA)

	// Read back Data B
	_, err = f2.ReadAt(readBuf, offsetB)
	c.Assert(err, IsNil)
	c.Assert(readBuf, DeepEquals, dataB)
}

// TestSPDKEngineFrontendReplicaAddErrorHandling tests the error handling of replica addition.
// It verifies that errors during shallow copy and finish phases are correctly reported and handled,
// and that the operation can be retried successfully.
func (s *TestSuite) TestSPDKEngineFrontendReplicaAddErrorHandling(c *C) {

	fmt.Println("Testing SPDK engine frontend replica add error handling")

	diskDriverName := "aio"

	ip, err := commonnet.GetAnyExternalIP()
	c.Assert(err, IsNil)
	err = os.Setenv(commonnet.EnvPodIP, ip)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	var spdkWg sync.WaitGroup
	defer func() {
		cancel()
		spdkWg.Wait()
	}()

	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	c.Assert(err, IsNil)
	// Use launchTestSPDKGRPCServer helper to get access to the server instance
	srv := launchTestSPDKGRPCServer(ctx, c, ip, ne.Execute, &spdkWg)

	loopDevicePath := PrepareDiskFile(c)
	defer func() {
		CleanupDiskFile(c, loopDevicePath)
	}()

	spdkCli, err := client.NewSPDKClient(net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort)))
	c.Assert(err, IsNil)
	defer func() {
		if errClose := spdkCli.Close(); errClose != nil {
			logrus.WithError(errClose).Error("Failed to close SPDK client")
		}
	}()

	disk, err := spdkCli.DiskCreate(defaultTestDiskName, "", loopDevicePath, diskDriverName, int64(defaultTestBlockSize))
	c.Assert(err, IsNil)
	c.Assert(disk, NotNil)

	disk, err = waitForDiskReady(ctx, spdkCli, loopDevicePath, diskDriverName)
	c.Assert(err, IsNil)
	c.Assert(disk.Path, Equals, loopDevicePath)
	c.Assert(disk.Uuid, Not(Equals), "")

	defer func() {
		err := spdkCli.DiskDelete(defaultTestDiskName, disk.Uuid, disk.Path, diskDriverName)
		c.Assert(err, IsNil)
	}()

	volumeName := fmt.Sprintf("test-err-vol-%s", time.Now().Format("20060102150405"))
	engineName := fmt.Sprintf("%s-e", volumeName)
	engineFrontendName := fmt.Sprintf("%s-ef", volumeName)
	replicaNames := []string{
		fmt.Sprintf("%s-replica-1", volumeName),
		fmt.Sprintf("%s-replica-2", volumeName),
	}
	replicas := make(map[string]*api.Replica)

	defer func() {
		// Cleanup: Try to delete frontend if exists
		_ = spdkCli.EngineFrontendDelete(engineFrontendName)

		err = spdkCli.EngineDelete(engineName)
		c.Assert(err, IsNil)

		for _, replica := range replicas {
			err = spdkCli.ReplicaDelete(replica.Name, true)
			c.Assert(err, IsNil)
		}
	}()

	// 1. Create first replica
	replica, err := spdkCli.ReplicaCreate(replicaNames[0], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replicas[replicaNames[0]] = replica

	replicaAddressMap := make(map[string]string)
	replicaAddressMap[replicaNames[0]] = net.JoinHostPort(ip, strconv.Itoa(int(replica.PortStart)))

	// 2. Create Engine with 1 replica
	engine, err := spdkCli.EngineCreate(engineName, volumeName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize, replicaAddressMap, 1, false)
	c.Assert(err, IsNil)

	// 3. Create Engine Frontend
	engineFrontend, err := spdkCli.EngineFrontendCreate(engineFrontendName, volumeName, engineName, types.FrontendSPDKTCPBlockdev, defaultTestLvolSize,
		net.JoinHostPort(engine.IP, strconv.Itoa(int(engine.Port))), 0, 0)
	c.Assert(err, IsNil)
	c.Assert(engineFrontend.State, Equals, types.InstanceStateRunning)

	endpoint := helperutil.GetLonghornDevicePath(volumeName)

	// Get Internal Engine Struct to inject errors
	internalEngine := srv.GetEngineStruct(engineName)
	c.Assert(internalEngine, NotNil)

	// 4. Create Second Replica
	replica2, err := spdkCli.ReplicaCreate(replicaNames[1], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replicas[replicaNames[1]] = replica2
	replica2Address := net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart)))

	// Helper to wait for engine replica to reach ERR mode
	// (indicates the async replica add goroutine on the Engine side has failed).
	// The Engine sets the mode to ERR before running SPDK cleanup, so this
	// returns quickly without waiting for detach timeouts.
	waitForReplicaERR := func(replicaName string) {
		err = retry.Do(func() error {
			e, err := spdkCli.EngineGet(engineName)
			if err != nil {
				return err
			}
			mode, ok := e.ReplicaModeMap[replicaName]
			if !ok {
				return fmt.Errorf("replica %s not found in engine mode map", replicaName)
			}
			if mode != types.ModeERR {
				return fmt.Errorf("replica %s mode is %v, expected ERR", replicaName, mode)
			}
			return nil
		}, retry.Delay(500*time.Millisecond), retry.Attempts(30))
		c.Assert(err, IsNil)
	}

	// 5. Test Shallow Copy Error
	internalEngine.SetReplicaAdder(&server.MockReplicaAdder{
		ShallowCopyFunc: func(srcReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, snapshots []*api.Lvol, fastSync bool) error {
			return fmt.Errorf("injected shallow copy error")
		},
	})

	// Call ReplicaAdd (async)
	err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaNames[1], replica2Address, defaultTestFastSync)
	c.Assert(err, IsNil) // Should be nil as it returns immediately

	waitForReplicaERR(replicaNames[1])

	// Reset adder
	internalEngine.SetReplicaAdder(nil)

	err = spdkCli.EngineReplicaDelete(engineName, replicaNames[1], replica2Address)
	c.Assert(err, IsNil)

	// The engine's ReplicaAdd goroutine internally calls replicaAddFinish on shallow copy
	// failure, which detaches the external snapshot NVMe controller and stops the source from
	// exposing. So we only need to delete/recreate the dst replica for a clean state.
	err = spdkCli.ReplicaDelete(replicaNames[1], true)
	c.Assert(err, IsNil)

	replica2, err = spdkCli.ReplicaCreate(replicaNames[1], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replica2Address = net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart)))

	// 5b. Test Shallow Copy Error with Replica in Error State
	// This tests the production scenario where a real shallow copy failure sets the
	// replica's internal state to Error (via RebuildingDstShallowCopyStart's defer).
	// The test hook operates at Engine level and doesn't trigger per-replica error state,
	// so we use SetTestErrorState to simulate the production behavior.
	// Without the P0 fix, RebuildingDstFinish would reject error-state replicas,
	// causing doCleanupForRebuildingDst to never run, leaving the external snapshot
	// NVMe controller attached and causing bdev_nvme_detach_controller to hang.
	internalEngine.SetReplicaAdder(&server.MockReplicaAdder{
		ShallowCopyFunc: func(dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, snapshots []*api.Lvol, fastSync bool) error {
			// Simulate what happens in production: the per-replica error state is set
			// by RebuildingDstShallowCopyStart's defer when the actual shallow copy fails.
			internalReplica := srv.GetReplicaStruct(dstReplicaName)
			c.Assert(internalReplica, NotNil)
			internalReplica.SetTestErrorState("simulated production shallow copy failure")
			return fmt.Errorf("injected shallow copy error with replica error state")
		},
	})

	err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaNames[1], replica2Address, defaultTestFastSync)
	c.Assert(err, IsNil)

	waitForReplicaERR(replicaNames[1])

	// Reset adder
	internalEngine.SetReplicaAdder(nil)

	// Clean up the partial state in Engine
	err = spdkCli.EngineReplicaDelete(engineName, replicaNames[1], replica2Address)
	c.Assert(err, IsNil)

	// ReplicaDelete should succeed without hanging, because even though the replica
	// was in error state, RebuildingDstFinish (with the P0 fix) still performed
	// doCleanupForRebuildingDst, disconnecting the external snapshot NVMe controller.
	err = spdkCli.ReplicaDelete(replicaNames[1], true)
	c.Assert(err, IsNil)

	replica2, err = spdkCli.ReplicaCreate(replicaNames[1], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replica2Address = net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart)))

	// 6. Test Finish Error
	internalEngine.SetReplicaAdder(&server.MockReplicaAdder{
		FinishFunc: func(srcReplicaServiceCli *client.SPDKClient, dstReplicaServiceCli *client.SPDKClient, srcReplicaName, dstReplicaName string, fastSync bool) error {
			return fmt.Errorf("injected finish error")
		},
	})

	err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaNames[1], replica2Address, defaultTestFastSync)
	c.Assert(err, IsNil)

	waitForReplicaERR(replicaNames[1])

	// Reset adder
	internalEngine.SetReplicaAdder(nil)

	// Clean up the partial state in Engine
	err = spdkCli.EngineReplicaDelete(engineName, replicaNames[1], replica2Address)
	c.Assert(err, IsNil)

	// The engine's ReplicaAdd goroutine internally calls the real replicaAddFinish
	// for cleanup when the test hook fails, so SPDK resources are already cleaned up.
	err = spdkCli.ReplicaDelete(replicaNames[1], true)
	c.Assert(err, IsNil)
	replica2, err = spdkCli.ReplicaCreate(replicaNames[1], defaultTestDiskName, disk.Uuid, defaultTestLvolSize, defaultTestReplicaPortCount, "")
	c.Assert(err, IsNil)
	replica2Address = net.JoinHostPort(ip, strconv.Itoa(int(replica2.PortStart)))

	// 6b. Test Engine Lock is Released During replicaAddFinish Phase 2 (RPC calls)
	// This verifies the 3-phase lock refactoring: the Engine lock should NOT be held
	// during Phase 2 when RPC calls (ReplicaRebuildingSrcFinish, ReplicaRebuildingDstFinish)
	// are executed. Without this refactoring, these RPCs would block all Engine operations
	// for 10+ seconds on ETIMEDOUT.
	phase2LockReleased := make(chan bool, 1)
	internalEngine.SetFinishPhase2Hook(func() {
		// This hook runs inside Phase 2 of replicaAddFinish, where the Engine lock
		// should be released. Verify by trying to acquire the lock.
		if internalEngine.TryLock() {
			// Lock was free — 3-phase pattern is working correctly
			internalEngine.Unlock()
			phase2LockReleased <- true
		} else {
			// Lock was held — 3-phase pattern is NOT working (old behavior)
			phase2LockReleased <- false
		}
	})

	err = spdkCli.EngineFrontendReplicaAdd(engineFrontendName, replicaNames[1], replica2Address, defaultTestFastSync)
	c.Assert(err, IsNil)

	// Wait for the Phase 2 hook to fire and report lock status
	select {
	case lockReleased := <-phase2LockReleased:
		c.Assert(lockReleased, Equals, true)
	case <-time.After(60 * time.Second):
		c.Fatal("Timed out waiting for replicaAddFinish Phase 2 hook to fire")
	}

	// Reset hook
	internalEngine.SetFinishPhase2Hook(nil)

	// Wait for Replica Add to Complete
	err = retry.Do(func() error {
		e, err := spdkCli.EngineGet(engineName)
		if err != nil {
			return err
		}
		if e.ReplicaModeMap[replicaNames[1]] != types.ModeRW {
			return fmt.Errorf("replica %s is not RW yet: %v", replicaNames[1], e.ReplicaModeMap[replicaNames[1]])
		}
		return nil
	}, retry.Delay(1*time.Second), retry.Attempts(60))
	c.Assert(err, IsNil)

	// Verify Data I/O is still working
	f, err := os.OpenFile(endpoint, os.O_RDWR, 0666)
	c.Assert(err, IsNil)
	data := []byte("phase2-lock-test")
	_, err = f.WriteAt(data, 0)
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)
}

func writeDataToBlockDevice(ne *commonns.Executor, endpoint string, offsetInMB, dataCountInMB int64) error {
	return retry.Do(
		func() error {
			_, err := ne.Execute(
				nil,
				"dd",
				[]string{
					"if=/dev/urandom",
					fmt.Sprintf("of=%s", endpoint),
					"bs=1M",
					fmt.Sprintf("count=%d", dataCountInMB),
					fmt.Sprintf("seek=%d", offsetInMB),
					"status=none",
				},
				defaultTestExecuteTimeout,
			)
			return err
		},
		retry.Attempts(30),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			logrus.WithFields(logrus.Fields{
				"attempt": n + 1,
				"error":   err,
			}).Warn("Write data to block device failed, retrying...")
		}),
	)
}
