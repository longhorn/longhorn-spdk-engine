package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

type Engine struct {
	sync.RWMutex

	Name               string
	VolumeName         string
	SpecSize           uint64
	ActualSize         uint64
	ReplicaAddressMap  map[string]string
	ReplicaBdevNameMap map[string]string
	ReplicaModeMap     map[string]types.Mode
	IP                 string
	Port               int32
	Frontend           string
	Endpoint           string

	log logrus.FieldLogger
}

func NewEngine(engineName, volumeName, frontend string, specSize uint64) *Engine {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineName": engineName,
		"volumeName": volumeName,
		"frontend":   frontend,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	return &Engine{
		Name:               engineName,
		VolumeName:         volumeName,
		Frontend:           frontend,
		SpecSize:           specSize,
		ReplicaAddressMap:  map[string]string{},
		ReplicaBdevNameMap: map[string]string{},
		ReplicaModeMap:     map[string]types.Mode{},

		log: log,
	}
}

func (e *Engine) Create(spdkClient *spdkclient.Client, replicaAddressMap, localReplicaBdevMap map[string]string, superiorPortAllocator *util.Bitmap) (ret *spdkrpc.Engine, err error) {
	e.Lock()
	defer e.Unlock()

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}

	replicaBdevList := []string{}
	for replicaName, replicaAddr := range replicaAddressMap {
		bdevName, err := getBdevNameForReplica(spdkClient, localReplicaBdevMap, replicaName, replicaAddr, podIP)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to get bdev from replica %s with address %s, will skip it and continue", replicaName, replicaAddr)
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.ReplicaBdevNameMap[replicaName] = ""
			continue
		}
		e.ReplicaModeMap[replicaName] = types.ModeRW
		e.ReplicaBdevNameMap[replicaName] = bdevName
		replicaBdevList = append(replicaBdevList, bdevName)
	}
	e.ReplicaAddressMap = replicaAddressMap

	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
		return nil, err
	}

	nqn := helpertypes.GetNQN(e.Name)
	port, _, err := superiorPortAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}
	if err := spdkClient.StartExposeBdev(nqn, e.Name, podIP, strconv.Itoa(int(port))); err != nil {
		return nil, err
	}
	e.IP = podIP
	e.Port = port

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
		if err != nil {
			return nil, err
		}
		if err := initiator.Start(podIP, strconv.Itoa(int(port))); err != nil {
			return nil, err
		}
		e.Endpoint = initiator.GetEndpoint()
	case types.FrontendSPDKTCPNvmf:
		e.Endpoint = GetNvmfEndpoint(nqn, e.IP, e.Port)
	default:
		return nil, fmt.Errorf("unknown frontend type %s", e.Frontend)

	}

	return e.getWithoutLock(), nil
}

func getBdevNameForReplica(spdkClient *spdkclient.Client, localReplicaBdevMap map[string]string, replicaName, replicaAddress, podIP string) (bdevName string, err error) {
	replicaIP, replicaPort, err := net.SplitHostPort(replicaAddress)
	if err != nil {
		return "", err
	}
	if replicaIP == podIP {
		if localReplicaBdevMap[replicaName] == "" {
			return "", fmt.Errorf("cannot to find local replica %s from the local replica bdev map", replicaName)

		}
		return localReplicaBdevMap[replicaName], nil
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(replicaName, helpertypes.GetNQN(replicaName), replicaIP, replicaPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4)
	if err != nil {
		return "", err
	}
	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching replica %s with address %s as a NVMe bdev: %+v", replicaName, replicaAddress, nvmeBdevNameList)
	}
	return nvmeBdevNameList[0], nil
}

func (e *Engine) Delete(spdkClient *spdkclient.Client, superiorPortAllocator *util.Bitmap) (err error) {
	e.Lock()
	defer e.Unlock()

	nqn := helpertypes.GetNQN(e.Name)

	initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
	if err != nil {
		return err
	}
	if err := initiator.Stop(); err != nil {
		return err
	}

	if err := spdkClient.StopExposeBdev(nqn); err != nil {
		return err
	}

	if e.Port != 0 {
		if err := superiorPortAllocator.ReleaseRange(e.Port, e.Port); err != nil {
			return err
		}
		e.Port = 0
	}

	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	for replicaName, replicaAddress := range e.ReplicaAddressMap {
		replicaIP, _, err := net.SplitHostPort(replicaAddress)
		if err != nil {
			return err
		}
		if replicaIP == e.IP {
			continue
		}
		bdevName := e.ReplicaBdevNameMap[replicaName]
		if bdevName == "" {
			continue
		}
		if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(bdevName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
	}

	return nil
}

func (e *Engine) Get() (res *spdkrpc.Engine) {
	e.RLock()
	defer e.RUnlock()

	return e.getWithoutLock()
}

func (e *Engine) getWithoutLock() (res *spdkrpc.Engine) {
	res = &spdkrpc.Engine{
		Name:              e.Name,
		SpecSize:          e.SpecSize,
		ActualSize:        e.ActualSize,
		ReplicaAddressMap: e.ReplicaAddressMap,
		ReplicaModeMap:    map[string]spdkrpc.ReplicaMode{},
		Ip:                e.IP,
		Port:              e.Port,
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
	}

	for replicaName, replicaMode := range e.ReplicaModeMap {
		res.ReplicaModeMap[replicaName] = spdkrpc.ReplicaModeToGRPCReplicaMode(replicaMode)
	}

	return res
}

func (e *Engine) ValidateAndUpdate(
	bdevMap map[string]*spdktypes.BdevInfo, subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	e.Lock()
	defer e.Unlock()

	podIP, err := util.GetIPForPod()
	if err != nil {
		return err
	}
	if e.IP != podIP {
		return fmt.Errorf("found mismatching between engine IP %s and pod IP %s for engine %s", e.IP, podIP, e.Name)
	}

	nqn := helpertypes.GetNQN(e.Name)
	subsystem := subsystemMap[nqn]
	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return fmt.Errorf("cannot find the Nvmf subsystem for engine %s", e.Name)
	}

	port := 0
	for _, listenAddr := range subsystem.ListenAddresses {
		if !strings.EqualFold(string(listenAddr.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(listenAddr.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			continue
		}
		if port, err = strconv.Atoi(listenAddr.Trsvcid); err != nil {
			return err
		}
		if e.Port == int32(port) {
			break
		}
	}
	if port == 0 || e.Port != int32(port) {
		return fmt.Errorf("cannot find a matching listener with port %d from Nvmf subsystem for engine %s", e.Port, e.Name)
	}

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
		if err != nil {
			return err
		}
		if err := initiator.LoadNVMeDeviceInfo(); err != nil {
			return err
		}

		if err := initiator.LoadEndpoint(); err != nil {
			return err
		}
		blockDevEndpoint := initiator.GetEndpoint()
		if e.Endpoint != "" && e.Endpoint != blockDevEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual block device endpoint %s for engine %s", e.Endpoint, blockDevEndpoint, e.Name)
		}
		e.Endpoint = blockDevEndpoint
	case types.FrontendSPDKTCPNvmf:
		nvmfEndpoint := GetNvmfEndpoint(nqn, e.IP, e.Port)
		if e.Endpoint != "" && e.Endpoint != nvmfEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual nvmf endpoint %s for engine %s", e.Endpoint, nvmfEndpoint, e.Name)
		}
		e.Endpoint = nvmfEndpoint
	default:
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	bdevRaid := bdevMap[e.Name]
	if spdktypes.GetBdevType(bdevRaid) != spdktypes.BdevTypeRaid {
		return fmt.Errorf("cannot find a raid bdev for engine %v", e.Name)
	}
	bdevRaidSize := bdevRaid.NumBlocks * uint64(bdevRaid.BlockSize)
	if e.SpecSize != bdevRaidSize {
		return fmt.Errorf("found mismatching between engine spec size %d and actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
	}

	for replicaName, bdevName := range e.ReplicaBdevNameMap {
		mode, err := e.validateAndUpdateReplicaMode(replicaName, bdevMap[bdevName])
		if err != nil {
			e.log.WithError(err).Errorf("Replica %s is invalid, will update the mode from %s to %s", replicaName, e.ReplicaModeMap[replicaName], types.ModeERR)
			e.ReplicaModeMap[replicaName] = types.ModeERR
			continue
		}
		if e.ReplicaModeMap[replicaName] != mode {
			e.log.Debugf("Replica %s mode is updated from %s to %s", replicaName, e.ReplicaModeMap[replicaName], mode)
			e.ReplicaModeMap[replicaName] = mode
		}
	}

	return nil
}

func (e *Engine) validateAndUpdateReplicaMode(replicaName string, bdev *spdktypes.BdevInfo) (mode types.Mode, err error) {
	if bdev == nil {
		return types.ModeERR, fmt.Errorf("cannot find a bdev for replica %s", replicaName)
	}
	bdevSpecSize := bdev.NumBlocks * uint64(bdev.BlockSize)
	if e.SpecSize != bdevSpecSize {
		return types.ModeERR, fmt.Errorf("found mismatching between replica %s bdev spec size %d and the engine spec size %d for engine %s", replicaName, bdevSpecSize, e.SpecSize, e.Name)
	}
	switch spdktypes.GetBdevType(bdev) {
	case spdktypes.BdevTypeLvol:
		replicaIP, _, err := net.SplitHostPort(e.ReplicaAddressMap[replicaName])
		if err != nil {
			return types.ModeERR, err
		}
		if e.IP != replicaIP {
			return types.ModeERR, fmt.Errorf("found mismatching between replica %s IP %s and the engine IP %s for engine %s", replicaName, replicaIP, e.IP, e.Name)
		}
	case spdktypes.BdevTypeNvme:
		if len(*bdev.DriverSpecific.Nvme) != 1 {
			return types.ModeERR, fmt.Errorf("found zero or multiple nvme info in a remote nvme base bdev %v for replica %s", bdev.Name, replicaName)
		}
		nvmeInfo := (*bdev.DriverSpecific.Nvme)[0]
		if !strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(nvmeInfo.Trid.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			return types.ModeERR, fmt.Errorf("found invalid address family %s and transport type %s in a remote nvme base bdev %s for replica %s", nvmeInfo.Trid.Adrfam, nvmeInfo.Trid.Trtype, bdev.Name, replicaName)
		}
		bdevAddr := net.JoinHostPort(nvmeInfo.Trid.Traddr, nvmeInfo.Trid.Trsvcid)
		if e.ReplicaAddressMap[replicaName] != bdevAddr {
			return types.ModeERR, fmt.Errorf("found mismatching between replica %s bdev address %s and the nvme bdev actual address %s", replicaName, e.ReplicaAddressMap[replicaName], bdevAddr)
		}
		// TODO: Validate NVMe controller state
		// TODO: Verify Mode WO
	default:
		return types.ModeERR, fmt.Errorf("found invalid bdev type %v for replica %s ", spdktypes.GetBdevType(bdev), replicaName)
	}

	return types.ModeRW, nil
}

func (e *Engine) SnapshotCreate(spdkClient *spdkclient.Client, name, snapshotName string) (res *spdkrpc.Engine, err error) {
	return nil, fmt.Errorf("unimplemented")
}

func (e *Engine) SnapshotDelete(spdkClient *spdkclient.Client, name, snapshotName string) (res *empty.Empty, err error) {
	return nil, fmt.Errorf("unimplemented")
}

func ServiceEngineToProtoEngine(e *Engine) *spdkrpc.Engine {
	replicaAddressMap := map[string]string{}
	for replicaName, address := range e.ReplicaAddressMap {
		replicaAddressMap[replicaName] = address
	}

	replicaModeMap := map[string]spdkrpc.ReplicaMode{}
	for replicaName, replicaMode := range e.ReplicaModeMap {
		replicaModeMap[replicaName] = spdkrpc.ReplicaModeToGRPCReplicaMode(replicaMode)
	}

	return &spdkrpc.Engine{
		Name:              e.Name,
		VolumeName:        e.VolumeName,
		SpecSize:          e.SpecSize,
		ActualSize:        e.ActualSize,
		Ip:                e.IP,
		Port:              e.Port,
		ReplicaAddressMap: replicaAddressMap,
		ReplicaModeMap:    replicaModeMap,
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
	}
}
