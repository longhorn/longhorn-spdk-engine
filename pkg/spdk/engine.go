package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"

	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

func svcEngineCreate(spdkClient *spdkclient.Client, name, frontend string, bdevAddressMap map[string]string, port int32) (ret *spdkrpc.Engine, err error) {
	if frontend != types.FrontendSPDKTCPBlockdev && frontend != types.FrontendSPDKTCPNvmf {
		return nil, fmt.Errorf("invalid frontend %s", frontend)
	}

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}

	// TODO: May need to do cleanup when there is an error

	replicaBdevList := []string{}
	for bdevName, addr := range bdevAddressMap {
		replicaIP, replicaPort, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid bdev %s address %s in engine %s creation", bdevName, addr, name)
		}
		if replicaIP == podIP {
			replicaBdevList = append(replicaBdevList, bdevName)
			continue
		}
		nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(bdevName, helpertypes.GetNQN(bdevName), replicaIP, replicaPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4)
		if err != nil {
			return nil, err
		}
		if len(nvmeBdevNameList) != 1 {
			return nil, fmt.Errorf("attaching bdev %s with address %s as a NVMe bdev does not get one result: %+v", bdevName, addr, nvmeBdevNameList)
		}
		replicaBdevList = append(replicaBdevList, nvmeBdevNameList[0])
	}

	if _, err := spdkClient.BdevRaidCreate(name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
		return nil, err
	}

	if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(name), name, podIP, strconv.Itoa(int(port))); err != nil {
		return nil, err
	}

	nqn := helpertypes.GetNQN(name)
	volumeName := util.GetVolumeNameFromEngineName(name)
	initiator, err := nvme.NewInitiator(volumeName, nqn, nvme.HostProc)
	if err != nil {
		return nil, err
	}

	if frontend == types.FrontendSPDKTCPBlockdev {
		if err := initiator.Start(podIP, strconv.Itoa(int(port))); err != nil {
			return nil, err
		}
	}

	return svcEngineGet(spdkClient, name)
}

func svcEngineDelete(spdkClient *spdkclient.Client, name string) (err error) {
	nqn := helpertypes.GetNQN(name)
	volumeName := util.GetVolumeNameFromEngineName(name)

	initiator, err := nvme.NewInitiator(volumeName, nqn, nvme.HostProc)
	if err != nil {
		return err
	}
	if err := initiator.Stop(); err != nil {
		return err
	}

	if err := spdkClient.StopExposeBdev(nqn); err != nil {
		return err
	}

	bdevRaidList, err := spdkClient.BdevRaidGet(name, 0)
	if err != nil {
		return err
	}
	switch len(bdevRaidList) {
	case 0:
		return nil
	case 1:
	default:
		return fmt.Errorf("found multiple raid bdev during engine %v deletion", name)
	}
	bdevRaid := bdevRaidList[0]

	if _, err := spdkClient.BdevRaidDelete(name); err != nil {
		return err
	}

	// TODO: How to figure out the rest attached nvme controllers and continue if one of the detaching fails

	bdevNvmeList, err := spdkClient.BdevNvmeGet("", 0)
	if err != nil {
		return err
	}
	bdevNvmeMap := map[string]spdktypes.BdevInfo{}
	for _, bdevNvme := range bdevNvmeList {
		bdevNvmeMap[bdevNvme.Name] = bdevNvme
	}

	for _, baseBdev := range bdevRaid.DriverSpecific.Raid.BaseBdevsList {
		bdevNvme, exists := bdevNvmeMap[baseBdev.Name]
		if !exists {
			// This replica must be a local lvol
			continue
		}

		if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(bdevNvme.Name)); err != nil {
			return err
		}
	}

	return nil
}

func svcEngineGet(spdkClient *spdkclient.Client, name string) (res *spdkrpc.Engine, err error) {
	res = &spdkrpc.Engine{
		Name:              name,
		ReplicaAddressMap: map[string]string{},
		ReplicaModeMap:    map[string]spdkrpc.ReplicaMode{},
	}

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}
	res.Ip = podIP

	nqn := helpertypes.GetNQN(name)
	subsystemList, err := spdkClient.NvmfGetSubsystems("", "")
	if err != nil {
		return nil, err
	}
	var subsystem *spdktypes.NvmfSubsystem
	for _, s := range subsystemList {
		if s.Nqn == nqn {
			subsystem = &s
			break
		}
	}
	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return nil, fmt.Errorf("cannot find the Nvmf subsystem for engine %s", name)
	}
	for _, listenAddr := range subsystem.ListenAddresses {
		if !strings.EqualFold(string(listenAddr.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(listenAddr.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			continue
		}
		port, err := strconv.Atoi(listenAddr.Trsvcid)
		if err != nil {
			return nil, err
		}
		res.Port = int32(port)
	}
	if res.Port == 0 {
		return nil, fmt.Errorf("cannot detect the port from Nvmf subsystem for engine %s", name)
	}

	bdevRaidList, err := spdkClient.BdevRaidGet(name, 0)
	if err != nil {
		return nil, err
	}
	if len(bdevRaidList) != 1 {
		return nil, fmt.Errorf("found multiple or zero raid bdevs in engine %v creation: %+v", name, bdevRaidList)
	}
	bdevRaid := bdevRaidList[0]
	bdevRaidInfo := bdevRaid.DriverSpecific.Raid
	res.Uuid = bdevRaid.UUID
	res.SpecSize = bdevRaid.NumBlocks * uint64(bdevRaid.BlockSize)

	bdevNvmeList, err := spdkClient.BdevNvmeGet("", 0)
	if err != nil {
		return nil, err
	}
	bdevNvmeMap := map[string]spdktypes.BdevInfo{}
	for _, bdevNvme := range bdevNvmeList {
		bdevNvmeMap[bdevNvme.Name] = bdevNvme
	}

	// TODO: Verify Mode
	for _, baseBdev := range bdevRaidInfo.BaseBdevsList {
		bdevNvme, exists := bdevNvmeMap[baseBdev.Name]
		if !exists {
			// This replica must be a local lvol
			replicaName := spdktypes.GetLvolNameFromAlias(baseBdev.Name)
			res.ReplicaAddressMap[replicaName] = ""
			res.ReplicaModeMap[replicaName] = spdkrpc.ReplicaMode_RW
			continue
		}

		if len(*bdevNvme.DriverSpecific.Nvme) < 1 {
			return nil, fmt.Errorf("found a remote base bdev %v that does not contain nvme info", bdevNvme.Name)
		}
		nvmeInfo := (*bdevNvme.DriverSpecific.Nvme)[0]
		if !strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(nvmeInfo.Trid.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			return nil, fmt.Errorf("found a remote base bdev %v that contains invalid address family %s and transport type %s", bdevNvme.Name, nvmeInfo.Trid.Adrfam, nvmeInfo.Trid.Trtype)
		}
		replicaName := helperutil.GetNvmeControllerNameFromNamespaceName(bdevNvme.Name)
		res.ReplicaAddressMap[replicaName] = fmt.Sprintf("%s:%s", nvmeInfo.Trid.Traddr, nvmeInfo.Trid.Trsvcid)
		res.ReplicaModeMap[replicaName] = spdkrpc.ReplicaMode_RW
	}

	volumeName := util.GetVolumeNameFromEngineName(name)

	initiator, err := nvme.NewInitiator(volumeName, nqn, nvme.HostProc)
	if err != nil {
		return nil, err
	}
	// Failed to load the NVMe device info, probably the frontend is spdk-tcp-nvmf and the initiator is not started.
	if err := initiator.LoadNVMeDeviceInfo(); err != nil {
		res.Endpoint = fmt.Sprintf("%s://%s:%d", nqn, res.Ip, res.Port)
		return res, nil
	}

	if err := initiator.LoadEndpoint(); err != nil {
		return nil, err
	}
	res.Endpoint = initiator.GetEndpoint()

	return res, nil
}

func svcEngineSnapshotCreate(spdkClient *spdkclient.Client, name, snapshotName string) (res *spdkrpc.Engine, err error) {
	return nil, fmt.Errorf("unimplemented")
}

func svcEngineSnapshotDelete(spdkClient *spdkclient.Client, name, snapshotName string) (res *empty.Empty, err error) {
	return nil, fmt.Errorf("unimplemented")
}
