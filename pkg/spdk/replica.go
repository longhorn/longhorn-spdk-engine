package spdk

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

type Replica struct {
	sync.RWMutex

	ActiveChain []*Lvol
	ChainLength int
	SnapshotMap map[string]*Lvol

	Name      string
	UUID      string
	Alias     string
	LvsName   string
	LvsUUID   string
	SpecSize  uint64
	IP        string
	PortStart int32
	PortEnd   int32

	State     ReplicaState
	IsExposed bool

	portAllocator *util.Bitmap

	log logrus.FieldLogger
}

type ReplicaState string

const (
	ReplicaStatePending = "pending"
	ReplicaStateStopped = "stopped"
	ReplicaStateStarted = "started"
	ReplicaStateError   = "error"
)

type Lvol struct {
	Name       string
	UUID       string
	Alias      string
	SpecSize   uint64
	ActualSize uint64
	Parent     string
	Children   map[string]*Lvol
}

func ServiceReplicaToProtoReplica(r *Replica) *spdkrpc.Replica {
	res := &spdkrpc.Replica{
		Name:      r.Name,
		Uuid:      r.UUID,
		LvsName:   r.LvsName,
		LvsUuid:   r.LvsUUID,
		SpecSize:  r.SpecSize,
		Snapshots: map[string]*spdkrpc.Lvol{},
		Ip:        r.IP,
		PortStart: r.PortStart,
		PortEnd:   r.PortEnd,
	}
	for name, lvol := range r.SnapshotMap {
		res.Snapshots[name] = ServiceLvolToProtoLvol(lvol)
	}

	return res
}

func ServiceLvolToProtoLvol(lvol *Lvol) *spdkrpc.Lvol {
	res := &spdkrpc.Lvol{
		Name:       lvol.Name,
		Uuid:       lvol.UUID,
		SpecSize:   lvol.SpecSize,
		ActualSize: lvol.ActualSize,
		Parent:     lvol.Parent,
		Children:   map[string]bool{},
	}
	for _, childSvcLvol := range lvol.Children {
		res.Children[childSvcLvol.Name] = true
	}

	return res
}

func BdevLvolInfoToServiceLvol(bdev *spdktypes.BdevInfo) *Lvol {
	return &Lvol{
		Name:     spdktypes.GetLvolNameFromAlias(bdev.Aliases[0]),
		Alias:    bdev.Aliases[0],
		UUID:     bdev.UUID,
		SpecSize: bdev.NumBlocks * uint64(bdev.BlockSize),
		Parent:   bdev.DriverSpecific.Lvol.BaseSnapshot,
		// Need to update this separately
		Children: map[string]*Lvol{},
	}
}

func NewReplica(replicaName, lvsName, lvsUUID string, specSize uint64) *Replica {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"replicaName": replicaName,
		"lvsName":     lvsName,
		"lvsUUID":     lvsUUID,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the specSize should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	return &Replica{
		ActiveChain: []*Lvol{
			{
				Name:     replicaName,
				Alias:    spdktypes.GetLvolAlias(lvsName, replicaName),
				SpecSize: roundedSpecSize,
				Children: map[string]*Lvol{},
			},
		},
		ChainLength: 1,
		SnapshotMap: map[string]*Lvol{},
		Name:        replicaName,
		Alias:       spdktypes.GetLvolAlias(lvsName, replicaName),
		LvsName:     lvsName,
		LvsUUID:     lvsUUID,
		SpecSize:    roundedSpecSize,
		State:       ReplicaStatePending,

		log: log,
	}
}

// Construct build Replica with the SnapshotMap and SnapshotChain from the bdev lvol list.
// This function is typically invoked for the existing lvols after node/service restart and device add.
func (r *Replica) Construct(bdevLvolMap map[string]*spdktypes.BdevInfo) (err error) {
	r.Lock()
	defer func() {
		if err != nil {
			r.State = ReplicaStateError
		}
		r.Unlock()
	}()

	if r.State != ReplicaStatePending {
		return fmt.Errorf("invalid state %s for replica %s construct", r.Name, r.State)
	}

	if err := r.validateReplicaInfoWithoutLock(bdevLvolMap[r.Name]); err != nil {
		return err
	}
	newChain, err := constructActiveChain(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	newSnapshotMap, err := constructSnapshotMap(r.Name, newChain[0], bdevLvolMap)
	if err != nil {
		return err
	}

	r.UUID = bdevLvolMap[r.Name].UUID
	r.ActiveChain = newChain
	r.ChainLength = len(r.ActiveChain)
	r.SnapshotMap = newSnapshotMap
	r.State = ReplicaStateStopped
	r.log.WithField("uuid", r.UUID)

	return nil
}

func (r *Replica) ValidateAndUpdate(spdkClient *spdkclient.Client,
	bdevLvolMap map[string]*spdktypes.BdevInfo, subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if err != nil {
			r.State = ReplicaStateError
			r.log.Errorf("Found error during validation and update: %v", err)
		}
	}()

	if err := r.validateReplicaInfoWithoutLock(bdevLvolMap[r.Name]); err != nil {
		return err
	}

	newChain, err := constructActiveChain(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	if len(r.ActiveChain) != len(newChain) {
		return fmt.Errorf("replica current active chain length %d is not the same as the latest chain length %d", len(r.ActiveChain), len(newChain))
	}
	for idx, svcLvol := range r.ActiveChain {
		newSvcLvol := newChain[idx]
		if err := compareSvcLvols(svcLvol, newChain[idx], false, svcLvol.Name != r.Name); err != nil {
			return err
		}
		// Then update the actual size for the head lvol
		if svcLvol.Name == r.Name {
			svcLvol.ActualSize = newSvcLvol.ActualSize
		}
	}

	newSnapshotMap, err := constructSnapshotMap(r.Name, newChain[0], bdevLvolMap)
	if err != nil {
		return err
	}
	if len(r.SnapshotMap) != len(newSnapshotMap) {
		return fmt.Errorf("replica current active snapshot map length %d is not the same as the latest snapshot map length %d", len(r.SnapshotMap), len(newSnapshotMap))
	}
	for svcLvolName, svcLvol := range r.SnapshotMap {
		newSvcLvol := newSnapshotMap[svcLvolName]
		if err := compareSvcLvols(svcLvol, newSvcLvol, true, true); err != nil {
			return err
		}
	}

	if r.IP == "" {
		return fmt.Errorf("found invalid IP %s for replica %s", r.IP, r.Name)
	}

	if r.IsExposed {
		return nil
	}

	if r.PortStart == 0 || r.PortEnd == 0 {
		return fmt.Errorf("found invalid Ports [%d, %d] for the exposed replica %s", r.PortStart, r.PortEnd, r.Name)
	}

	nqn := helpertypes.GetNQN(r.Name)
	subsystem := subsystemMap[nqn]
	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return fmt.Errorf("cannot find the Nvmf subsystem with NQN %s for the exposed replica %s", nqn, r.Name)
	}
	exposedPort := 0
	for _, listenAddr := range subsystem.ListenAddresses {
		if listenAddr.Adrfam != spdktypes.NvmeAddressFamilyIPv4 || listenAddr.Trtype != spdktypes.NvmeTransportTypeTCP {
			continue
		}
		exposedPort, err = strconv.Atoi(listenAddr.Trsvcid)
		if err != nil {
			return err
		}
		break
	}
	if int32(exposedPort) != r.PortStart {
		if stopErr := spdkClient.StopExposeBdev(nqn); stopErr != nil {
			return errors.Wrapf(stopErr, "failed to stop the replica expose after finding mismatching between the actual exposed port %d and the recorded port %d for the exposed replica %s", exposedPort, r.PortStart, r.Name)
		}
		return fmt.Errorf("found mismatching between the actual exposed port %d and the recorded port %d for the exposed replica %s", exposedPort, r.PortStart, r.Name)
	}

	return nil
}

func compareSvcLvols(prev, cur *Lvol, checkChildren, checkActualSize bool) error {
	if prev == nil && cur == nil {
		return nil
	}
	if prev == nil {
		return fmt.Errorf("cannot find the corresponding prev lvol")
	}
	if cur == nil {
		return fmt.Errorf("cannot find the corresponding cur lvol")
	}
	if prev.Name != cur.Name || prev.UUID != cur.UUID || prev.SpecSize != cur.SpecSize || prev.Parent != cur.Parent || len(prev.Children) != len(cur.Children) {
		return fmt.Errorf("found mismatching lvol %+v with recorded prev lvol %+v when validating the active chain", cur, prev)
	}
	if checkChildren {
		for childName := range prev.Children {
			if cur.Children[childName] == nil {
				return fmt.Errorf("found mismatching lvol children %+v with recorded prev lvol children %+v when validating the active chain lvol %s", cur.Children, prev.Children, prev.Name)
			}
		}
	}
	if checkActualSize && prev.ActualSize != cur.ActualSize {
		return fmt.Errorf("found mismatching lvol actual size %v with recorded prev lvol actual size %v when validating the active chain lvol %s", cur.ActualSize, prev.ActualSize, prev.Name)
	}

	return nil
}

func (r *Replica) validateReplicaInfoWithoutLock(headBdevLvol *spdktypes.BdevInfo) (err error) {
	if headBdevLvol == nil {
		return fmt.Errorf("find nil head bdev lvol for replica %s", r.Name)
	}
	if headBdevLvol.DriverSpecific.Lvol.Snapshot {
		return fmt.Errorf("found the head bdev lvol is a snapshot lvol for replica %s", r.Name)
	}
	if r.LvsUUID != headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID {
		return fmt.Errorf("found mismatching lvol LvsUUID %v with recorded LvsUUID %v for replica %s", headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID, r.LvsUUID, r.Name)
	}
	if r.UUID != "" && headBdevLvol.UUID != r.UUID {
		return fmt.Errorf("found mismatching lvol UUID %v with recorded UUID %v for replica %s", headBdevLvol.UUID, r.UUID, r.Name)
	}
	bdevLvolSpecSize := headBdevLvol.NumBlocks * uint64(headBdevLvol.BlockSize)
	if r.SpecSize != 0 && r.SpecSize != bdevLvolSpecSize {
		return fmt.Errorf("found mismatching lvol spec size %v with recorded spec size %v for replica %s", bdevLvolSpecSize, r.SpecSize, r.Name)
	}

	return nil
}

func constructActiveChain(replicaName string, bdevLvolMap map[string]*spdktypes.BdevInfo) (res []*Lvol, err error) {
	newChain := []*Lvol{}

	headBdevLvol := bdevLvolMap[replicaName]
	if headBdevLvol == nil {
		return nil, fmt.Errorf("cannot find the head bdev lvol from the bdev lvol map for replica %s", replicaName)
	}
	headSvcLvol := BdevLvolInfoToServiceLvol(headBdevLvol)
	// TODO: Considering the clone, this function or `constructSnapshotMap` may need to construct the children map for the head

	for prevSvcLvol, curBdevLvol := headSvcLvol, bdevLvolMap[headBdevLvol.DriverSpecific.Lvol.BaseSnapshot]; curBdevLvol != nil; {
		curvSvcLvol := BdevLvolInfoToServiceLvol(curBdevLvol)
		curvSvcLvol.Children[prevSvcLvol.Name] = prevSvcLvol
		prevSvcLvol.Parent = curvSvcLvol.Name
		newChain = append(newChain, curvSvcLvol)

		prevSvcLvol = curvSvcLvol
		curBdevLvol = bdevLvolMap[headBdevLvol.DriverSpecific.Lvol.BaseSnapshot]
	}

	// Need to flip r.ActiveSnapshotChain since the oldest should be the first entry
	for head, tail := 0, len(newChain)-1; head < tail; head, tail = head+1, tail-1 {
		newChain[head], newChain[tail] = newChain[tail], newChain[head]
	}
	newChain = append(newChain, headSvcLvol)

	return newChain, nil
}

func constructSnapshotMap(replicaName string, rootSvcLvol *Lvol, bdevLvolMap map[string]*spdktypes.BdevInfo) (res map[string]*Lvol, err error) {
	res = map[string]*Lvol{}

	queue := []*Lvol{rootSvcLvol}
	for ; len(queue) > 0; queue = queue[1:] {
		curSvcLvol := queue[0]
		if curSvcLvol == nil || curSvcLvol.Name == replicaName {
			continue
		}
		res[curSvcLvol.Name] = curSvcLvol

		if bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones == nil {
			continue
		}
		for _, childName := range bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones {
			if bdevLvolMap[childName] == nil {
				return nil, fmt.Errorf("cannot find child lvol %v for lvol %v during the snapshot map construction", childName, curSvcLvol.Name)
			}
			tmpSvcLvol := curSvcLvol.Children[childName]
			if tmpSvcLvol == nil {
				tmpSvcLvol = BdevLvolInfoToServiceLvol(bdevLvolMap[childName])
			}
			curSvcLvol.Children[childName] = tmpSvcLvol
			queue = append(queue, tmpSvcLvol)
		}
	}

	return res, nil
}

func (r *Replica) Create(spdkClient *spdkclient.Client, exposeRequired bool, superiorPortAllocator *util.Bitmap) (ret *spdkrpc.Replica, err error) {
	r.Lock()
	defer r.Unlock()

	if r.State != ReplicaStatePending && r.State != ReplicaStateStopped {
		return nil, fmt.Errorf("invalid state %s for replica %s creation", r.Name, r.State)
	}

	defer func() {
		if err != nil {
			r.State = ReplicaStateError
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica creation", r.ChainLength)
	}
	headSvcLvol := r.ActiveChain[r.ChainLength-1]

	// Create bdev lvol if the replica is the new one
	if r.State == ReplicaStatePending {
		r.log.Infof("Creating a lvol bdev for the new replica")
		if _, err := spdkClient.BdevLvolCreate(r.LvsName, r.Name, "", util.BytesToMiB(r.SpecSize), "", true); err != nil {
			return nil, err
		}
		bdevLvolList, err := spdkClient.BdevLvolGet(r.Alias, 0)
		if err != nil {
			return nil, err
		}
		if len(bdevLvolList) < 1 {
			return nil, fmt.Errorf("cannot find lvol %v after creation", r.Alias)
		}
		headSvcLvol.UUID = bdevLvolList[0].UUID
		r.UUID = bdevLvolList[0].UUID
		r.State = ReplicaStateStopped
		r.log.WithField("uuid", r.UUID)
	}

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}
	r.IP = podIP

	r.PortStart, r.PortEnd, err = superiorPortAllocator.AllocateRange(types.DefaultReplicaReservedPortCount)
	if err != nil {
		return nil, err
	}
	// Always reserved the 1st port for replica expose and the rest for rebuilding
	r.portAllocator = util.NewBitmap(r.PortStart+1, r.PortEnd)

	if exposeRequired {
		if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.Name), r.UUID, podIP, string(r.PortStart)); err != nil {
			return nil, err
		}
		r.IsExposed = true
	}
	r.State = ReplicaStateStarted

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) Delete(spdkClient *spdkclient.Client, cleanupRequired bool, superiorPortAllocator *util.Bitmap) (err error) {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if err != nil {
			r.State = ReplicaStateError
		} else {
			r.State = ReplicaStateStopped
		}
		// The port can be released once the rebuilding and expose are stopped
		if !r.IsExposed && r.PortStart != 0 {
			if releaseErr := superiorPortAllocator.ReleaseRange(r.PortStart, r.PortEnd); releaseErr != nil {
				r.log.Errorf("Failed to release port %d to %d at the end of replica deletion: %v", r.PortStart, r.PortEnd, releaseErr)
				return
			}
			r.portAllocator = nil
			r.PortStart, r.PortEnd = 0, 0
		}
	}()

	// TODO: Need to stop all in-progress rebuilding first

	if r.IsExposed {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.IsExposed = false
	}

	if !cleanupRequired {
		return nil
	}

	if _, err := spdkClient.BdevLvolDelete(r.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	for _, lvol := range r.SnapshotMap {
		if _, err := spdkClient.BdevLvolDelete(lvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		delete(r.SnapshotMap, lvol.Name)
	}

	return nil
}

func (r *Replica) Get() (pReplica *spdkrpc.Replica) {
	r.RLock()
	defer r.RUnlock()
	return ServiceReplicaToProtoReplica(r)
}

func (r *Replica) SnapshotCreate(spdkClient *spdkclient.Client, snapshotName string) (pReplica *spdkrpc.Replica, err error) {
	r.Lock()
	defer r.Unlock()

	if r.State != ReplicaStateStopped && r.State != ReplicaStateStarted {
		return nil, fmt.Errorf("invalid state %v for replica %s snapshot creation", r.State, r.Name)
	}

	defer func() {
		if err != nil {
			r.State = ReplicaStateError
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica snapshot creation", r.ChainLength)
	}
	headSvcLvol := r.ActiveChain[r.ChainLength-1]

	snapUUID, err := spdkClient.BdevLvolSnapshot(headSvcLvol.UUID, GetReplicaSnapshotLvolName(headSvcLvol.Name, snapshotName))
	if err != nil {
		return nil, err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return nil, err
	}
	if len(bdevLvolList) != 1 {
		return nil, fmt.Errorf("zero or multiple snap lvols with UUID %s found after lvol snapshot", snapUUID)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	snapSvcLvol.Children[headSvcLvol.Name] = headSvcLvol

	// Already contain one active snapshot before this snapshot creation
	if r.ChainLength > 1 {
		prevSvcLvol := r.ActiveChain[r.ChainLength-2]
		delete(prevSvcLvol.Children, headSvcLvol.Name)
		prevSvcLvol.Children[snapSvcLvol.Name] = snapSvcLvol
	}
	r.ActiveChain = append(r.ActiveChain, snapSvcLvol)
	r.ChainLength++
	r.SnapshotMap[snapSvcLvol.Name] = snapSvcLvol
	headSvcLvol.Parent = snapSvcLvol.Name

	return ServiceReplicaToProtoReplica(r), err
}

func (r *Replica) SnapshotDelete(spdkClient *spdkclient.Client, snapshotName string) (pReplica *spdkrpc.Replica, err error) {
	r.Lock()
	defer r.Unlock()

	if r.State != ReplicaStateStopped && r.State != ReplicaStateStarted {
		return nil, fmt.Errorf("invalid state %v for replica %s snapshot deletion", r.State, r.Name)
	}

	lvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	if r.SnapshotMap[lvolName] == nil {
		return ServiceReplicaToProtoReplica(r), nil
	}
	if len(r.SnapshotMap[lvolName].Children) > 1 {
		return nil, fmt.Errorf("cannot delete snapshot %s(%s) since it has %d children", snapshotName, lvolName, len(r.SnapshotMap[lvolName].Children))
	}

	defer func() {
		if err != nil {
			r.State = ReplicaStateError
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica snapshot delete", r.ChainLength)
	}

	r.removeLvolFromSnapshotMapWithoutLock(lvolName)
	r.removeLvolFromActiveChainWithoutLock(lvolName)

	if _, err := spdkClient.BdevLvolDelete(lvolName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, err
	}

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) removeLvolFromSnapshotMapWithoutLock(name string) {
	var deletingSvcLvol, parentSvcLvol, childSvcLvol *Lvol

	deletingSvcLvol = r.SnapshotMap[name]
	parentSvcLvol = r.SnapshotMap[deletingSvcLvol.Parent]
	for _, childSvcLvol = range deletingSvcLvol.Children {
		break
	}

	if parentSvcLvol != nil {
		delete(parentSvcLvol.Children, deletingSvcLvol.Name)
		if childSvcLvol != nil {
			parentSvcLvol.Children[childSvcLvol.Name] = childSvcLvol
			childSvcLvol.Parent = parentSvcLvol.Name
		}
	} else {
		if childSvcLvol != nil {
			childSvcLvol.Parent = ""
		}
	}
}

func (r *Replica) removeLvolFromActiveChainWithoutLock(name string) int {
	pos := -1
	for idx, lvol := range r.ActiveChain {
		if lvol.Name == name {
			pos = idx
			break
		}
	}

	if pos >= 0 && pos < r.ChainLength-1 {
		r.ActiveChain = append([]*Lvol{}, r.ActiveChain[:pos]...)
		r.ActiveChain = append(r.ActiveChain, r.ActiveChain[pos+1:]...)
	}
	r.ChainLength = len(r.ActiveChain)

	return pos
}
