package spdk

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

// NVMe-oF connection timeouts for the ShardGroup process's connections to
// shard endpoints. Tuned longer than the replica equivalents so that brief
// network blips do not flap an EC slot to FAILED before reconnect can land.
const (
	ecShardCtrlrLossTimeoutSec  = 120
	ecShardFastIOFailTimeoutSec = 15
)

// ShardGroup is the SPDK-side process state for an EC volume's lvstore + head
// lvol layer. Each ShardGroup process owns:
//
//   - k+m NVMe-oF connections to shard endpoints (the base bdevs).
//   - A bdev_ec on top of those shard bdevs.
//   - A per-volume lvstore on top of bdev_ec.
//   - A head lvol in the lvstore, exposed via NVMe-oF for the engine to consume.
//
// The lvstore and head lvol survive volume detach (cleanupRequired=false on
// Delete). They are torn down only on volume delete (cleanupRequired=true).
// This cleanupRequired discipline is what prevents the EC-volume detach
// data-loss bug - detach must never call BdevLvolDeleteLvstore on the
// bdev_ec-backed lvstore.
type ShardGroup struct {
	sync.RWMutex

	ctx context.Context

	Name       string // typically equals VolumeName
	VolumeName string

	// EC parameters, immutable after Create.
	DataChunks   uint32
	ParityChunks uint32
	StripSizeKb  uint32
	SpecSize     uint64

	// SalvageRequested selects the recovery path on Create: tolerate
	// missing shard connections (passing "" for unreachable slots) and
	// skip lvstore + head lvol creation - SPDK's bdev_examine
	// auto-imports the existing lvstore from the encoded shard blocks.
	// Set by the controller for ShardGroup process re-provisioning on a
	// new node (engine-node failover) or after process crash.
	SalvageRequested bool

	// Upstream shard endpoints keyed by Shard CR external name
	// (<volumeName>-<slotIndex>). Populated at Create time from
	// ShardGroupSpec.shards.
	Shards map[string]*ShardEndpoint

	// SPDK names for the layered bdev stack.
	EcBdevName  string // <volumeName>-ec
	LvsName     string // <volumeName>-lvs
	LvsUUID     string // populated after lvstore creation
	ClusterSize uint64 // lvstore cluster size in bytes. Queried from SPDK, not
	// hardcoded: the lvstore is created with cluster_sz=0 (SPDK's default,
	// currently 4 MiB), and refreshECSnapshotMapNoLock multiplies
	// NumAllocatedClusters by this value to get bytes.
	HeadLvolName string // == VolumeName
	HeadLvolUUID string // populated after head lvol creation
	HeadAlias    string // <LvsName>/<HeadLvolName>
	Nqn          string // NVMe-oF subsystem NQN for the exposed head lvol

	// Cached snapshot lineage for ShardGroupGet. Refreshed by
	// refreshECSnapshotMapNoLock() after create/expand/snapshot operations.
	Head        *api.Lvol
	SnapshotMap map[string]*api.Lvol
	ActualSize  uint64

	IP   string
	Port int32

	State    types.InstanceState
	ErrorMsg string

	IsExposed bool

	// UpdateCh should not be protected by the ShardGroup lock.
	UpdateCh chan interface{}

	log *safelog.SafeLogger
}

// ShardEndpoint is the upstream shard address and slot index that the
// ShardGroup process consumes as a base bdev for bdev_ec.
type ShardEndpoint struct {
	Address   string // ip:port
	SlotIndex uint32
}

// GetShardGroupEcBdevName returns the SPDK bdev name for the per-volume bdev_ec.
func GetShardGroupEcBdevName(volumeName string) string {
	return fmt.Sprintf("%s-ec", volumeName)
}

// GetShardGroupLvsName returns the SPDK lvstore name on the bdev_ec.
func GetShardGroupLvsName(volumeName string) string {
	return fmt.Sprintf("%s-lvs", volumeName)
}

// NewShardGroup constructs a ShardGroup in InstanceStatePending. A subsequent
// Create call materializes the SPDK stack on disk.
//
// salvageRequested=true selects the recovery path on Create - tolerate missing
// shard connections and skip lvstore + head lvol creation, letting bdev_examine
// re-discover the existing lvstore from the encoded blocks. salvageRequested is
// also forwarded to bdev_ec_create so the SPDK layer refuses to fresh-zero a
// torn in-band unmap bitmap on a recreate.
func NewShardGroup(ctx context.Context, name, volumeName string, specSize uint64,
	dataChunks, parityChunks, stripSizeKb uint32, shards map[string]*ShardEndpoint,
	salvageRequested bool, updateCh chan interface{}) *ShardGroup {

	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"shardGroupName": name,
		"volumeName":     volumeName,
		"dataChunks":     dataChunks,
		"parityChunks":   parityChunks,
		"stripSizeKb":    stripSizeKb,
	})

	roundedSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSize != specSize {
		log.Infof("Rounded up size from %v to %v since the size should be a multiple of MiB", specSize, roundedSize)
	}
	log = log.WithField("specSize", roundedSize)

	lvsName := GetShardGroupLvsName(volumeName)
	headLvolName := volumeName

	return &ShardGroup{
		ctx: ctx,

		Name:       name,
		VolumeName: volumeName,

		DataChunks:       dataChunks,
		ParityChunks:     parityChunks,
		StripSizeKb:      stripSizeKb,
		SpecSize:         roundedSize,
		SalvageRequested: salvageRequested,

		Shards: shards,

		EcBdevName:   GetShardGroupEcBdevName(volumeName),
		LvsName:      lvsName,
		HeadLvolName: headLvolName,
		HeadAlias:    spdktypes.GetLvolAlias(lvsName, headLvolName),
		Nqn:          helpertypes.GetNQN(headLvolName),

		SnapshotMap: map[string]*api.Lvol{},

		State: types.InstanceStatePending,

		UpdateCh: updateCh,

		log: safelog.NewSafeLogger(log),
	}
}

// ServiceShardGroupToProtoShardGroup converts in-memory ShardGroup state to the
// gRPC response message. EcStatus is left nil here; it is populated by
// ShardGroup.Get from live SPDK state, only when the ShardGroup is Running
// (the EC bdev exists only after a successful Create).
//
// Head and snapshot lvols route through serviceShardGroupLvolToProtoLvol, which
// renames the head to types.VolumeHead; without it the engine's ancestor
// selection rejects the EC upstream and leaves e.Head, e.SnapshotMap, and
// e.ActualSize unwritten.
func ServiceShardGroupToProtoShardGroup(sg *ShardGroup) *spdkrpc.ShardGroup {
	snapshots := make(map[string]*spdkrpc.Lvol, len(sg.SnapshotMap))
	for snapshotName, snapshotLvol := range sg.SnapshotMap {
		snapshots[snapshotName] = serviceShardGroupLvolToProtoLvol(sg.HeadLvolName, snapshotLvol)
	}

	return &spdkrpc.ShardGroup{
		Name:         sg.Name,
		VolumeName:   sg.VolumeName,
		SpecSize:     sg.SpecSize,
		DataChunks:   sg.DataChunks,
		ParityChunks: sg.ParityChunks,
		StripSizeKb:  sg.StripSizeKb,
		EcBdevName:   sg.EcBdevName,
		LvsName:      sg.LvsName,
		LvsUuid:      sg.LvsUUID,
		HeadLvolName: sg.HeadLvolName,
		HeadLvolUuid: sg.HeadLvolUUID,
		NvmfNqn:      sg.Nqn,
		Ip:           sg.IP,
		Port:         sg.Port,
		ProcessState: instanceStateToProcessState(sg.State),
		ErrorMsg:     sg.ErrorMsg,
		ActualSize:   sg.ActualSize,
		Head:         serviceShardGroupLvolToProtoLvol(sg.HeadLvolName, sg.Head),
		Snapshots:    snapshots,
	}
}

// serviceShardGroupLvolToProtoLvol is the EC analog of ServiceLvolToProtoLvol.
// It carries the same engine-facing contract - the head lvol's Name is reported
// as types.VolumeHead, and any Parent / Children reference to the head bdev is
// rewritten to the same constant - so engine code that compares against
// types.VolumeHead works uniformly across RAID1 and EC topologies.
//
// EC snapshot lvols carry no replica-prefix (unlike v2 replication), so no
// name stripping is required; only the head identity is rewritten.
//
// Children is allocated fresh rather than mutated in place: api.LvolToProtoLvol
// copies the Children map reference, so in-place rewriting would alias back
// into the in-memory sg.Head.Children / sg.SnapshotMap[*].Children and corrupt
// the cache across calls.
func serviceShardGroupLvolToProtoLvol(headLvolName string, lvol *api.Lvol) *spdkrpc.Lvol {
	p := api.LvolToProtoLvol(lvol)
	if p == nil {
		return nil
	}
	if p.Name == headLvolName {
		p.Name = types.VolumeHead
	}
	if p.Parent == headLvolName {
		p.Parent = types.VolumeHead
	}
	if len(p.Children) > 0 {
		newChildren := make(map[string]bool, len(p.Children))
		for k, v := range p.Children {
			if k == headLvolName {
				newChildren[types.VolumeHead] = v
			} else {
				newChildren[k] = v
			}
		}
		p.Children = newChildren
	}
	return p
}

func instanceStateToProcessState(s types.InstanceState) string {
	switch s {
	case types.InstanceStateRunning:
		return "running"
	case types.InstanceStateError:
		return "error"
	default:
		return "stopped"
	}
}

func copyEcCountersFromBdevInfo(status *spdkrpc.EcStatus, info *spdktypes.BdevEcInfo) {
	status.UnmapsSubmitted = info.UnmapsSubmitted
	status.UnmapsCompleted = info.UnmapsCompleted
	status.UnmapsDeferredBusy = info.UnmapsDeferredBusy
	status.UnmapsViaWriteZeros = info.UnmapsViaWriteZeros
	status.UnmapFanoutMisses = info.UnmapFanoutMisses
	status.UnmappedStripes = info.UnmappedStripes
	status.DegradedReadEioDirty = info.DegradedReadEioDirty
	status.DegradedReadsReconstructed = info.DegradedReadsReconstructed
	status.RmwTotal = info.RmwTotal
	status.RmwDeferredScrub = info.RmwDeferredScrub
	status.RmwDeferredDirty = info.RmwDeferredDirty
	status.RmwDeferredInflight = info.RmwDeferredInflight
	status.FullStripeWrites = info.FullStripeWrites
	status.FullStripeWritesDeferred = info.FullStripeWritesDeferred
	status.UnmapsFailed = info.UnmapsFailed
	status.UnmappedReadsSynthesized = info.UnmappedReadsSynthesized
	status.WritesIntoUnmapped = info.WritesIntoUnmapped
	status.WritesIntoUnmappedFailed = info.WritesIntoUnmappedFailed
}

// getEcBdevInfoNoLock returns the single EC bdev backing this shardgroup,
// erroring if SPDK reports zero or multiple. The caller must hold sg's lock.
func (sg *ShardGroup) getEcBdevInfoNoLock(spdkClient *spdkclient.Client) (*spdktypes.BdevEcInfo, error) {
	bdevList, err := spdkClient.BdevEcGetBdevs(sg.EcBdevName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get EC bdev info for shardgroup %s", sg.Name)
	}
	if len(bdevList) != 1 {
		return nil, fmt.Errorf("expected exactly one EC bdev for shardgroup %s, got %d", sg.Name, len(bdevList))
	}
	return &bdevList[0], nil
}

// Get returns the proto ShardGroup enriched with live EC state queried from
// SPDK. Base fields come from the in-memory cache via
// ServiceShardGroupToProtoShardGroup; EcStatus is populated only when the
// ShardGroup is Running (the EC bdev exists only after a successful Create),
// and is otherwise nil - which the controller treats as "no live state
// available yet" rather than as healthy/degraded.
func (sg *ShardGroup) Get(spdkClient *spdkclient.Client) (*spdkrpc.ShardGroup, error) {
	sg.RLock()
	defer sg.RUnlock()

	proto := ServiceShardGroupToProtoShardGroup(sg)
	if sg.State != types.InstanceStateRunning {
		return proto, nil
	}

	ecStatus := &spdkrpc.EcStatus{}

	ecInfo, err := sg.getEcBdevInfoNoLock(spdkClient)
	if err != nil {
		return nil, err
	}
	copyEcCountersFromBdevInfo(ecStatus, ecInfo)

	unmapStatus, err := spdkClient.BdevEcGetUnmapStatus(sg.EcBdevName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get unmap bitmap status for shardgroup %s", sg.Name)
	}
	ecStatus.UnmapBitmapStatus = &spdkrpc.EcUnmapBitmapStatus{
		Generation:     unmapStatus.Generation,
		BlobBytes:      unmapStatus.BlobBytes,
		NumStripes:     unmapStatus.NumStripes,
		ActiveCopy:     unmapStatus.ActiveCopy,
		PersistPending: unmapStatus.PersistPending,
	}

	proto.EcStatus = ecStatus
	return proto, nil
}

// refreshECSnapshotMapNoLock rebuilds the in-memory head/snapshot cache for
// ShardGroupGet from live lvol bdevs in this ShardGroup's lvstore. Caller must
// hold the shardgroup lock.
//
// Per-lvol ActualSize is NumAllocatedClusters * sg.ClusterSize. The cluster
// size MUST come from the lvstore (queried at create/discovery time, cached on
// sg.ClusterSize) rather than from the defaultClusterSize constant - the
// EC-stack lvstore is created with cluster_sz=0 so SPDK picks its own default
// (currently 4 MiB), and using defaultClusterSize (1 MiB) would silently
// report every size as 1/4 of physical.
//
// sg.ActualSize sums head + all snapshots, mirroring the RAID1 path at
// replica.go. Reporting only the head's allocation would drop to ~0 every
// time a snapshot moves the previously-written data into a snapshot lvol.
func (sg *ShardGroup) refreshECSnapshotMapNoLock(spdkClient *spdkclient.Client) error {
	if sg.ClusterSize == 0 {
		return fmt.Errorf("BUG: shardgroup %s has zero ClusterSize; must be populated before refreshECSnapshotMapNoLock", sg.Name)
	}

	filter := func(b *spdktypes.BdevInfo) bool {
		if b.DriverSpecific == nil || b.DriverSpecific.Lvol == nil {
			return false
		}
		return b.DriverSpecific.Lvol.LvolStoreUUID == sg.LvsUUID
	}

	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, filter)
	if err != nil {
		return errors.Wrapf(err, "failed to get lvol map for shardgroup %s", sg.Name)
	}

	newSnapshotMap := map[string]*api.Lvol{}
	var newHead *api.Lvol

	for lvolName, bdev := range bdevLvolMap {
		if bdev.DriverSpecific == nil || bdev.DriverSpecific.Lvol == nil {
			continue
		}

		svcLvol := &api.Lvol{
			Name:              lvolName,
			SpecSize:          bdev.NumBlocks * uint64(bdev.BlockSize),
			ActualSize:        bdev.DriverSpecific.Lvol.NumAllocatedClusters * sg.ClusterSize,
			Parent:            bdev.DriverSpecific.Lvol.BaseSnapshot,
			Children:          map[string]bool{},
			CreationTime:      bdev.CreationTime,
			UserCreated:       bdev.DriverSpecific.Lvol.Xattrs[spdkclient.UserCreated] == strconv.FormatBool(true),
			SnapshotTimestamp: bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotTimestamp],
			SnapshotChecksum:  bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum],
		}

		if lvolName == sg.HeadLvolName {
			newHead = svcLvol
			continue
		}

		if bdev.DriverSpecific.Lvol.Snapshot {
			newSnapshotMap[lvolName] = svcLvol
		}
	}

	if newHead == nil {
		return fmt.Errorf("failed to find head lvol %s in lvstore %s", sg.HeadLvolName, sg.LvsName)
	}

	for snapshotName, snapshotLvol := range newSnapshotMap {
		if snapshotLvol.Parent == "" {
			continue
		}
		if snapshotLvol.Parent == sg.HeadLvolName {
			continue
		}

		if parentSnapshot, ok := newSnapshotMap[snapshotLvol.Parent]; ok {
			parentSnapshot.Children[snapshotName] = true
		}
	}

	// Link the head as a child of its parent snapshot. Without this, consumers
	// walking the tree from the latest snapshot can't find the head - they'd
	// only see snapshot-to-snapshot edges. Matches RAID1's pattern in
	// replica.go. The proto converter (serviceShardGroupLvolToProtoLvol)
	// renames the head bdev to types.VolumeHead at the wire boundary, so the
	// engine sees the same shape as RAID1.
	if newHead.Parent != "" {
		if parentSnapshot, ok := newSnapshotMap[newHead.Parent]; ok {
			parentSnapshot.Children[sg.HeadLvolName] = true
		}
	}

	sg.Head = newHead
	sg.SnapshotMap = newSnapshotMap
	sg.ActualSize = newHead.ActualSize
	for _, snapLvol := range newSnapshotMap {
		sg.ActualSize += snapLvol.ActualSize
	}

	return nil
}
