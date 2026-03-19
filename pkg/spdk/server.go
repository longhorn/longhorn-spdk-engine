package spdk

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	butil "github.com/longhorn/backupstore/util"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util/broadcaster"
)

const (
	MonitorInterval            = 3 * time.Second
	replicaAddPhaseMetadata    = "x-longhorn-replica-add-phase"
	replicaAddPhaseStart       = "start"
	replicaAddPhaseShallowCopy = "shallow-copy"
	replicaAddPhaseFinish      = "finish"

	// Metadata keys used by EngineFrontendReplicaAdd to pass EF callback info
	// so that EngineReplicaAdd can call back for suspend/resume via gRPC.
	replicaAddEFNameMetadata    = "x-longhorn-engine-frontend-name"
	replicaAddEFAddressMetadata = "x-longhorn-engine-frontend-address"

	// Metadata keys for the replica-add completion callback.
	// Engine → EngineFrontend notification that the async replica add finished.
	replicaAddCompleteMetadata      = "x-longhorn-replica-add-complete"
	replicaAddCompleteErrorMetadata = "x-longhorn-replica-add-complete-error"
)

type Server struct {
	spdkrpc.UnimplementedSPDKServiceServer
	sync.RWMutex

	diskCreateLock sync.Mutex
	hotplugActive  atomic.Bool // use atomic.Bool to avoid data races across goroutines.

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *commonbitmap.Bitmap

	diskMap           map[string]*Disk
	replicaMap        map[string]*Replica
	engineMap         map[string]*Engine
	engineFrontendMap map[string]*EngineFrontend

	backupMap map[string]*Backup

	// We store BackingImage in each lvstore
	backingImageMap map[string]*BackingImage

	broadcasters map[types.InstanceType]*broadcaster.Broadcaster
	broadcastChs map[types.InstanceType]chan interface{}
	updateChs    map[types.InstanceType]chan interface{}

	currentBdevIostat *spdktypes.BdevIostatResponse
	bdevMetricMap     map[string]*spdkrpc.Metrics

	// metadataDir is the base path for persisting engine frontend records
	// (e.g. /var/lib/longhorn). If empty, persistence is disabled.
	metadataDir string
}

func NewServer(ctx context.Context, portStart, portEnd int32) (*Server, error) {
	cli, err := spdkclient.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bitmap, err := commonbitmap.NewBitmap(portStart, portEnd)
	if err != nil {
		return nil, err
	}

	if _, err = cli.BdevNvmeSetOptions(
		replicaCtrlrLossTimeoutSec,
		replicaReconnectDelaySec,
		replicaFastIOFailTimeoutSec,
		replicaTransportAckTimeout,
		replicaKeepAliveTimeoutMs); err != nil {
		return nil, errors.Wrap(err, "failed to set NVMe options")
	}

	broadcasters := map[types.InstanceType]*broadcaster.Broadcaster{}
	broadcastChs := map[types.InstanceType]chan interface{}{}
	updateChs := map[types.InstanceType]chan interface{}{}
	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine, types.InstanceTypeEngineFrontend, types.InstanceTypeBackingImage} {
		broadcasters[t] = &broadcaster.Broadcaster{}
		broadcastChs[t] = make(chan interface{})
		updateChs[t] = make(chan interface{})
	}

	s := &Server{
		ctx: ctx,

		hotplugActive: atomic.Bool{},

		spdkClient:    cli,
		portAllocator: bitmap,

		diskMap: map[string]*Disk{},

		replicaMap:        map[string]*Replica{},
		engineMap:         map[string]*Engine{},
		engineFrontendMap: map[string]*EngineFrontend{},

		backupMap: map[string]*Backup{},

		backingImageMap: map[string]*BackingImage{},

		broadcasters: broadcasters,
		broadcastChs: broadcastChs,
		updateChs:    updateChs,

		metadataDir: types.MetadataDir,
	}
	s.hotplugActive.Store(true)

	if _, err := s.broadcasters[types.InstanceTypeReplica].Subscribe(ctx, s.replicaBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngine].Subscribe(ctx, s.engineBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngineFrontend].Subscribe(ctx, s.engineFrontendBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeBackingImage].Subscribe(ctx, s.backingImageBroadcastConnector); err != nil {
		return nil, err
	}

	// Start broadcasting before recovery so that UpdateCh sends inside
	// RecoverFromHost do not block on the unbuffered channel.
	go s.broadcasting()

	s.recoverEngineFrontends()

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoring()

	return s, nil
}

func (s *Server) monitoring() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			err := s.verify()
			if err == nil {
				break
			}

			logrus.WithError(err).Errorf("spdk gRPC server: failed to verify and update replica cache, will retry later")

			if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) || jsonrpc.IsJSONRPCRespErrorInvalidCharacter(err) {
				err = s.tryEnsureSPDKTgtConnectionHealthy()
				if err != nil {
					logrus.WithError(err).Error("spdk gRPC server: failed to ensure spdk_tgt connection healthy")
				}
			}
		}
		if done {
			break
		}
	}
}

func (s *Server) tryEnsureSPDKTgtConnectionHealthy() error {
	running, err := util.IsSPDKTargetProcessRunning()
	if err != nil {
		return errors.Wrap(err, "failed to check spdk_tgt is running")
	}
	if !running {
		return errors.New("spdk_tgt is not running")
	}

	logrus.Info("spdk gRPC server: reconnecting to spdk_tgt")
	return s.clientReconnect()
}

func (s *Server) clientReconnect() error {
	s.Lock()
	defer func() {
		s.Unlock()
	}()

	oldClient := s.spdkClient

	client, err := spdkclient.NewClient(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create new SPDK client")
	}
	s.spdkClient = client

	// Try the best effort to close the old client after a new client is created
	err = oldClient.Close()
	if err != nil {
		logrus.WithError(err).Warn("Failed to close old SPDK client")
	}
	return nil
}

type verifyState struct {
	replicaMap            map[string]*Replica
	replicaMapForSync     map[string]*Replica
	engineMapForSync      map[string]*Engine
	engineFrontendForSync map[string]*EngineFrontend
	backingImageMap       map[string]*BackingImage
	backingImageForSync   map[string]*BackingImage
	spdkClient            *spdkclient.Client
}

func (s *Server) verify() (err error) {
	s.Lock()
	locked := true
	defer func() {
		if locked {
			s.Unlock()
		}
	}()

	state := s.newVerifyState()

	defer func() {
		s.handleVerifyError(err, state)
	}()

	s.trySelfHealHotplug()

	if err = s.rebuildCachedLvolObjects(state); err != nil {
		return err
	}

	if len(s.replicaMap) != len(state.replicaMap) {
		logrus.Infof("spdk gRPC server: replica map updated, map count is changed from %d to %d", len(s.replicaMap), len(state.replicaMap))
	}

	s.replicaMap = state.replicaMap
	s.backingImageMap = state.backingImageMap
	s.UpdateEngineMetrics()

	s.Unlock()
	locked = false

	return s.syncVerifiedObjects(state)
}

func (s *Server) newVerifyState() *verifyState {
	state := &verifyState{
		replicaMap:            map[string]*Replica{},
		replicaMapForSync:     map[string]*Replica{},
		engineMapForSync:      map[string]*Engine{},
		engineFrontendForSync: map[string]*EngineFrontend{},
		backingImageMap:       map[string]*BackingImage{},
		backingImageForSync:   map[string]*BackingImage{},
		spdkClient:            s.spdkClient,
	}

	for k, v := range s.replicaMap {
		state.replicaMap[k] = v
		state.replicaMapForSync[k] = v
	}
	for k, v := range s.engineMap {
		state.engineMapForSync[k] = v
	}
	for k, v := range s.engineFrontendMap {
		state.engineFrontendForSync[k] = v
	}
	for k, v := range s.backingImageMap {
		state.backingImageMap[k] = v
		state.backingImageForSync[k] = v
	}

	return state
}

func (s *Server) handleVerifyError(err error, state *verifyState) {
	if err == nil {
		return
	}
	if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
		logrus.WithError(err).Warn("spdk gRPC server: marking all non-stopped and non-error replicas and engines as error")
		for _, r := range state.replicaMapForSync {
			r.SetErrorState()
		}
		for _, e := range state.engineMapForSync {
			e.SetErrorState()
		}
		for _, ef := range state.engineFrontendForSync {
			ef.SetErrorState()
		}
	}
}

func (s *Server) trySelfHealHotplug() {
	// Self-heal: re-enable hotplug only if no disks are being created and the last enablement failed.
	isDiskCreating := false
	for _, disk := range s.diskMap {
		if disk.GetState() == DiskStateCreating {
			isDiskCreating = true
			break
		}
	}
	if !isDiskCreating && !s.hotplugActive.Load() {
		if success := setNvmeHotPlug(s.spdkClient, true); success {
			s.hotplugActive.Store(true)
		}
	}
}

func buildBdevLvolMap(bdevList []spdktypes.BdevInfo) map[string]*spdktypes.BdevInfo {
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		if spdktypes.GetBdevType(bdev) != spdktypes.BdevTypeLvol {
			continue
		}
		if len(bdev.Aliases) != 1 {
			continue
		}
		bdevLvolMap[spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])] = bdev
	}
	return bdevLvolMap
}

func buildLvsUUIDNameMap(lvsList []spdktypes.LvstoreInfo) map[string]string {
	lvsUUIDNameMap := map[string]string{}
	for _, lvs := range lvsList {
		lvsUUIDNameMap[lvs.UUID] = lvs.Name
	}
	return lvsUUIDNameMap
}

func (s *Server) rebuildCachedLvolObjects(state *verifyState) error {
	bdevList, err := state.spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return err
	}
	bdevLvolMap := buildBdevLvolMap(bdevList)

	lvsList, err := state.spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return err
	}
	lvsUUIDNameMap := buildLvsUUIDNameMap(lvsList)

	// Detect if the lvol bdev is an uncached replica or backing image.
	for lvolName, bdevLvol := range bdevLvolMap {
		if bdevLvol.DriverSpecific.Lvol.Snapshot && !types.IsBackingImageSnapLvolName(lvolName) {
			continue
		}
		if types.IsBackingImageTempHead(lvolName) {
			if state.backingImageMap[types.GetBackingImageSnapLvolNameFromTempHeadLvolName(lvolName)] == nil {
				lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
				logrus.Infof("Found one backing image temp head lvol %v while there is no backing image record in the server", lvolName)
				if err := cleanupOrphanBackingImageTempHead(state.spdkClient, lvsUUIDNameMap[lvsUUID], lvolName); err != nil {
					logrus.WithError(err).Warnf("Failed to clean up orphan backing image temp head")
				}
			}
			continue
		}
		if state.replicaMap[lvolName] != nil {
			continue
		}
		if state.backingImageMap[lvolName] != nil {
			continue
		}
		if IsRebuildingLvol(lvolName) {
			if state.replicaMap[GetReplicaNameFromRebuildingLvolName(lvolName)] != nil {
				continue
			}
		}
		if IsCloningLvol(lvolName) {
			if state.replicaMap[GetReplicaNameFromCloningLvolName(lvolName)] != nil {
				continue
			}
		}
		if types.IsBackingImageSnapLvolName(lvolName) {
			lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
			backingImageName, _, err := ExtractBackingImageAndDiskUUID(lvolName)
			if err != nil {
				logrus.WithError(err).Warnf("failed to extract backing image name and disk UUID from lvol name %v", lvolName)
				continue
			}
			size := bdevLvol.NumBlocks * uint64(bdevLvol.BlockSize)
			alias := bdevLvol.Aliases[0]
			expectedChecksum, err := GetSnapXattr(state.spdkClient, alias, types.LonghornBackingImageSnapshotAttrChecksum)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve checksum attribute for backing image snapshot %v", alias)
				continue
			}
			backingImageUUID, err := GetSnapXattr(state.spdkClient, alias, types.LonghornBackingImageSnapshotAttrUUID)
			if err != nil {
				logrus.WithError(err).Warnf("failed to retrieve backing image UUID attribute for snapshot %v", alias)
				continue
			}
			backingImage := NewBackingImage(s.ctx, backingImageName, backingImageUUID, lvsUUID, size, expectedChecksum, s.updateChs[types.InstanceTypeBackingImage])
			backingImage.Alias = alias
			backingImage.State = types.BackingImageStatePending
			state.backingImageForSync[lvolName] = backingImage
			state.backingImageMap[lvolName] = backingImage
		} else if IsProbablyReplicaName(lvolName) {
			lvsUUID := bdevLvol.DriverSpecific.Lvol.LvolStoreUUID
			specSize := bdevLvol.NumBlocks * uint64(bdevLvol.BlockSize)
			actualSize := bdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * uint64(defaultClusterSize)
			state.replicaMap[lvolName] = NewReplica(s.ctx, lvolName, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, true, s.updateChs[types.InstanceTypeReplica])
			state.replicaMapForSync[lvolName] = state.replicaMap[lvolName]
			logrus.Infof("Detected one possible existing replica %s(%s) with disk %s(%s), spec size %d, actual size %d", bdevLvol.Aliases[0], bdevLvol.UUID, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, actualSize)
		}
	}

	// Remove replicas from the cache if their lvol bdevs are gone.
	for replicaName, r := range state.replicaMap {
		// Try the best to avoid eliminating broken replicas or rebuilding replicas
		if bdevLvolMap[r.Name] == nil {
			if r.IsRebuilding() {
				continue
			}
			noReplicaLvol := true
			for lvolName := range bdevLvolMap {
				if IsReplicaLvol(r.Name, lvolName) {
					noReplicaLvol = false
					break
				}
			}
			if noReplicaLvol {
				delete(state.replicaMap, replicaName)
				delete(state.replicaMapForSync, replicaName)
			}
		}
	}

	return nil
}

func (s *Server) syncVerifiedObjects(state *verifyState) error {
	for _, r := range state.replicaMapForSync {
		if err := r.Sync(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, e := range state.engineMapForSync {
		if err := e.ValidateAndUpdate(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, ef := range state.engineFrontendForSync {
		if err := ef.ValidateAndUpdate(state.spdkClient); err != nil && jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
			return err
		}
	}

	for _, bi := range state.backingImageForSync {
		if err := bi.ValidateAndUpdate(state.spdkClient); err != nil {
			if jsonrpc.IsJSONRPCRespErrorBrokenPipe(err) {
				return err
			}
			continue
		}
	}

	// TODO: send update signals if there is a Replica/Replica change
	return nil
}

func (s *Server) broadcasting() {
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped broadcasting instances due to the context done")
			// Keep draining updateChs so that senders on unbuffered channels
			// do not block forever after broadcasting stops forwarding.
			// Other goroutines will eventually observe ctx.Done() and stop sending.
			for {
				select {
				case <-s.updateChs[types.InstanceTypeReplica]:
				case <-s.updateChs[types.InstanceTypeEngine]:
				case <-s.updateChs[types.InstanceTypeEngineFrontend]:
				case <-s.updateChs[types.InstanceTypeBackingImage]:
				}
			}
		case <-s.updateChs[types.InstanceTypeReplica]:
			s.broadcastChs[types.InstanceTypeReplica] <- nil
		case <-s.updateChs[types.InstanceTypeEngine]:
			s.broadcastChs[types.InstanceTypeEngine] <- nil
		case <-s.updateChs[types.InstanceTypeEngineFrontend]:
			s.broadcastChs[types.InstanceTypeEngineFrontend] <- nil
		case <-s.updateChs[types.InstanceTypeBackingImage]:
			s.broadcastChs[types.InstanceTypeBackingImage] <- nil
		}
	}
}

func (s *Server) Subscribe(instanceType types.InstanceType) (<-chan interface{}, error) {
	switch instanceType {
	case types.InstanceTypeEngine:
		return s.broadcasters[types.InstanceTypeEngine].Subscribe(context.TODO(), s.engineBroadcastConnector)
	case types.InstanceTypeEngineFrontend:
		return s.broadcasters[types.InstanceTypeEngineFrontend].Subscribe(context.TODO(), s.engineFrontendBroadcastConnector)
	case types.InstanceTypeReplica:
		return s.broadcasters[types.InstanceTypeReplica].Subscribe(context.TODO(), s.replicaBroadcastConnector)
	case types.InstanceTypeBackingImage:
		return s.broadcasters[types.InstanceTypeBackingImage].Subscribe(context.TODO(), s.backingImageBroadcastConnector)
	}
	return nil, fmt.Errorf("invalid instance type %v for subscription", instanceType)
}

func (s *Server) replicaBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeReplica], nil
}

func (s *Server) engineBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeEngine], nil
}

func (s *Server) engineFrontendBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeEngineFrontend], nil
}

func (s *Server) backingImageBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeBackingImage], nil
}

func (s *Server) isLvsExist(lvsUUID, lvsName string) (bool, error) {
	if lvsUUID == "" && lvsName == "" {
		return false, fmt.Errorf("either lvstore UUID or name must be provided")
	}

	name := ""
	uuid := ""

	if lvsUUID != "" {
		uuid = lvsUUID
	} else {
		name = lvsName
	}

	lvsList, err := s.spdkClient.BdevLvolGetLvstore(name, uuid)
	if err != nil {
		return false, err
	}

	if len(lvsList) == 0 {
		return false, fmt.Errorf("found zero lvstore with name %q and UUID %q", lvsName, lvsUUID)
	}

	return true, nil
}

func (s *Server) newReplica(req *spdkrpc.ReplicaCreateRequest) (*Replica, error) {
	s.Lock()
	defer func() {
		s.Unlock()
	}()

	r, ok := s.replicaMap[req.Name]
	if ok {
		return r, nil
	}

	exists, err := s.isLvsExist(req.LvsUuid, req.LvsName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check lvstore %v(%v) existence for replica %v creation", req.LvsName, req.LvsUuid, req.Name)
	}
	if !exists {
		return nil, fmt.Errorf("lvstore %v(%v) does not exist for replica %v creation", req.LvsName, req.LvsUuid, req.Name)
	}
	return NewReplica(s.ctx, req.Name, req.LvsName, req.LvsUuid, req.SpecSize, true, s.updateChs[types.InstanceTypeReplica]), nil
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.LvsName == "" && req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "either lvstore name or UUID is required")
	}

	r, err := s.newReplica(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		// Always update the replica map
		s.Lock()
		s.replicaMap[req.Name] = r
		s.Unlock()
	}()

	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	var backingImage *BackingImage
	if req.BackingImageName != "" {
		backingImage, err = s.getBackingImage(req.BackingImageName, req.LvsUuid)
		if err != nil {
			return nil, err
		}
	}

	return r.Create(spdkClient, req.PortCount, s.portAllocator, backingImage)
}

func (s *Server) getBackingImage(backingImageName, lvsUUID string) (backingImage *BackingImage, err error) {
	backingImageSnapLvolName := GetBackingImageSnapLvolName(backingImageName, lvsUUID)

	s.RLock()
	backingImage = s.backingImageMap[backingImageSnapLvolName]
	s.RUnlock()

	if backingImage == nil {
		return nil, grpcstatus.Error(grpccodes.NotFound, "failed to find the backing image in the spdk server")
	}

	return backingImage, nil
}

// ReplicaDelete deletes a replica
func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil && req.CleanupRequired {
			s.Lock()
			delete(s.replicaMap, req.Name)
			s.Unlock()
		}
	}()

	if r != nil {
		if err := r.Delete(spdkClient, req.CleanupRequired, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// ReplicaGet returns a specific replica
func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	return r.Get(), nil
}

// ReplicaExpand expands a replica
func (s *Server) ReplicaExpand(ctx context.Context, req *spdkrpc.ReplicaExpandRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	if err := r.Expand(spdkClient, req.Size); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaList returns all replicas
func (s *Server) ReplicaList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.ReplicaListResponse, error) {
	replicaMap := map[string]*Replica{}
	res := map[string]*spdkrpc.Replica{}

	s.RLock()
	for k, v := range s.replicaMap {
		replicaMap[k] = v
	}
	s.RUnlock()

	for replicaName, r := range replicaMap {
		res[replicaName] = r.Get()
	}

	return &spdkrpc.ReplicaListResponse{Replicas: res}, nil
}

// ReplicaWatch returns a stream of replica updates
func (s *Server) ReplicaWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_ReplicaWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeReplica)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service replica watch errored out")
		} else {
			logrus.Info("SPDK service replica watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service replica update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped replica watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

// ReplicaSnapshotCreate creates a snapshot for a replica
func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot create", req.Name)
	}

	opts := &api.SnapshotOptions{
		UserCreated: req.UserCreated,
		Timestamp:   req.SnapshotTimestamp,
	}

	return r.SnapshotCreate(spdkClient, req.SnapshotName, opts)
}

// ReplicaSnapshotDelete deletes a snapshot for a replica
func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot delete", req.Name)
	}

	_, err = r.SnapshotDelete(spdkClient, req.SnapshotName)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotRevert reverts a snapshot for a replica
func (s *Server) ReplicaSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot revert", req.Name)
	}

	_, err = r.SnapshotRevert(spdkClient, req.SnapshotName)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotPurge purges all snapshots for a replica
func (s *Server) ReplicaSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot purge", req.Name)
	}

	err = r.SnapshotPurge(spdkClient)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotHash hashes a snapshot for a replica
func (s *Server) ReplicaSnapshotHash(ctx context.Context, req *spdkrpc.SnapshotHashRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot hash", req.Name)
	}

	err = r.SnapshotHash(spdkClient, req.SnapshotName, req.Rehash)
	return &emptypb.Empty{}, err
}

// ReplicaSnapshotHashStatus returns the hash status of a snapshot for a replica
func (s *Server) ReplicaSnapshotHashStatus(ctx context.Context, req *spdkrpc.SnapshotHashStatusRequest) (ret *spdkrpc.ReplicaSnapshotHashStatusResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for snapshot hash status", req.Name)
	}

	state, checksum, errMsg, silentlyCorrupted, err := r.SnapshotHashStatus(req.SnapshotName)
	return &spdkrpc.ReplicaSnapshotHashStatusResponse{
		State:             state,
		Checksum:          checksum,
		Error:             errMsg,
		SilentlyCorrupted: silentlyCorrupted,
	}, err
}

// ReplicaSnapshotRangeHashGet returns the range hash of a snapshot for a replica
func (s *Server) ReplicaSnapshotRangeHashGet(ctx context.Context, req *spdkrpc.ReplicaSnapshotRangeHashGetRequest) (ret *spdkrpc.ReplicaSnapshotRangeHashGetResponse, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for snapshot range hash get", req.Name)
	}

	rangeHashMap, err := r.SnapshotRangeHashGet(spdkClient, req.SnapshotName, req.ClusterStartIndex, req.ClusterCount)
	return &spdkrpc.ReplicaSnapshotRangeHashGetResponse{
		RangeHashMap: rangeHashMap,
	}, err
}

// ReplicaSnapshotCloneDstStart starts a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneDstStart(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneDstStartRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "srcReplicaName", Value: req.SrcReplicaName},
		util.Param{Name: "srcReplicaAddress", Value: req.SrcReplicaAddress},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneDstStart", req.Name)
	}

	if err := r.SnapshotCloneDstStart(spdkClient, req.SnapshotName, req.SrcReplicaName, req.SrcReplicaAddress, req.CloneMode); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to do SnapshotCloneDstStart during ReplicaSnapshotCloneDstStart")
	}
	return &emptypb.Empty{}, nil
}

// ReplicaSnapshotCloneDstStatusCheck checks the status of a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneDstStatusCheck(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneDstStatusCheckRequest) (ret *spdkrpc.ReplicaSnapshotCloneDstStatusCheckResponse, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneDstStatusCheck", req.Name)
	}

	return r.SnapshotCloneDstStatusCheck()
}

// ReplicaSnapshotCloneSrcStart starts a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcStart(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcStartRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcStart", req.Name)
	}

	if err := r.SnapshotCloneSrcStart(spdkClient, req.SnapshotName, req.DstReplicaName, req.DstCloningLvolAddress, req.CloneMode); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaSnapshotCloneSrcStatusCheck checks the status of a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcStatusCheck(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcStatusCheckRequest) (ret *spdkrpc.ReplicaSnapshotCloneSrcStatusCheckResponse, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcStatusCheck", req.Name)
	}

	return r.SnapshotCloneSrcStatusCheck(spdkClient, req.SnapshotName, req.DstReplicaName)
}

// ReplicaSnapshotCloneSrcFinish finishes a clone for a snapshot for a replica
func (s *Server) ReplicaSnapshotCloneSrcFinish(ctx context.Context, req *spdkrpc.ReplicaSnapshotCloneSrcFinishRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "dstReplicaName", Value: req.DstReplicaName},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during ReplicaSnapshotCloneSrcFinish", req.Name)
	}

	if err := r.SnapshotCloneSrcFinish(spdkClient, req.DstReplicaName); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcStart starts a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcStartRequest) (ret *spdkrpc.ReplicaRebuildingSrcStartResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.DstReplicaName == "" || req.DstReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst replica name and address are required")
	}
	if req.ExposedSnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "src replica exposed snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src start", req.Name)
	}

	exposedSnapshotLvolAddress, err := r.RebuildingSrcStart(spdkClient, req.DstReplicaName, req.DstReplicaAddress, req.ExposedSnapshotName)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.ReplicaRebuildingSrcStartResponse{ExposedSnapshotLvolAddress: exposedSnapshotLvolAddress}, nil
}

// ReplicaRebuildingSrcFinish finishes a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcFinish(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcFinishRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.DstReplicaName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src finish", req.Name)
	}

	if err = r.RebuildingSrcFinish(spdkClient, req.DstReplicaName); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcShallowCopyStart starts a shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}
	if req.DstRebuildingLvolAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst rebuilding lvol address is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s shallow copy start", req.Name, req.SnapshotName)
	}

	if err := r.RebuildingSrcShallowCopyStart(spdkClient, req.SnapshotName, req.DstRebuildingLvolAddress); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcRangeShallowCopyStart starts a range shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcRangeShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcRangeShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}
	if req.DstRebuildingLvolAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "dst rebuilding lvol address is required")
	}
	if len(req.MismatchingClusterList) < 1 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "mismatching cluster list is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s range shallow copy start", req.Name, req.SnapshotName)
	}

	if err := r.RebuildingSrcRangeShallowCopyStart(spdkClient, req.SnapshotName, req.DstRebuildingLvolAddress, req.MismatchingClusterList); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingSrcShallowCopyCheck checks the shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingSrcShallowCopyCheck(ctx context.Context, req *spdkrpc.ReplicaRebuildingSrcShallowCopyCheckRequest) (ret *spdkrpc.ReplicaRebuildingSrcShallowCopyCheckResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding src snapshot %s shallow copy check", req.Name, req.SnapshotName)
	}

	return r.RebuildingSrcShallowCopyCheck(req.SnapshotName)
}

// ReplicaRebuildingDstStart starts a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstStartRequest) (ret *spdkrpc.ReplicaRebuildingDstStartResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SrcReplicaName == "" || req.SrcReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "src replica name and address are required")
	}
	if req.ExternalSnapshotName == "" || req.ExternalSnapshotAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "external external snapshot name and address are required")
	}
	if req.RebuildingSnapshotList == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "rebuilding snapshot list is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst start", req.Name)
	}

	var rebuildingSnapshotList []*api.Lvol
	for _, snapshot := range req.RebuildingSnapshotList {
		rebuildingSnapshotList = append(rebuildingSnapshotList, api.ProtoLvolToLvol(snapshot))
	}
	address, err := r.RebuildingDstStart(spdkClient, req.SrcReplicaName, req.SrcReplicaAddress, req.ExternalSnapshotName, req.ExternalSnapshotAddress, rebuildingSnapshotList)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.ReplicaRebuildingDstStartResponse{DstHeadLvolAddress: address}, nil
}

// ReplicaRebuildingDstFinish finishes a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstFinish(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstFinishRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst finish", req.Name)
	}

	if err = r.RebuildingDstFinish(spdkClient); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstShallowCopyStart starts a shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstShallowCopyStart(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstShallowCopyStartRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica snapshot name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot %s shallow copy start", req.Name, req.SnapshotName)
	}

	if err = r.RebuildingDstShallowCopyStart(spdkClient, req.SnapshotName, req.FastSync); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstShallowCopyCheck checks the shallow copy for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstShallowCopyCheck(ctx context.Context, req *spdkrpc.ReplicaRebuildingDstShallowCopyCheckRequest) (ret *spdkrpc.ReplicaRebuildingDstShallowCopyCheckResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot shallow copy check", req.Name)
	}

	return r.RebuildingDstShallowCopyCheck(spdkClient)
}

// ReplicaRebuildingDstSnapshotCreate creates a snapshot for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during rebuilding dst snapshot create", req.Name)
	}

	opts := &api.SnapshotOptions{
		UserCreated: req.UserCreated,
		Timestamp:   req.SnapshotTimestamp,
	}

	if err = r.RebuildingDstSnapshotCreate(spdkClient, req.SnapshotName, opts); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ReplicaRebuildingDstSetQosLimit sets the QoS limit for a rebuilding for a replica
func (s *Server) ReplicaRebuildingDstSetQosLimit(
	ctx context.Context,
	req *spdkrpc.ReplicaRebuildingDstSetQosLimitRequest,
) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.QosLimitMbps < 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "QoS limit must not be negative, got %d", req.QosLimitMbps)
	}

	s.RLock()
	r := s.replicaMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s for QoS setting", req.Name)
	}

	if err := r.RebuildingDstSetQos(spdkClient, req.QosLimitMbps); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to set QoS limit on replica %s: %v", req.Name, err)
	}

	return &emptypb.Empty{}, nil
}

// EngineCreate creates an engine
func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}
	if req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine volume name is required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine spec size is required")
	}

	s.Lock()

	e, ok := s.engineMap[req.Name]
	if ok {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine %v already exists", req.Name)
	}

	if e == nil {
		s.engineMap[req.Name] = NewEngine(req.Name, req.VolumeName, req.Frontend, req.SpecSize, s.updateChs[types.InstanceTypeEngine])
		e = s.engineMap[req.Name]
	}

	spdkClient := s.spdkClient
	s.Unlock()

	return e.Create(spdkClient, req.ReplicaAddressMap, req.PortCount, s.portAllocator, req.SalvageRequested)
}

// EngineDelete deletes an engine
func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.engineMap, req.Name)
			s.Unlock()
		}
	}()

	if e != nil {
		if err := e.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

// EngineGet returns a specific engine
func (s *Server) EngineExpand(ctx context.Context, req *spdkrpc.EngineExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for expansion", req.Name)
	}

	if types.IsUblkFrontend(e.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "cannot expand ublk frontend engine %v", req.Name)
	}

	err = e.Expand(spdkClient, req.Size)
	if err != nil {
		return nil, toExpansionGRPCError(err, "failed to expand engine %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineExpandPrecheck checks if expansion is required for an engine. The engine spec size should be updated before precheck.
func (s *Server) EngineExpandPrecheck(ctx context.Context, req *spdkrpc.EngineExpandPrecheckRequest) (*spdkrpc.EngineExpandPrecheckResponse, error) {
	if req.Name == "" {
		return &spdkrpc.EngineExpandPrecheckResponse{
			ExpansionRequired: false,
		}, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return &spdkrpc.EngineExpandPrecheckResponse{}, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for expansion", req.Name)
	}

	requireExpansion, err := e.ExpandPrecheck(spdkClient, req.Size)
	if err != nil {
		return &spdkrpc.EngineExpandPrecheckResponse{
			ExpansionRequired: false,
		}, toExpansionGRPCError(err, "failed to precheck expand engine %v", req.Name)
	}

	return &spdkrpc.EngineExpandPrecheckResponse{
		ExpansionRequired: requireExpansion,
	}, nil
}

// EngineFrontendSwitchOver switches over the frontend of an engine to a new target address. The engine frontend should be in normal state before switch over.
func (s *Server) EngineFrontendSwitchOver(ctx context.Context, req *spdkrpc.EngineFrontendSwitchOverRequest) (ret *emptypb.Empty, err error) {
	if req == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "request is required")
	}
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend or engine name is required")
	}
	if req.TargetAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "target address is required")
	}
	if targetIP, targetPort, splitErr := splitHostPort(req.TargetAddress); splitErr != nil || targetIP == "" || targetPort == 0 {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid target address %q", req.TargetAddress)
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	if ef == nil {
		// Backward compatible lookup: allow name to be engine name if there is exactly one frontend.
		for _, frontend := range s.engineFrontendMap {
			if frontend.EngineName != req.Name {
				continue
			}
			if ef != nil {
				s.RUnlock()
				return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "multiple engine frontends found for engine %s", req.Name)
			}
			ef = frontend
		}
	}
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend or engine %v for target switchover", req.Name)
	}

	if err := ef.SwitchOverTarget(spdkClient, req.EngineName, req.TargetAddress); err != nil {
		return nil, toSwitchOverGRPCError(err, "failed to switch over target for %s", req.Name)
	}

	return &emptypb.Empty{}, nil
}

func toSwitchOverGRPCError(err error, format string, args ...interface{}) error {
	code := grpccodes.Internal

	// Preserve downstream gRPC code when available.
	if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
		code = statusErr.Code()
	} else {
		switch {
		case errors.Is(err, ErrSwitchOverTargetInvalidInput):
			code = grpccodes.InvalidArgument
		case errors.Is(err, ErrSwitchOverTargetPrecondition):
			code = grpccodes.FailedPrecondition
		case errors.Is(err, ErrSwitchOverTargetEngineNotFound):
			code = grpccodes.NotFound
		case errors.Is(err, context.DeadlineExceeded):
			code = grpccodes.DeadlineExceeded
		case errors.Is(err, context.Canceled):
			code = grpccodes.Canceled
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

// EngineFrontendSuspend suspends an engine frontend. The engine frontend can be resumed later. The engine frontend should be in normal state before suspension.
func (s *Server) EngineFrontendSuspend(ctx context.Context, req *spdkrpc.EngineFrontendSuspendRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for suspension", req.Name)
	}

	err = ef.Suspend(spdkClient)
	if err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to suspend engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendResume resumes an engine frontend. The engine frontend should have been suspended before resumption.
func (s *Server) EngineFrontendResume(ctx context.Context, req *spdkrpc.EngineFrontendResumeRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for resumption", req.Name)
	}

	err = ef.Resume(spdkClient)
	if err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to resume engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineDeleteTarget deletes the target for an engine.
// TODO: The API is currently not implemented and will be removed in the future as target management will be handled by engine frontends instead of the engine itself.
func (s *Server) EngineDeleteTarget(ctx context.Context, req *spdkrpc.EngineDeleteTargetRequest) (ret *emptypb.Empty, err error) {
	return &emptypb.Empty{}, grpcstatus.Error(grpccodes.Unimplemented, "EngineDeleteTarget is not implemented yet and will be removed in the future")
}

// EngineGet returns a specific engine
func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v", req.Name)
	}

	return e.Get(), nil
}

// EngineList returns all engines
func (s *Server) EngineList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.EngineListResponse, error) {
	engineMap := map[string]*Engine{}
	res := map[string]*spdkrpc.Engine{}

	s.RLock()
	for k, v := range s.engineMap {
		engineMap[k] = v
	}
	s.RUnlock()

	for engineName, e := range engineMap {
		res[engineName] = e.Get()
	}

	return &spdkrpc.EngineListResponse{Engines: res}, nil
}

// EngineWatch returns a stream of engine updates
func (s *Server) EngineWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_EngineWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeEngine)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service engine watch errored out")
		} else {
			logrus.Info("SPDK service engine watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service engine update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped engine watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

// Deprecated: EngineReplicaAdd is kept for backward compatibility with clients
// that still multiplex phases via gRPC metadata. New clients should call
// EngineReplicaAddStart, EngineReplicaAddShallowCopy, or EngineReplicaAddFinish
// directly — or use EngineFrontendReplicaAdd which delegates here with EF
// callback metadata for suspend/resume.
func (s *Server) EngineReplicaAdd(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}

	// Check for EF callback metadata — if present, this is a full-flow
	// ReplicaAdd request from EngineFrontendReplicaAdd.
	var efName, efAddress string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(replicaAddEFNameMetadata); len(values) > 0 {
			efName = values[0]
		}
		if values := md.Get(replicaAddEFAddressMetadata); len(values) > 0 {
			efAddress = values[0]
		}
	}

	if efName != "" && efAddress != "" {
		// Full-flow path: Engine owns start → shallow copy → finish
		// with gRPC callback to EngineFrontend for suspend/resume.
		s.RLock()
		e := s.engineMap[req.EngineName]
		spdkClient := s.spdkClient
		s.RUnlock()

		if e == nil {
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s add", req.EngineName, req.ReplicaName, req.ReplicaAddress)
		}

		callbackLog := logrus.WithFields(logrus.Fields{
			"engineName":     req.EngineName,
			"replicaName":    req.ReplicaName,
			"engineFrontend": efName,
		})
		finishWrapper := buildGRPCReplicaAddFinishWrapper(efName, efAddress, callbackLog)
		onComplete := buildGRPCReplicaAddOnComplete(efName, efAddress, callbackLog)
		if err := e.ReplicaAdd(spdkClient, req.ReplicaName, req.ReplicaAddress, req.FastSync, finishWrapper, onComplete); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}

	// Legacy path: phase-based routing via metadata.
	phase := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(replicaAddPhaseMetadata); len(values) > 0 {
			phase = strings.ToLower(values[0])
		}
	}

	switch phase {
	case replicaAddPhaseStart:
		return s.EngineReplicaAddStart(ctx, req)
	case replicaAddPhaseShallowCopy:
		return s.EngineReplicaAddShallowCopy(ctx, req)
	case replicaAddPhaseFinish:
		return s.EngineReplicaAddFinish(ctx, req)
	}

	return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
		"replica add requires phase metadata (deprecated) or use the dedicated EngineReplicaAddStart/ShallowCopy/Finish RPCs")
}

// buildGRPCReplicaAddFinishWrapper builds a replicaAddFinishWrapper that
// calls back to the EngineFrontend on a (potentially remote) node via gRPC
// for suspend/resume around the finish step.
// If the frontend does not support suspend (e.g. NVMf frontend), the suspend
// error is treated as non-fatal and the finish proceeds without suspension.
func buildGRPCReplicaAddFinishWrapper(efName, efAddress string, log *logrus.Entry) replicaAddFinishWrapper {
	return func(finish func() error) error {
		efClient, err := GetServiceClient(efAddress)
		if err != nil {
			return errors.Wrapf(err, "failed to get SPDK client for engine frontend %s at %s for suspend", efName, efAddress)
		}
		defer func() {
			if errClose := efClient.Close(); errClose != nil {
				log.WithError(errClose).Warnf("Failed to close engine frontend SPDK client for %s", efName)
			}
		}()

		// Suspend the frontend before finish.
		// If suspend is not supported (e.g. NVMf or empty frontend), treat as non-fatal.
		suspended := false
		if err := efClient.EngineFrontendSuspend(efName); err != nil {
			if isGRPCUnimplemented(err) {
				log.Infof("Engine frontend %s does not support suspend, proceeding without suspension", efName)
			} else {
				return errors.Wrapf(err, "failed to suspend engine frontend %s before replica add finish", efName)
			}
		} else {
			suspended = true
		}

		finishErr := finish()

		// Resume the frontend after finish.
		if suspended {
			if resumeErr := efClient.EngineFrontendResume(efName); resumeErr != nil {
				resumeErr = errors.Wrapf(resumeErr, "failed to resume engine frontend %s after replica add finish", efName)
				if finishErr != nil {
					return errors.Wrap(finishErr, resumeErr.Error())
				}
				return resumeErr
			}
		}

		return finishErr
	}
}

// buildGRPCReplicaAddOnComplete builds a replicaAddOnComplete callback that
// notifies the EngineFrontend on a (potentially remote) node via gRPC when
// Engine's async replica-add goroutine finishes. The notification clears
// the EF's isReplicaAdding guard and, on failure, transitions it to error state.
func buildGRPCReplicaAddOnComplete(efName, efAddress string, log *logrus.Entry) replicaAddOnComplete {
	return func(replicaAddErr error) {
		efClient, err := GetServiceClient(efAddress)
		if err != nil {
			log.WithError(err).Errorf("Failed to get SPDK client for engine frontend %s at %s for replica add completion", efName, efAddress)
			return
		}
		defer func() {
			if errClose := efClient.Close(); errClose != nil {
				log.WithError(errClose).Warnf("Failed to close engine frontend SPDK client for %s", efName)
			}
		}()

		if notifyErr := efClient.EngineFrontendReplicaAddComplete(efName, replicaAddErr); notifyErr != nil {
			log.WithError(notifyErr).Errorf("Failed to notify engine frontend %s of replica add completion", efName)
		}
	}
}

// isGRPCUnimplemented checks if the error is a gRPC Unimplemented status.
func isGRPCUnimplemented(err error) bool {
	if st, ok := grpcstatus.FromError(err); ok {
		return st.Code() == grpccodes.Unimplemented
	}
	return false
}

func (s *Server) EngineReplicaAddStart(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}

	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s add start", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}
	return &emptypb.Empty{}, e.ReplicaAddStart(spdkClient, req.ReplicaName, req.ReplicaAddress, req.FastSync)
}

func (s *Server) EngineReplicaAddShallowCopy(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s to shallow copy", req.EngineName, req.ReplicaName)
	}
	return &emptypb.Empty{}, e.ReplicaAddShallowCopy(req.ReplicaName)
}

func (s *Server) EngineReplicaAddFinish(ctx context.Context, req *spdkrpc.EngineReplicaAddRequest) (ret *emptypb.Empty, err error) {
	if req.ReplicaName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}

	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s add finish", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}
	return &emptypb.Empty{}, e.ReplicaAddFinish(req.ReplicaName, nil)
}

// EngineFrontendReplicaAdd adds a replica to an engine frontend, or handles
// the async completion callback from Engine's background goroutine.
// When the request carries x-longhorn-replica-add-complete metadata, it is
// treated as a completion notification that clears EF's isReplicaAdding guard.
func (s *Server) EngineFrontendReplicaAdd(ctx context.Context, req *spdkrpc.EngineFrontendReplicaAddRequest) (ret *emptypb.Empty, err error) {
	// Check for completion callback from Engine's async replica add.
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(replicaAddCompleteMetadata); len(values) > 0 && values[0] == "true" {
			return s.handleReplicaAddComplete(req.EngineFrontendName, md)
		}
	}

	if req.ReplicaName == "" || req.ReplicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and address are required")
	}
	s.RLock()
	var ef *EngineFrontend
	ef, ok := s.engineFrontendMap[req.EngineFrontendName]
	if !ok {
		s.RUnlock()
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for replica %s with address %s add", req.EngineFrontendName, req.ReplicaName, req.ReplicaAddress)
	}
	spdkClient := s.spdkClient
	s.RUnlock()

	log := logrus.WithFields(logrus.Fields{
		"engineFrontend": req.EngineFrontendName,
		"replicaName":    req.ReplicaName,
	})
	log.Info("Starting frontend-aware replica add")

	return &emptypb.Empty{}, ef.ReplicaAdd(spdkClient, req.ReplicaName, req.ReplicaAddress, req.FastSync)
}

// handleReplicaAddComplete processes the completion callback from Engine's
// background replica-add goroutine. It clears the EngineFrontend's
// isReplicaAdding guard and sets error state if the replica add failed.
func (s *Server) handleReplicaAddComplete(efName string, md metadata.MD) (*emptypb.Empty, error) {
	s.RLock()
	ef, ok := s.engineFrontendMap[efName]
	s.RUnlock()
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for replica add completion", efName)
	}

	var replicaAddErr error
	if errValues := md.Get(replicaAddCompleteErrorMetadata); len(errValues) > 0 && errValues[0] != "" {
		replicaAddErr = fmt.Errorf("%s", errValues[0])
	}

	ef.completeReplicaAdd(replicaAddErr)
	return &emptypb.Empty{}, nil
}

// EngineReplicaList returns all replicas for an engine
func (s *Server) EngineReplicaList(ctx context.Context, req *spdkrpc.EngineReplicaListRequest) (ret *spdkrpc.EngineReplicaListResponse, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica list", req.EngineName)
	}

	replicas, err := e.ReplicaList(spdkClient)
	if err != nil {
		return nil, err
	}

	ret = &spdkrpc.EngineReplicaListResponse{
		Replicas: map[string]*spdkrpc.Replica{},
	}

	for _, r := range replicas {
		ret.Replicas[r.Name] = api.ReplicaToProtoReplica(r)
	}

	return ret, nil
}

func (s *Server) EngineReplicaDelete(ctx context.Context, req *spdkrpc.EngineReplicaDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s delete", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}

	if err := e.ReplicaDelete(spdkClient, req.ReplicaName, req.ReplicaAddress); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.SnapshotResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot creation", req.Name)
	}

	snapshotName, err := e.SnapshotCreate(spdkClient, req.SnapshotName)
	return &spdkrpc.SnapshotResponse{SnapshotName: snapshotName}, err
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot deletion", req.Name)
	}

	if err := e.SnapshotDelete(spdkClient, req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot revert", req.Name)
	}

	if err := e.SnapshotRevert(spdkClient, req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot purge", req.Name)
	}

	if err := e.SnapshotPurge(spdkClient); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotHash(ctx context.Context, req *spdkrpc.SnapshotHashRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot hash", req.Name)
	}

	if err := e.SnapshotHash(spdkClient, req.SnapshotName, req.Rehash); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineSnapshotHashStatus(ctx context.Context, req *spdkrpc.SnapshotHashStatusRequest) (ret *spdkrpc.EngineSnapshotHashStatusResponse, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot hash status", req.Name)
	}

	return e.SnapshotHashStatus(req.SnapshotName)
}

func (s *Server) EngineSnapshotClone(ctx context.Context, req *spdkrpc.EngineSnapshotCloneRequest) (ret *emptypb.Empty, err error) {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: req.Name},
		util.Param{Name: "snapshotName", Value: req.SnapshotName},
		util.Param{Name: "srcEngineName", Value: req.SrcEngineName},
		util.Param{Name: "srcEngineAddress", Value: req.SrcEngineAddress},
	); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot clone", req.Name)
	}

	if err := e.SnapshotClone(req.SnapshotName, req.SrcEngineName, req.SrcEngineAddress, req.CloneMode); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) EngineBackupCreate(ctx context.Context, req *spdkrpc.BackupCreateRequest) (ret *spdkrpc.BackupCreateResponse, err error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	recv, err := e.BackupCreate(req.BackupName, req.VolumeName, req.EngineName, req.SnapshotName, req.BackingImageName, req.BackingImageChecksum,
		req.Labels, req.BackupTarget, req.Credential, req.ConcurrentLimit, req.CompressionMethod, req.StorageClassName, e.SpecSize)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.BackupCreateResponse{
		Backup:         recv.BackupName,
		IsIncremental:  recv.IsIncremental,
		ReplicaAddress: recv.ReplicaAddress,
	}, nil
}

func (s *Server) ReplicaBackupCreate(ctx context.Context, req *spdkrpc.BackupCreateRequest) (ret *spdkrpc.BackupCreateResponse, err error) {
	backupName := req.BackupName

	backupType, err := butil.CheckBackupType(req.BackupTarget)
	if err != nil {
		return nil, err
	}

	err = butil.SetupCredential(backupType, req.Credential)
	if err != nil {
		err = errors.Wrapf(err, "failed to setup credential of backup target %v for backup %v", req.BackupTarget, backupName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	var labelMap map[string]string
	if req.Labels != nil {
		labelMap, err = util.ParseLabels(req.Labels)
		if err != nil {
			err = errors.Wrapf(err, "failed to parse backup labels for backup %v", backupName)
			return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
		}
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.backupMap[backupName]; ok {
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backup %v already exists", backupName)
	}

	replica, ok := s.replicaMap[req.ReplicaName]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v for volume %v backup creation", req.ReplicaName, req.VolumeName)
	}

	backup, err := NewBackup(s.spdkClient, backupName, req.VolumeName, req.SnapshotName, replica, s.portAllocator)
	if err != nil {
		err = errors.Wrapf(err, "failed to create backup instance %v for volume %v", backupName, req.VolumeName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	config := &backupstore.DeltaBackupConfig{
		BackupName:      backupName,
		ConcurrentLimit: req.ConcurrentLimit,
		Volume: &backupstore.Volume{
			Name:                 req.VolumeName,
			Size:                 req.Size,
			Labels:               labelMap,
			BackingImageName:     req.BackingImageName,
			BackingImageChecksum: req.BackingImageChecksum,
			CompressionMethod:    req.CompressionMethod,
			StorageClassName:     req.StorageClassName,
			CreatedTime:          util.Now(),
			DataEngine:           string(backupstore.DataEngineV2),
		},
		Snapshot: &backupstore.Snapshot{
			Name:        req.SnapshotName,
			CreatedTime: util.Now(),
		},
		DestURL:  req.BackupTarget,
		DeltaOps: backup,
		Labels:   labelMap,
	}

	s.backupMap[backupName] = backup
	if err := backup.BackupCreate(config); err != nil {
		delete(s.backupMap, backupName)
		err = errors.Wrapf(err, "failed to create backup %v for volume %v", backupName, req.VolumeName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	return &spdkrpc.BackupCreateResponse{
		Backup:        backup.Name,
		IsIncremental: backup.IsIncremental,
	}, nil
}

func (s *Server) EngineBackupStatus(ctx context.Context, req *spdkrpc.BackupStatusRequest) (*spdkrpc.BackupStatusResponse, error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	return e.BackupStatus(req.Backup, req.ReplicaAddress)
}

func (s *Server) ReplicaBackupStatus(ctx context.Context, req *spdkrpc.BackupStatusRequest) (ret *spdkrpc.BackupStatusResponse, err error) {
	s.RLock()
	defer s.RUnlock()

	backup, ok := s.backupMap[req.Backup]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backup %v", req.Backup)
	}

	replicaAddress := ""
	if backup.replica != nil {
		replicaAddress = fmt.Sprintf("tcp://%s", backup.replica.GetAddress())
	}

	return &spdkrpc.BackupStatusResponse{
		Progress:       int32(backup.Progress),
		BackupUrl:      backup.BackupURL,
		Error:          backup.Error,
		SnapshotName:   backup.SnapshotName,
		State:          string(backup.State),
		ReplicaAddress: replicaAddress,
	}, nil
}

func (s *Server) EngineBackupRestore(ctx context.Context, req *spdkrpc.EngineBackupRestoreRequest) (ret *spdkrpc.EngineBackupRestoreResponse, err error) {
	logrus.WithFields(logrus.Fields{
		"backup":       req.BackupUrl,
		"engine":       req.EngineName,
		"snapshotName": req.SnapshotName,
		"concurrent":   req.ConcurrentLimit,
	}).Info("Restoring backup")

	s.RLock()
	e := s.engineMap[req.EngineName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for restoring backup", req.EngineName)
	}

	return e.BackupRestore(spdkClient, req.BackupUrl, req.EngineName, req.SnapshotName, req.Credential, req.ConcurrentLimit)
}

func (s *Server) ReplicaBackupRestore(ctx context.Context, req *spdkrpc.ReplicaBackupRestoreRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	replica := s.replicaMap[req.ReplicaName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if replica == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v for restoring backup %v", req.ReplicaName, req.BackupUrl)
	}

	err = replica.BackupRestore(spdkClient, req.BackupUrl, req.SnapshotName, req.Credential, req.ConcurrentLimit)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) EngineRestoreStatus(ctx context.Context, req *spdkrpc.RestoreStatusRequest) (*spdkrpc.RestoreStatusResponse, error) {
	s.RLock()
	e := s.engineMap[req.EngineName]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for backup creation", req.EngineName)
	}

	resp, err := e.RestoreStatus()
	if err != nil {
		err = errors.Wrapf(err, "failed to get restore status for engine %v", req.EngineName)
		return nil, grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}
	return resp, nil
}

func (s *Server) ReplicaRestoreStatus(ctx context.Context, req *spdkrpc.ReplicaRestoreStatusRequest) (ret *spdkrpc.ReplicaRestoreStatusResponse, err error) {
	s.RLock()
	defer s.RUnlock()

	replica, ok := s.replicaMap[req.ReplicaName]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.ReplicaName)
	}

	if replica.restore == nil {
		return &spdkrpc.ReplicaRestoreStatusResponse{
			ReplicaName: replica.Name,
			IsRestoring: false,
		}, nil
	}

	return &spdkrpc.ReplicaRestoreStatusResponse{
		ReplicaName:            replica.Name,
		ReplicaAddress:         net.JoinHostPort(replica.restore.ip, strconv.Itoa(int(replica.restore.port))),
		IsRestoring:            replica.isRestoring,
		LastRestored:           replica.restore.LastRestored,
		Progress:               int32(replica.restore.Progress),
		Error:                  replica.restore.Error,
		DestFileName:           replica.restore.LvolName,
		State:                  string(replica.restore.State),
		BackupUrl:              replica.restore.BackupURL,
		CurrentRestoringBackup: replica.restore.CurrentRestoringBackup,
	}, nil
}

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (*spdkrpc.Disk, error) {
	s.Lock()
	spdkClient := s.spdkClient

	disk, exists := s.diskMap[req.DiskName]
	if exists {
		if disk.GetState() == DiskStateReady {
			s.Unlock()
			return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
		}
		s.Unlock()

		return &spdkrpc.Disk{
			State: string(disk.GetState()),
		}, nil
	}

	disk = NewDisk(req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize)
	s.diskMap[req.DiskName] = disk
	s.Unlock()

	go func(d *Disk, req *spdkrpc.DiskCreateRequest) {
		// Serialize SPDK disk creation to avoid race conditions in global SPDK subsystems
		// The upper-level DiskCreate() flow remains asynchronous — the gRPC call immediately returns and the creation
		// runs in a background goroutine — but within that goroutine, we serialize the
		// lower-level SPDK operations to ensure controller attach and lvstore operations
		// are performed safely and deterministically without concurrent access issues.

		// It may have issues if multiple disks are being created simultaneously without this lock
		s.diskCreateLock.Lock()
		defer s.diskCreateLock.Unlock()

		// Disabling hotplug is a best-effort guard to improve stability; creation continues even if this call fails.
		// TODO: If virtio-blk requires the same handling, add similar guards to the virtio-blk path.
		if isNvmeDriver(req.DiskDriver, req.DiskPath) {
			_ = setNvmeHotPlug(spdkClient, false)
			defer func() {
				if success := setNvmeHotPlug(spdkClient, true); success {
					s.hotplugActive.Store(true)
				} else {
					s.hotplugActive.Store(false)
				}
			}()
		}

		if err := d.DiskCreate(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize); err != nil {
			logrus.WithError(err).Errorf("Failed to create disk %s(%s) path %s", req.DiskName, req.DiskUuid, req.DiskPath)
			return
		}

		logrus.Infof("Disk %v is created, replicas will be discovered by the next monitoring cycle", req.DiskName)
	}(disk, req)

	return &spdkrpc.Disk{
		State: string(disk.GetState()),
	}, nil
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.diskMap, req.DiskName)
			s.Unlock()
		}
	}()

	if disk == nil {
		// If the specified disk does not exist, tlog a warning for visibility.
		logrus.Warnf("Disk %s not found; skipping deletion", req.DiskName)
		return &emptypb.Empty{}, nil
	}

	return disk.DiskDelete(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver)
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if disk == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find disk %v", req.DiskName)
	}

	return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
}

func (s *Server) DiskHealthGet(ctx context.Context, req *spdkrpc.DiskHealthGetRequest) (ret *spdkrpc.DiskHealthGetResponse, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if disk == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "disk %q not found", req.DiskName)
	}

	diskHealth, err := disk.diskHealthGet(spdkClient, req.DiskName, req.DiskDriver)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.DiskHealthGetResponse{
		ModelNumber:                             diskHealth.ModelNumber,
		SerialNumber:                            diskHealth.SerialNumber,
		FirmwareRevision:                        diskHealth.FirmwareRevision,
		Traddr:                                  diskHealth.Traddr,
		CriticalWarning:                         diskHealth.CriticalWarning,
		TemperatureCelsius:                      diskHealth.TemperatureCelsius,
		AvailableSparePercentage:                diskHealth.AvailableSparePercentage,
		AvailableSpareThresholdPercentage:       diskHealth.AvailableSpareThresholdPercentage,
		PercentageUsed:                          diskHealth.PercentageUsed,
		DataUnitsRead:                           diskHealth.DataUnitsRead,
		DataUnitsWritten:                        diskHealth.DataUnitsWritten,
		HostReadCommands:                        diskHealth.HostReadCommands,
		HostWriteCommands:                       diskHealth.HostWriteCommands,
		ControllerBusyTime:                      diskHealth.ControllerBusyTime,
		PowerCycles:                             diskHealth.PowerCycles,
		PowerOnHours:                            diskHealth.PowerOnHours,
		UnsafeShutdowns:                         diskHealth.UnsafeShutdowns,
		MediaErrors:                             diskHealth.MediaErrors,
		NumErrLogEntries:                        diskHealth.NumErrLogEntries,
		WarningTemperatureTimeMinutes:           diskHealth.WarningTemperatureTimeMinutes,
		CriticalCompositeTemperatureTimeMinutes: diskHealth.CriticalCompositeTemperatureTimeMinutes,
	}, nil
}

func (s *Server) LogSetLevel(ctx context.Context, req *spdkrpc.LogSetLevelRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	err = svcLogSetLevel(spdkClient, req.Level)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogSetFlags(ctx context.Context, req *spdkrpc.LogSetFlagsRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	err = svcLogSetFlags(spdkClient, req.Flags)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogGetLevel(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.LogGetLevelResponse, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	level, err := svcLogGetLevel(spdkClient)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.LogGetLevelResponse{
		Level: level,
	}, nil
}

func (s *Server) LogGetFlags(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.LogGetFlagsResponse, err error) {
	s.RLock()
	spdkClient := s.spdkClient
	s.RUnlock()

	flags, err := svcLogGetFlags(spdkClient)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.LogGetFlagsResponse{
		Flags: flags,
	}, nil
}

func (s *Server) VersionDetailGet(context.Context, *emptypb.Empty) (*spdkrpc.VersionDetailGetReply, error) {
	// TODO: Implement this
	return &spdkrpc.VersionDetailGetReply{
		Version: &spdkrpc.VersionOutput{},
	}, nil
}

func (s *Server) newBackingImage(req *spdkrpc.BackingImageCreateRequest) (*BackingImage, error) {
	s.Lock()
	defer s.Unlock()

	// The backing image key is in this form "bi-%s-disk-%s" to distinguish different disks.
	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)
	if _, ok := s.backingImageMap[backingImageSnapLvolName]; !ok {
		exists, err := s.isLvsExist(req.LvsUuid, "")
		if err != nil || !exists {
			return nil, err
		}
		s.backingImageMap[backingImageSnapLvolName] = NewBackingImage(s.ctx, req.Name, req.BackingImageUuid, req.LvsUuid, req.Size, req.Checksum, s.updateChs[types.InstanceTypeBackingImage])
	}

	return s.backingImageMap[backingImageSnapLvolName], nil
}

func (s *Server) BackingImageCreate(ctx context.Context, req *spdkrpc.BackingImageCreateRequest) (ret *spdkrpc.BackingImage, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.BackingImageUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image UUID is required")
	}
	if req.Size == uint64(0) {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image size is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	if req.Checksum == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "checksum is required")
	}

	// Don't recreate the backing image
	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)

	s.RLock()
	bi := s.backingImageMap[backingImageSnapLvolName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi != nil {
		if bi.BackingImageUUID == req.BackingImageUuid {
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backing image %v already exists", req.Name)
		}

		logrus.Infof("Found backing image exists with different backing image UUID %v, deleting it", bi.BackingImageUUID)

		if err := bi.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete backing image %v in lvs %v with different UUID", req.Name, req.LvsUuid).Error())
		}

		s.Lock()
		delete(s.backingImageMap, backingImageSnapLvolName)
		s.Unlock()
	}

	newBI, err := s.newBackingImage(req)
	if err != nil {
		return nil, err
	}

	s.RLock()
	spdkClient = s.spdkClient
	s.RUnlock()

	return newBI.Create(spdkClient, s.portAllocator, req.FromAddress, req.SrcLvsUuid)
}

func (s *Server) BackingImageDelete(ctx context.Context, req *spdkrpc.BackingImageDeleteRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}

	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.backingImageMap, GetBackingImageSnapLvolName(req.Name, req.LvsUuid))
			s.Unlock()
		}
	}()

	if bi != nil {
		if err := bi.Delete(spdkClient, s.portAllocator); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to delete backing image %v in lvs %v", req.Name, req.LvsUuid).Error())
		}
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) BackingImageGet(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *spdkrpc.BackingImage, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}

	backingImageSnapLvolName := GetBackingImageSnapLvolName(req.Name, req.LvsUuid)

	s.RLock()
	bi := s.backingImageMap[backingImageSnapLvolName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		lvsName, err := GetLvsNameByUUID(spdkClient, req.LvsUuid)
		if err != nil {
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "failed to get the lvs name with lvs uuid %v", req.LvsUuid)
		}

		if lvsName != "" {
			backingImageSnapLvolAlias := spdktypes.GetLvolAlias(lvsName, backingImageSnapLvolName)
			bdevLvolList, err := spdkClient.BdevLvolGet(backingImageSnapLvolAlias, 0)
			if err != nil {
				return nil, grpcstatus.Errorf(grpccodes.NotFound, "got error %v when getting lvol %v in the lvs %v", err, req.Name, req.LvsUuid)
			}
			if len(bdevLvolList) != 1 {
				return nil, grpcstatus.Errorf(grpccodes.NotFound, "zero or multiple lvols with alias %s found when finding backing image %v in lvs %v", backingImageSnapLvolAlias, req.Name, req.LvsUuid)
			}
			// If we can get the lvol, verify() will reconstruct the backing image record in the server, should inform the caller
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "backing image %v lvol found in the lvs %v but failed to find the record in the server", req.Name, req.LvsUuid)
		}
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	return bi.Get(), nil
}

func (s *Server) BackingImageList(ctx context.Context, req *emptypb.Empty) (ret *spdkrpc.BackingImageListResponse, err error) {
	backingImageMap := map[string]*BackingImage{}
	res := map[string]*spdkrpc.BackingImage{}

	s.RLock()
	for k, v := range s.backingImageMap {
		backingImageMap[k] = v
	}
	s.RUnlock()

	// backingImageName is in the form of "bi-%s-disk-%s"
	for backingImageName, bi := range backingImageMap {
		res[backingImageName] = bi.Get()
	}

	return &spdkrpc.BackingImageListResponse{BackingImages: res}, nil
}

func (s *Server) BackingImageWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_BackingImageWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeBackingImage)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service backing image watch errored out")
		} else {
			logrus.Info("SPDK service backing image watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service backing image update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped backing image watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

func (s *Server) BackingImageExpose(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *spdkrpc.BackingImageExposeResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	exposedSnapshotLvolAddress, err := bi.BackingImageExpose(spdkClient, s.portAllocator)
	if err != nil {
		return nil, err
	}
	return &spdkrpc.BackingImageExposeResponse{ExposedSnapshotLvolAddress: exposedSnapshotLvolAddress}, nil

}

func (s *Server) BackingImageUnexpose(ctx context.Context, req *spdkrpc.BackingImageGetRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "backing image name is required")
	}
	if req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs UUID is required")
	}
	s.RLock()
	bi := s.backingImageMap[GetBackingImageSnapLvolName(req.Name, req.LvsUuid)]
	spdkClient := s.spdkClient
	s.RUnlock()

	if bi == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find backing image %v in lvs %v", req.Name, req.LvsUuid)
	}

	err = bi.BackingImageUnexpose(spdkClient, s.portAllocator)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to unexpose backing image %v in lvs %v", req.Name, req.LvsUuid).Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) UpdateEngineMetrics() {
	previousBdevIostat := s.currentBdevIostat
	bdevIostat, err := s.spdkClient.BdevGetIostat("", false)
	if err != nil {
		logrus.WithError(err).Error("failed to get bdev iostat")
		return
	}
	s.currentBdevIostat = bdevIostat
	// If this is the first execution, there is no previous data, so exit
	if previousBdevIostat == nil {
		return
	}
	// Calculate the elapsed time in ticks
	tickElapsed := s.currentBdevIostat.Ticks - previousBdevIostat.Ticks
	if tickElapsed == 0 {
		return
	}
	// Convert ticks to seconds
	elapsedSeconds := float64(tickElapsed) / float64(s.currentBdevIostat.TickRate)

	// Convert previous Bdev data into a map for quick lookup
	prevBdevMap := make(map[string]spdktypes.BdevStats)
	for _, prevBdev := range previousBdevIostat.Bdevs {
		prevBdevMap[prevBdev.Name] = prevBdev
	}

	// Initialize a new BdevMetricMap
	s.bdevMetricMap = make(map[string]*spdkrpc.Metrics)
	for _, bdev := range s.currentBdevIostat.Bdevs {
		prevBdev, exists := prevBdevMap[bdev.Name]
		if !exists {
			continue
		}

		// Calculate differences in operations and data
		readOpsDiff := bdev.NumReadOps - prevBdev.NumReadOps
		writeOpsDiff := bdev.NumWriteOps - prevBdev.NumWriteOps
		bytesReadDiff := bdev.BytesRead - prevBdev.BytesRead
		bytesWrittenDiff := bdev.BytesWritten - prevBdev.BytesWritten
		readLatencyDiff := bdev.ReadLatencyTicks - prevBdev.ReadLatencyTicks
		writeLatencyDiff := bdev.WriteLatencyTicks - prevBdev.WriteLatencyTicks

		// Convert latency from ticks to nanoseconds
		readLatency := calculateLatencyInNs(readLatencyDiff, readOpsDiff, s.currentBdevIostat.TickRate)
		writeLatency := calculateLatencyInNs(writeLatencyDiff, writeOpsDiff, s.currentBdevIostat.TickRate)

		s.bdevMetricMap[bdev.Name] = &spdkrpc.Metrics{
			ReadIOPS:        uint64(float64(readOpsDiff) / elapsedSeconds),
			WriteIOPS:       uint64(float64(writeOpsDiff) / elapsedSeconds),
			ReadThroughput:  uint64(float64(bytesReadDiff) / elapsedSeconds),
			WriteThroughput: uint64(float64(bytesWrittenDiff) / elapsedSeconds),
			ReadLatency:     readLatency,
			WriteLatency:    writeLatency,
		}
	}
}

// Converts latency from ticks to nanoseconds
func calculateLatencyInNs(latencyTicks, opsDiff, tickRate uint64) uint64 {
	if opsDiff == 0 {
		return 0 // Prevent division by zero
	}
	return uint64((float64(latencyTicks) / float64(opsDiff)) * (1e9 / float64(tickRate)))
}

func (s *Server) MetricsGet(ctx context.Context, req *spdkrpc.MetricsRequest) (ret *spdkrpc.Metrics, err error) {
	s.RLock()
	defer s.RUnlock()
	m, ok := s.bdevMetricMap[req.Name]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine metrics: %s", req.Name)
	}
	return m, nil
}

func setNvmeHotPlug(spdkClient *spdkclient.Client, enable bool) (success bool) {
	// default value: 100000 microseconds = 0.1 seconds
	_, hotPlugErr := spdkClient.BdevNvmeSetHotplug(enable, 100000)
	if hotPlugErr != nil {
		logrus.WithError(hotPlugErr).Warnf("Failed to set nvme hotplug to %v", enable)
		return false
	}
	return true
}

// engineFrontendByVolumeName returns the first engine frontend that matches
// the given volume name, or nil if none exists. Caller must hold s.RLock or
// s.Lock.
func (s *Server) engineFrontendByVolumeName(volumeName string) *EngineFrontend {
	for _, ef := range s.engineFrontendMap {
		if ef.VolumeName == volumeName {
			return ef
		}
	}
	return nil
}

// EngineFrontendCreate creates a new engine frontend.
func (s *Server) EngineFrontendCreate(ctx context.Context, req *spdkrpc.EngineFrontendCreateRequest) (ret *spdkrpc.EngineFrontend, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}
	if req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "volume name is required")
	}
	if req.EngineName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name is required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "spec size is required")
	}

	if !types.IsFrontendSupported(req.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "frontend %v is not supported", req.Frontend)
	}

	s.Lock()
	_, ok := s.engineFrontendMap[req.Name]
	if ok {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists", req.Name)
	}
	if existing := s.engineFrontendByVolumeName(req.VolumeName); existing != nil {
		s.Unlock()
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists for volume %v", existing.Name, req.VolumeName)
	}

	ef := NewEngineFrontend(req.Name, req.EngineName, req.VolumeName, req.Frontend, req.SpecSize,
		req.UblkQueueDepth, req.UblkNumberOfQueue, s.updateChs[types.InstanceTypeEngineFrontend])
	ef.metadataDir = s.metadataDir

	spdkClient := s.spdkClient
	s.Unlock()

	ret, createErr := ef.Create(spdkClient, req.TargetAddress)

	// Distinguish hard errors (validation / precondition) from runtime
	// failures (e.g. NVMe initiator can't connect).  Hard errors are
	// returned before Create mutates state, so the frontend must NOT be
	// registered.  Runtime failures leave the frontend in Error state;
	// we register it so callers can inspect and clean it up via Delete.
	if createErr != nil &&
		(errors.Is(createErr, ErrEngineFrontendCreateInvalidArgument) ||
			errors.Is(createErr, ErrEngineFrontendCreatePrecondition)) {
		return nil, toEngineFrontendCreateGRPCError(createErr, "failed to create engine frontend %v", req.Name)
	}

	s.Lock()
	// Re-check after Create() to guard against a concurrent create that
	// raced through the same window.
	duplicateName := false
	duplicateVolume := false
	var winner *EngineFrontend
	if existing, exists := s.engineFrontendMap[req.Name]; exists {
		duplicateName = true
		winner = existing
	} else if existing := s.engineFrontendByVolumeName(req.VolumeName); existing != nil {
		duplicateVolume = true
		winner = existing
	}
	if duplicateName || duplicateVolume {
		s.Unlock()
		// The race loser holds a fully-created frontend with real SPDK
		// resources (bdevs, NVMe controllers, etc.). Clean them up so
		// they don't leak.
		// Only clear metadataDir when the loser shares the same
		// volumeName as the winner — they use the same persistence
		// directory, so the loser's Delete() must not remove it.
		// When volumeNames differ, each has its own directory and the
		// loser should clean up its own record.
		if winner != nil && ef.VolumeName == winner.VolumeName {
			ef.metadataDir = ""
		}
		if deleteErr := ef.Delete(spdkClient); deleteErr != nil {
			logrus.WithError(deleteErr).Warnf("Failed to clean up race-loser engine frontend %v", req.Name)
		}
		// The loser's Create() may have overwritten the winner's
		// persistence record (both share the same volumeName key).
		// Re-persist the winner to restore correct on-disk state.
		if winner != nil && winner.metadataDir != "" && ef.VolumeName == winner.VolumeName {
			if err := saveEngineFrontendRecord(winner.metadataDir, winner); err != nil {
				logrus.WithError(err).Warnf("Failed to re-persist winner engine frontend %v record after race", winner.Name)
			}
		}
		if duplicateVolume {
			return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend already exists for volume %v", req.VolumeName)
		}
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine frontend %v already exists", req.Name)
	}
	s.engineFrontendMap[req.Name] = ef
	s.Unlock()

	// Runtime failure: the frontend is registered in Error state so it
	// can be inspected and cleaned up via Delete.
	if createErr != nil {
		return ef.Get(), nil
	}

	return ret, nil
}

func toEngineFrontendCreateGRPCError(err error, format string, args ...any) error {
	code := grpccodes.Internal

	// Check sentinel errors first — they are the most specific indicators
	// of what went wrong and should take priority over any embedded gRPC
	// status that might exist deeper in the error chain.
	switch {
	case errors.Is(err, ErrEngineFrontendCreateInvalidArgument):
		code = grpccodes.InvalidArgument
	case errors.Is(err, ErrEngineFrontendCreatePrecondition):
		code = grpccodes.FailedPrecondition
	case errors.Is(err, context.DeadlineExceeded):
		code = grpccodes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = grpccodes.Canceled
	default:
		// Fall back to any embedded gRPC status.
		if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
			code = statusErr.Code()
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

func toEngineFrontendLifecycleGRPCError(err error, format string, args ...any) error {
	code := grpccodes.Internal

	switch {
	case errors.Is(err, ErrEngineFrontendLifecyclePrecondition), errors.Is(err, ErrSwitchOverTargetPrecondition):
		code = grpccodes.FailedPrecondition
	case errors.Is(err, ErrEngineFrontendLifecycleUnimplemented):
		code = grpccodes.Unimplemented
	case errors.Is(err, context.DeadlineExceeded):
		code = grpccodes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = grpccodes.Canceled
	default:
		if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
			code = statusErr.Code()
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

// EngineFrontendDelete deletes an engine frontend.
func (s *Server) EngineFrontendDelete(ctx context.Context, req *spdkrpc.EngineFrontendDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err != nil {
			return
		}

		s.Lock()
		delete(s.engineFrontendMap, req.Name)
		s.Unlock()
	}()

	if ef == nil {
		return &emptypb.Empty{}, nil
	}

	if err := ef.Delete(spdkClient); err != nil {
		return nil, toEngineFrontendLifecycleGRPCError(err, "failed to delete engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

// EngineFrontendGet returns a specific engine frontend
func (s *Server) EngineFrontendGet(ctx context.Context, req *spdkrpc.EngineFrontendGetRequest) (ret *spdkrpc.EngineFrontend, err error) {
	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v", req.Name)
	}

	return ef.Get(), nil
}

// EngineFrontendList lists all engine frontends.
func (s *Server) EngineFrontendList(ctx context.Context, req *emptypb.Empty) (*spdkrpc.EngineFrontendListResponse, error) {
	engineFrontendMap := map[string]*EngineFrontend{}
	res := map[string]*spdkrpc.EngineFrontend{}

	s.RLock()
	for k, v := range s.engineFrontendMap {
		engineFrontendMap[k] = v
	}
	s.RUnlock()

	for engineFrontendName, ef := range engineFrontendMap {
		res[engineFrontendName] = ef.Get()
	}

	return &spdkrpc.EngineFrontendListResponse{EngineFrontends: res}, nil
}

// EngineFrontendWatch watches engine frontends.
func (s *Server) EngineFrontendWatch(req *emptypb.Empty, srv spdkrpc.SPDKService_EngineFrontendWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeEngineFrontend)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service engine frontend watch errored out")
		} else {
			logrus.Info("SPDK service engine frontend watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service engine frontend update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("spdk gRPC server: stopped engine target watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

func (s *Server) EngineFrontendExpand(ctx context.Context, req *spdkrpc.EngineFrontendExpandRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	spdkClient := s.spdkClient
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v", req.Name)
	}

	if types.IsUblkFrontend(ef.Frontend) {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "cannot expand ublk frontend engine %v", ef.Name)
	}

	err = ef.Expand(ctx, spdkClient, req.Size)
	if err != nil {
		return nil, toExpansionGRPCError(err, "failed to expand engine frontend %v", req.Name)
	}

	return &emptypb.Empty{}, nil
}

func toExpansionGRPCError(err error, format string, args ...interface{}) error {
	code := grpccodes.Internal

	// Preserve downstream gRPC code when available.
	if statusErr, ok := grpcstatus.FromError(errors.UnwrapAll(err)); ok {
		code = statusErr.Code()
	} else {
		switch {
		case errors.Is(err, ErrExpansionInProgress), errors.Is(err, ErrRestoringInProgress):
			code = grpccodes.FailedPrecondition
		case errors.Is(err, ErrExpansionInvalidSize):
			code = grpccodes.InvalidArgument
		case errors.Is(err, context.DeadlineExceeded):
			code = grpccodes.DeadlineExceeded
		case errors.Is(err, context.Canceled):
			code = grpccodes.Canceled
		}
	}

	return grpcstatus.Error(code, errors.Wrapf(err, format, args...).Error())
}

func (s *Server) EngineFrontendSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.SnapshotResponse, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot creation", req.Name)
	}

	snapshotName, err := ef.SnapshotCreate(req.SnapshotName)
	return &spdkrpc.SnapshotResponse{SnapshotName: snapshotName}, err
}

func (s *Server) EngineFrontendSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name and snapshot name are required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot deletion", req.Name)
	}

	if err := ef.SnapshotDelete(req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineFrontendSnapshotRevert(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name and snapshot name are required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot revert", req.Name)
	}

	if err := ef.SnapshotRevert(req.SnapshotName); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *Server) EngineFrontendSnapshotPurge(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *emptypb.Empty, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend name is required")
	}

	s.RLock()
	ef := s.engineFrontendMap[req.Name]
	s.RUnlock()

	if ef == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine frontend %v for snapshot purge", req.Name)
	}

	if err := ef.SnapshotPurge(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// GetEngineStruct returns the internal Engine struct.
// This is for testing purposes only to allow access to internal fields and methods not exposed via RPC.
func (s *Server) GetEngineStruct(name string) *Engine {
	s.RLock()
	defer s.RUnlock()
	return s.engineMap[name]
}

// GetReplicaStruct returns the internal Replica struct.
// This is for testing purposes only to allow tests to inspect or manipulate internal replica state.
func (s *Server) GetReplicaStruct(name string) *Replica {
	s.RLock()
	defer s.RUnlock()
	return s.replicaMap[name]
}

// recoverEngineFrontends loads persisted engine frontend records from disk
// and attempts to recover them by detecting existing NVMe initiators on the host.
// This is called during server startup to restore state after instance-manager restart.
func (s *Server) recoverEngineFrontends() {
	if s.metadataDir == "" {
		return
	}

	records, err := loadEngineFrontendRecords(s.metadataDir)
	if err != nil {
		logrus.WithError(err).Error("Failed to load engine frontend records for recovery")
		return
	}

	if len(records) == 0 {
		return
	}

	logrus.Infof("Recovering %d engine frontend(s) from persisted records", len(records))

	s.Lock()
	spdkClient := s.spdkClient
	for _, record := range records {
		if _, exists := s.engineFrontendMap[record.Name]; exists {
			logrus.Infof("Engine frontend %s already exists in map, skipping recovery", record.Name)
			continue
		}

		ef := NewEngineFrontend(record.Name, record.EngineName, record.VolumeName,
			record.Frontend, record.SpecSize, 0, 0, s.updateChs[types.InstanceTypeEngineFrontend])
		ef.metadataDir = s.metadataDir
		if ef.NvmeTcpFrontend != nil {
			if record.TargetIP != "" {
				ef.NvmeTcpFrontend.TargetIP = record.TargetIP
				ef.EngineIP = record.TargetIP
			}
			if record.TargetPort != 0 {
				ef.NvmeTcpFrontend.TargetPort = record.TargetPort
			}
		}

		s.engineFrontendMap[record.Name] = ef

		logrus.Infof("Recovered engine frontend %s for volume %s from persisted record", record.Name, record.VolumeName)
	}
	s.Unlock()

	// Attempt to recover each frontend's initiator state from the host.
	// This is done outside the server lock to avoid holding it during potentially
	// slow NVMe device discovery operations.
	for _, record := range records {
		s.RLock()
		ef := s.engineFrontendMap[record.Name]
		s.RUnlock()

		if ef == nil {
			continue
		}

		if err := ef.RecoverFromHost(spdkClient); err != nil {
			if errors.Is(err, ErrRecoverDeviceNotFound) {
				s.Lock()
				delete(s.engineFrontendMap, record.Name)
				s.Unlock()
				logrus.Warnf("Removed engine frontend %s from map: device not found on host", record.Name)
				continue
			}
			logrus.WithError(err).Warnf("Failed to recover engine frontend %s from host, setting error state", record.Name)
		}
	}
}
