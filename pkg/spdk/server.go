package spdk

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

const (
	MonitorInterval = 3 * time.Second

	defaultClusterSize = 4 * 1024 * 1024 // 4MB
	defaultBlockSize   = 4096            // 4KB

	hostPrefix = "/host"
)

type Server struct {
	sync.RWMutex

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *util.Bitmap

	replicaMap map[string]*Replica
}

func NewServer(ctx context.Context, portStart, portEnd int32) (*Server, error) {
	cli, err := spdkclient.NewClient()
	if err != nil {
		return nil, err
	}

	s := &Server{
		ctx: ctx,

		spdkClient:    cli,
		portAllocator: util.NewBitmap(portStart, portEnd),

		replicaMap: map[string]*Replica{},
	}

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoringReplicas()

	return s, nil
}

func (s *Server) monitoringReplicas() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			bdevLvolList, err := s.spdkClient.BdevLvolGet("", 0)
			if err != nil {
				logrus.Errorf("SPDK Server: failed to get lvol bdevs for replica cache update, will retry later: %v", err)
				continue
			}
			bdevLvolMap := map[string]*spdktypes.BdevInfo{}
			for idx := range bdevLvolList {
				bdevLvol := &bdevLvolList[idx]
				if len(bdevLvol.Aliases) == 0 {
					continue
				}
				bdevLvolMap[spdktypes.GetLvolNameFromAlias(bdevLvol.Aliases[0])] = bdevLvol
			}

			s.Lock()
			for _, r := range s.replicaMap {
				r.ValidateAndUpdate(bdevLvolMap)
			}
			s.Unlock()
		}
		if done {
			break
		}
	}
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	s.Lock()
	if s.replicaMap[req.Name] == nil {
		s.replicaMap[req.Name] = NewReplica(req.Name, req.LvsName, req.LvsUuid, req.SpecSize)
	}
	r := s.replicaMap[req.Name]
	s.Unlock()

	portStart, _, err := s.portAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	return r.Create(s.spdkClient, portStart, req.ExposeRequired)
}

func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *empty.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	delete(s.replicaMap, req.Name)
	s.Unlock()

	if r != nil {
		if err := r.Delete(s.spdkClient, req.CleanupRequired); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, fmt.Errorf("replica %s is not found during get", req.Name)
	}

	return r.Get()
}

func (s *Server) ReplicaList(ctx context.Context, req *empty.Empty) (*spdkrpc.ReplicaListResponse, error) {
	// TODO: Implement this
	return &spdkrpc.ReplicaListResponse{}, nil
}

func (s *Server) ReplicaWatch(req *empty.Empty, srv spdkrpc.SPDKService_ReplicaWatchServer) error {
	// TODO: Implement this
	return nil
}

func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, fmt.Errorf("replica %s is not found during snapshot create", req.Name)
	}

	return r.SnapshotCreate(s.spdkClient, req.Name)
}

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, fmt.Errorf("replica %s is not found during snapshot delete", req.Name)
	}

	_, err = r.SnapshotDelete(s.spdkClient, req.Name)
	return &empty.Empty{}, err
}

func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	portStart, _, err := s.portAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	return SvcEngineCreate(s.spdkClient, req.Name, req.Frontend, req.ReplicaAddressMap, portStart)
}

func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *empty.Empty, err error) {
	if err = SvcEngineDelete(s.spdkClient, req.Name); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	return SvcEngineGet(s.spdkClient, req.Name)
}

func (s *Server) EngineList(ctx context.Context, req *empty.Empty) (*spdkrpc.EngineListResponse, error) {
	// TODO: Implement this
	return &spdkrpc.EngineListResponse{}, nil
}

func (s *Server) EngineWatch(req *empty.Empty, srv spdkrpc.SPDKService_EngineWatchServer) error {
	// TODO: Implement this
	return nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Engine, err error) {
	return SvcEngineSnapshotCreate(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	_, err = SvcEngineSnapshotDelete(s.spdkClient, req.Name, req.SnapshotName)
	return &empty.Empty{}, err
}

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (*spdkrpc.Disk, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":  req.DiskName,
		"diskPath":  req.DiskPath,
		"blockSize": req.BlockSize,
	})

	log.Info("Creating disk")

	if req.DiskName == "" || req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	s.Lock()
	defer s.Unlock()

	if err := s.validateDiskCreateRequest(req); err != nil {
		log.WithError(err).Error("Failed to validate disk create request")
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, errors.Wrap(err, "failed to validate disk create request").Error())
	}

	blockSize := uint64(defaultBlockSize)
	if req.BlockSize > 0 {
		log.Infof("Using custom block size %v", req.BlockSize)
		blockSize = uint64(req.BlockSize)
	}

	uuid, err := s.addBlockDevice(req.DiskName, req.DiskPath, blockSize)
	if err != nil {
		log.WithError(err).Error("Failed to add block device")
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrap(err, "failed to add block device").Error())
	}

	return s.lvstoreToDisk(req.DiskPath, "", uuid)
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName": req.DiskName,
		"diskUUID": req.DiskUuid,
	})

	log.Info("Deleting disk")

	if req.DiskName == "" || req.DiskUuid == "" {
		return &empty.Empty{}, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk UUID are required")
	}

	s.Lock()
	defer s.Unlock()

	lvstores, err := s.spdkClient.BdevLvolGetLvstore("", req.DiskUuid)
	if err != nil {
		resp, parseErr := parseErrorMessage(err.Error())
		if parseErr != nil || !isNoSuchDevice(resp.Message) {
			return nil, errors.Wrapf(err, "failed to get lvstore with UUID %v", req.DiskUuid)
		}
		log.WithError(err).Errorf("Cannot find lvstore with UUID %v", req.DiskUuid)
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find lvstore with UUID %v", req.DiskUuid)
	}

	lvstore := &lvstores[0]

	if lvstore.Name != req.DiskName {
		log.Warnf("Disk name %v does not match lvstore name %v", req.DiskName, lvstore.Name)
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "disk name %v does not match lvstore name %v", req.DiskName, lvstore.Name)
	}

	_, err = s.spdkClient.BdevAioDelete(lvstore.BaseBdev)
	if err != nil {
		log.WithError(err).Errorf("Failed to delete AIO bdev %v", lvstore.BaseBdev)
		return nil, errors.Wrapf(err, "failed to delete AIO bdev %v", lvstore.BaseBdev)
	}
	return &empty.Empty{}, nil
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (*spdkrpc.Disk, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName": req.DiskName,
		"diskPath": req.DiskPath,
	})

	log.Info("Getting disk info")

	if req.DiskName == "" || req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	s.RLock()
	defer s.RUnlock()

	// Check if the disk exists
	bdevs, err := s.spdkClient.BdevAioGet(req.DiskName, 0)
	if err != nil {
		resp, parseErr := parseErrorMessage(err.Error())
		if parseErr != nil || !isNoSuchDevice(resp.Message) {
			log.WithError(err).Errorf("Failed to get AIO bdev with name %v", req.DiskName)
			return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get AIO bdev with name %v", req.DiskName).Error())
		}
	}
	if len(bdevs) == 0 {
		log.WithError(err).Errorf("Cannot find AIO bdev with name %v", req.DiskName)
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find AIO bdev with name %v", req.DiskName)
	}

	diskPath := getDiskPath(req.DiskPath)

	var targetBdev *spdktypes.BdevInfo
	for i, bdev := range bdevs {
		if bdev.DriverSpecific != nil ||
			bdev.DriverSpecific.Aio != nil ||
			bdev.DriverSpecific.Aio.FileName == diskPath {
			targetBdev = &bdevs[i]
			break
		}
	}
	if targetBdev == nil {
		log.WithError(err).Errorf("Failed to get AIO bdev name for disk path %v", diskPath)
		return nil, grpcstatus.Errorf(grpccodes.NotFound, errors.Wrapf(err, "failed to get AIO bdev name for disk path %v", diskPath).Error())
	}

	return s.lvstoreToDisk(req.DiskPath, req.DiskName, "")
}

func (s *Server) VersionDetailGet(context.Context, *empty.Empty) (*spdkrpc.VersionDetailGetReply, error) {
	// TODO: Implement this
	return &spdkrpc.VersionDetailGetReply{
		Version: &spdkrpc.VersionOutput{},
	}, nil
}
