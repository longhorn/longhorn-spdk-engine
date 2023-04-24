package spdk

import (
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/ptypes"
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

	return s, nil
}

func (s *Server) ReplicaCreate(ctx context.Context, req *ptypes.ReplicaCreateRequest) (ret *ptypes.Replica, err error) {
	return nil, nil
}

func (s *Server) ReplicaDelete(ctx context.Context, req *ptypes.ReplicaDeleteRequest) (ret *empty.Empty, err error) {
	return nil, nil
}

func (s *Server) ReplicaGet(ctx context.Context, req *ptypes.ReplicaGetRequest) (ret *ptypes.Replica, err error) {
	return nil, nil
}

func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *ptypes.SnapshotRequest) (ret *ptypes.Replica, err error) {
	return nil, nil
}

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *ptypes.SnapshotRequest) (ret *ptypes.Replica, err error) {
	return nil, nil
}

func (s *Server) EngineCreate(ctx context.Context, req *ptypes.EngineCreateRequest) (ret *ptypes.Engine, err error) {
	return nil, nil
}

func (s *Server) EngineDelete(ctx context.Context, req *ptypes.EngineCreateRequest) (ret *empty.Empty, err error) {
	return nil, nil
}

func (s *Server) EngineGet(ctx context.Context, req *ptypes.EngineCreateRequest) (ret *ptypes.Engine, err error) {
	return nil, nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *ptypes.SnapshotRequest) (ret *ptypes.Engine, err error) {
	return nil, nil
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *ptypes.SnapshotRequest) (ret *ptypes.Engine, err error) {
	return nil, nil
}
