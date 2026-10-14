package spdk

import (
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestSnapshotCloneRejectsEmptyLinkedCloneReplicaMap(t *testing.T) {
	source := NewEngine("source-engine", "source-volume", types.FrontendSPDKTCPBlockdev, 1<<20, nil, 1, nil)
	t.Cleanup(source.cancelCtx)
	source.State = types.InstanceStateRunning

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for source engine: %v", err)
	}
	server := grpc.NewServer()
	spdkrpc.RegisterSPDKServiceServer(server, &Server{
		engineMap: map[string]*Engine{source.Name: source},
	})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve source engine: %v", err)
		}
	})

	for _, tc := range []struct {
		name       string
		replicaMap map[string]string
	}{
		{name: "nil"},
		{name: "empty", replicaMap: map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			destination := NewEngine("destination-engine", "destination-volume", types.FrontendSPDKTCPBlockdev, 1<<20, nil, 1, client.NewSPDKClient)
			t.Cleanup(destination.cancelCtx)

			err := destination.SnapshotClone("snapshot", source.Name, listener.Addr().String(), spdkrpc.CloneMode_CLONE_MODE_LINKED_CLONE, tc.replicaMap)
			if err == nil {
				t.Fatal("linked clone accepted an empty destination-to-source replica map")
			}
			if !strings.Contains(err.Error(), "linked-clone snapshot clone requires a non-empty dst->src replica name map from the manager") {
				t.Fatalf("unexpected linked-clone validation error: %v", err)
			}
		})
	}
}
