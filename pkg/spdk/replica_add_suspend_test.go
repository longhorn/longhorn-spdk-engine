package spdk

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
)

type fakeEngineFrontendSuspendServer struct {
	spdkrpc.UnimplementedSPDKServiceServer

	mu         sync.Mutex
	suspendErr error
	suspends   int
	resumes    int
}

func (f *fakeEngineFrontendSuspendServer) EngineFrontendSuspend(context.Context, *spdkrpc.EngineFrontendSuspendRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspends++
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
	return &emptypb.Empty{}, nil
}

func (f *fakeEngineFrontendSuspendServer) EngineFrontendResume(context.Context, *spdkrpc.EngineFrontendResumeRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumes++
	return &emptypb.Empty{}, nil
}

func startFakeEngineFrontendSuspendServer(t *testing.T, suspendErr error) (*fakeEngineFrontendSuspendServer, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake engine frontend: %v", err)
	}
	fake := &fakeEngineFrontendSuspendServer{suspendErr: suspendErr}
	server := grpc.NewServer()
	spdkrpc.RegisterSPDKServiceServer(server, fake)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve fake engine frontend: %v", err)
		}
	})
	return fake, listener.Addr().String()
}

// The rebuild snapshot must not be taken while the frontend can still issue
// I/O, so the snapshot step requires the suspension, and only the finish step
// proceeds without it.
func TestReplicaAddFrontendSuspendResumeWrapper(t *testing.T) {
	switchingOver := grpcstatus.Error(grpccodes.FailedPrecondition, "engine frontend is switching over target")
	unimplemented := grpcstatus.Error(grpccodes.Unimplemented, "suspend frontend ublk is unimplemented")
	timedOut := grpcstatus.Error(grpccodes.Internal, "failed to suspend linear dm device: timeout")

	for _, tc := range []struct {
		name            string
		suspendErr      error
		suspendRequired bool
		wantWork        bool
		wantErr         bool
		wantResume      bool
	}{
		{name: "snapshot step, suspended", suspendRequired: true, wantWork: true, wantResume: true},
		{name: "snapshot step, switching over", suspendErr: switchingOver, suspendRequired: true, wantErr: true},
		{name: "snapshot step, suspend failed", suspendErr: timedOut, suspendRequired: true, wantErr: true},
		{name: "snapshot step, cannot be suspended", suspendErr: unimplemented, suspendRequired: true, wantWork: true},
		{name: "finish step, suspended", wantWork: true, wantResume: true},
		{name: "finish step, switching over", suspendErr: switchingOver, wantWork: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, address := startFakeEngineFrontendSuspendServer(t, tc.suspendErr)
			wrapper := buildGRPCReplicaAddFrontendSuspendResumeWrapper("ef", address, logrus.NewEntry(logrus.New()), client.NewSPDKClient)

			worked := false
			err := wrapper(func() error {
				worked = true
				return nil
			}, tc.suspendRequired)

			if (err != nil) != tc.wantErr {
				t.Fatalf("wrapper error = %v, want error %v", err, tc.wantErr)
			}
			if worked != tc.wantWork {
				t.Fatalf("work ran = %v, want %v", worked, tc.wantWork)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.suspends != 1 {
				t.Fatalf("suspends = %d, want 1", fake.suspends)
			}
			if wantResumes := map[bool]int{true: 1, false: 0}[tc.wantResume]; fake.resumes != wantResumes {
				t.Fatalf("resumes = %d, want %d", fake.resumes, wantResumes)
			}
		})
	}
}

func TestReplicaAddFrontendSuspendResumeWrapperUnreachable(t *testing.T) {
	unreachable := func(string) (*client.SPDKClient, error) {
		return nil, errors.New("connection refused")
	}
	wrapper := buildGRPCReplicaAddFrontendSuspendResumeWrapper("ef", "127.0.0.1:1", logrus.NewEntry(logrus.New()), unreachable)

	for _, suspendRequired := range []bool{true, false} {
		worked := false
		err := wrapper(func() error {
			worked = true
			return nil
		}, suspendRequired)
		if suspendRequired && (err == nil || worked) {
			t.Fatalf("snapshot step ran without suspension: err %v, work ran %v", err, worked)
		}
		if !suspendRequired && (err != nil || !worked) {
			t.Fatalf("finish step did not run: err %v, work ran %v", err, worked)
		}
	}
}
