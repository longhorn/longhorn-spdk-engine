package spdk

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

type fakeEngineFrontendSuspendServer struct {
	spdkrpc.UnimplementedSPDKServiceServer

	mu           sync.Mutex
	suspendErr   error
	suspends     int
	resumes      int
	suspendNames []string
	noFlush      bool
	noLockFs     bool
}

func (f *fakeEngineFrontendSuspendServer) EngineFrontendSuspend(_ context.Context, req *spdkrpc.EngineFrontendSuspendRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspends++
	f.suspendNames = append(f.suspendNames, req.Name)
	f.noFlush = req.NoFlush
	f.noLockFs = req.NoLockFs
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
	deadlineExceeded := grpcstatus.Error(grpccodes.DeadlineExceeded, "context deadline exceeded")

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
		// The suspend may still complete on the frontend after the RPC timed out.
		{name: "snapshot step, suspend RPC timed out", suspendErr: deadlineExceeded, suspendRequired: true, wantErr: true, wantResume: true},
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
			// Syncing and freezing the filesystem can exceed the dmsetup timeout under
			// heavy writes, and neither step of the replica add needs it.
			if !fake.noFlush || !fake.noLockFs {
				t.Fatalf("suspend request noFlush = %v, noLockFs = %v, want both true", fake.noFlush, fake.noLockFs)
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

// The default suspension, used by other callers of the RPC, must keep syncing and
// freezing the filesystem.
func TestEngineFrontendSuspendKeepsFlushByDefault(t *testing.T) {
	fake, address := startFakeEngineFrontendSuspendServer(t, nil)
	cli, err := client.NewSPDKClient(address)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer func() {
		if errClose := cli.Close(); errClose != nil {
			t.Errorf("close client: %v", errClose)
		}
	}()

	if err := cli.EngineFrontendSuspend("ef"); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	fake.mu.Lock()
	noFlush, noLockFs := fake.noFlush, fake.noLockFs
	fake.mu.Unlock()
	if noFlush || noLockFs {
		t.Fatalf("default suspend request noFlush = %v, noLockFs = %v, want both false", noFlush, noLockFs)
	}

	if err := cli.EngineFrontendSuspendWithOptions("ef", true, false); err != nil {
		t.Fatalf("suspend with options: %v", err)
	}
	fake.mu.Lock()
	noFlush, noLockFs = fake.noFlush, fake.noLockFs
	names := append([]string(nil), fake.suspendNames...)
	fake.mu.Unlock()
	if !noFlush || noLockFs {
		t.Fatalf("suspend request noFlush = %v, noLockFs = %v, want true and false", noFlush, noLockFs)
	}
	if len(names) != 2 || names[0] != "ef" || names[1] != "ef" {
		t.Fatalf("suspend names = %v, want [ef ef]", names)
	}
}

// An engine frontend snapshot operation holds the frontend lock while it calls
// into the Engine, so the frontend must be suspended outside the Engine lock.
func TestEngineReplicaAddSuspendsFrontendOutsideEngineLock(t *testing.T) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendEmpty, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.State = lhtypes.InstanceStateRunning

	suspends := 0
	wrapper := func(work func() error, suspendRequired bool) error {
		if !suspendRequired {
			return work()
		}
		suspends++
		if !e.TryLock() {
			t.Fatal("frontend is suspended under the Engine lock")
		}
		e.Unlock()
		return work()
	}

	// The engine has no replica to rebuild from, so the replica add fails
	// after the suspension.
	if err := e.ReplicaAdd(nil, "replica-new", "10.0.0.2:20000", false, "", "", "", wrapper); err == nil {
		t.Fatal("replica add without a source replica succeeded")
	}
	if suspends != 1 {
		t.Fatalf("suspends = %d, want 1", suspends)
	}
}

// A snapshot taken while the frontend is suspended by another operation, e.g. a
// replica add, would skip the filesystem sync and freeze, so it is rejected.
func TestEngineFrontendSnapshotCreateRejectedWhileSuspended(t *testing.T) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.Endpoint = "/dev/longhorn/vol-a"
	ef.State = lhtypes.InstanceStateSuspended

	if _, err := ef.snapshotOperation("snap-a", SnapshotOperationCreate, nil); err == nil || !strings.Contains(err.Error(), "is suspended") {
		t.Fatalf("snapshot create error = %v, want the frontend to be rejected as suspended", err)
	}
	if ef.State != lhtypes.InstanceStateSuspended {
		t.Fatalf("engine frontend state = %v, want %v", ef.State, lhtypes.InstanceStateSuspended)
	}
}

// Expanding a suspended frontend would resume it under the suspender.
func TestEngineFrontendExpandRejectedWhileSuspended(t *testing.T) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateSuspended

	if err := ef.Expand(context.Background(), nil, 2048); err == nil || !strings.Contains(err.Error(), "is suspended") {
		t.Fatalf("expansion error = %v, want the frontend to be rejected as suspended", err)
	}
	if ef.State != lhtypes.InstanceStateSuspended || ef.isExpanding {
		t.Fatalf("engine frontend state = %v, expanding = %v, want %v and not expanding", ef.State, ef.isExpanding, lhtypes.InstanceStateSuspended)
	}
}

// A suspend request that timed out while waiting for the volume host lock must
// not suspend the frontend after the caller has resumed it.
func TestEngineFrontendSuspendRejectedAfterRequestExpired(t *testing.T) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1024, 0, 0, make(chan interface{}, 1), nil)
	ef.State = lhtypes.InstanceStateRunning
	s := &Server{engineFrontendMap: map[string]*EngineFrontend{ef.Name: ef}}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := s.EngineFrontendSuspend(ctx, &spdkrpc.EngineFrontendSuspendRequest{Name: ef.Name, NoFlush: true, NoLockFs: true})
	if grpcstatus.Code(err) != grpccodes.DeadlineExceeded {
		t.Fatalf("suspend error = %v, want %v", err, grpccodes.DeadlineExceeded)
	}
	if ef.State != lhtypes.InstanceStateRunning || ef.ErrorMsg != "" {
		t.Fatalf("engine frontend state = %v, error = %q, want %v without error", ef.State, ef.ErrorMsg, lhtypes.InstanceStateRunning)
	}
}
