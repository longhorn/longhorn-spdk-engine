package spdk

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

type fakeEngineExpandServer struct {
	spdkrpc.UnimplementedSPDKServiceServer

	mu          sync.Mutex
	specSize    uint64
	state       string
	isExpanding bool
	lastErr     string
	gets        int
	// notFound makes EngineGet report that the engine does not exist.
	notFound bool
	// async makes EngineExpand return once the expansion has started, like a RAID1
	// engine. Otherwise it returns once the expansion is done, like an EC engine.
	async bool
	// expandDelay is how long the engine takes to expand.
	expandDelay time.Duration
	// expandErr makes the engine fail to expand, with this error.
	expandErr string
	// rejectErr makes the engine reject the expansion request.
	rejectErr string
}

func (f *fakeEngineExpandServer) EngineGet(context.Context, *spdkrpc.EngineGetRequest) (*spdkrpc.Engine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.notFound {
		return nil, grpcstatus.Error(grpccodes.NotFound, "cannot find engine")
	}
	return &spdkrpc.Engine{
		Name:               "engine",
		SpecSize:           f.specSize,
		ActualSize:         f.specSize,
		State:              f.state,
		IsExpanding:        f.isExpanding,
		LastExpansionError: f.lastErr,
	}, nil
}

func (f *fakeEngineExpandServer) EngineExpandPrecheck(context.Context, *spdkrpc.EngineExpandPrecheckRequest) (*spdkrpc.EngineExpandPrecheckResponse, error) {
	return &spdkrpc.EngineExpandPrecheckResponse{}, nil
}

func (f *fakeEngineExpandServer) EngineExpand(_ context.Context, req *spdkrpc.EngineExpandRequest) (*emptypb.Empty, error) {
	if f.rejectErr != "" {
		return nil, grpcstatus.Error(grpccodes.FailedPrecondition, f.rejectErr)
	}

	f.mu.Lock()
	f.isExpanding = true
	f.mu.Unlock()

	expand := func() error {
		// Like the engine, keep expanding although the request times out.
		time.Sleep(f.expandDelay)

		f.mu.Lock()
		defer f.mu.Unlock()
		f.isExpanding = false
		if f.expandErr != "" {
			// Like the engine, keep the original size and move to the error state.
			f.state = string(types.InstanceStateError)
			f.lastErr = f.expandErr
			return grpcstatus.Error(grpccodes.Internal, f.expandErr)
		}
		f.specSize = req.Size
		return nil
	}
	if f.async {
		go func() { _ = expand() }()
		return &emptypb.Empty{}, nil
	}
	if err := expand(); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func startFakeEngineExpandServer(t *testing.T, fake *fakeEngineExpandServer) string {
	t.Helper()
	address, _ := startStoppableFakeEngineExpandServer(t, fake)
	return address
}

func startStoppableFakeEngineExpandServer(t *testing.T, fake *fakeEngineExpandServer) (string, *grpc.Server) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake engine: %v", err)
	}
	server := grpc.NewServer()
	spdkrpc.RegisterSPDKServiceServer(server, fake)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve fake engine: %v", err)
		}
	})
	return listener.Addr().String(), server
}

// Like the V1 engine controller, a RAID1 engine expands in the background, since it
// expands the replicas one by one, which can take longer than a request. The engine
// frontend must wait for the expansion, and take its result, instead of failing while the
// engine still expands.
func TestEngineFrontendExpandWaitsForEngineExpansion(t *testing.T) {
	const (
		oldSize = uint64(10 << 20)
		newSize = uint64(20 << 20)
	)

	interval := engineExpansionWaitInterval
	engineExpansionWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() { engineExpansionWaitInterval = interval })

	const slow = 500 * time.Millisecond
	for _, tc := range []struct {
		name     string
		engine   *fakeEngineExpandServer
		wantErr  string
		wantSize uint64
	}{
		{name: "engine expands in the background", engine: &fakeEngineExpandServer{async: true, expandDelay: slow}, wantSize: newSize},
		{name: "engine fails in the background", engine: &fakeEngineExpandServer{async: true, expandDelay: slow, expandErr: "replica failed"}, wantErr: "replica failed", wantSize: oldSize},
		// An engine created for an offline expansion can be at the requested size before
		// its RAID bdev is.
		{name: "engine at the requested size fails in the background", engine: &fakeEngineExpandServer{specSize: newSize, async: true, expandDelay: slow, expandErr: "replica failed"}, wantErr: "replica failed", wantSize: oldSize},
		{name: "engine expands before the request returns", engine: &fakeEngineExpandServer{}, wantSize: newSize},
		{name: "engine fails before the request returns", engine: &fakeEngineExpandServer{expandErr: "replica failed"}, wantErr: "replica failed", wantSize: oldSize},
		{name: "engine rejects the expansion", engine: &fakeEngineExpandServer{rejectErr: "restoring"}, wantErr: "restoring", wantSize: oldSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := tc.engine
			if fake.specSize == 0 {
				fake.specSize = oldSize
			}
			fake.state = string(types.InstanceStateRunning)
			address := startFakeEngineExpandServer(t, fake)
			ef := NewEngineFrontend("ef", "engine", "vol", types.FrontendEmpty, oldSize, 0, 0, make(chan interface{}, 1),
				func(string) (*client.SPDKClient, error) { return client.NewSPDKClient(address) }, false)

			err := ef.Expand(context.Background(), nil, newSize)

			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expand error = %v, want nil", err)
				}
				if ef.State == types.InstanceStateError {
					t.Fatalf("engine frontend state = %v, error %q", ef.State, ef.ErrorMsg)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expand error = %v, want %q", err, tc.wantErr)
				}
				if ef.State != types.InstanceStateError {
					t.Fatalf("engine frontend state = %v, want %v", ef.State, types.InstanceStateError)
				}
			}
			if ef.SpecSize != tc.wantSize {
				t.Fatalf("engine frontend size = %v, want %v", ef.SpecSize, tc.wantSize)
			}
			if ef.isExpanding {
				t.Fatal("engine frontend is still expanding")
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.isExpanding {
				t.Fatal("engine frontend returned while the engine still expands")
			}
		})
	}
}

// The wait for the engine expansion holds the volume host lock, which blocks the other
// frontend operations of the volume, such as Delete. It must stop once the engine is gone
// instead of waiting for the whole expansion budget.
func TestWaitForEngineExpansionStops(t *testing.T) {
	waitTimeout, unavailableTimeout, interval := engineExpansionWaitTimeout, engineExpansionUnavailableTimeout, engineExpansionWaitInterval
	engineExpansionWaitInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		engineExpansionWaitTimeout, engineExpansionUnavailableTimeout, engineExpansionWaitInterval = waitTimeout, unavailableTimeout, interval
	})

	for _, tc := range []struct {
		name               string
		engine             *fakeEngineExpandServer
		stopEngine         bool
		waitTimeout        time.Duration
		unavailableTimeout time.Duration
		wantCode           grpccodes.Code
		wantErr            string
	}{
		{
			name:               "engine becomes unavailable",
			engine:             &fakeEngineExpandServer{isExpanding: true},
			stopEngine:         true,
			waitTimeout:        time.Minute,
			unavailableTimeout: 200 * time.Millisecond,
			wantCode:           grpccodes.Unavailable,
			wantErr:            "has been unavailable",
		},
		{
			name:               "engine is not found",
			engine:             &fakeEngineExpandServer{notFound: true},
			waitTimeout:        time.Minute,
			unavailableTimeout: time.Minute,
			wantCode:           grpccodes.NotFound,
			wantErr:            "cannot find engine",
		},
		{
			name:               "engine keeps expanding",
			engine:             &fakeEngineExpandServer{isExpanding: true},
			waitTimeout:        200 * time.Millisecond,
			unavailableTimeout: time.Minute,
			wantErr:            "still expanding",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engineExpansionWaitTimeout = tc.waitTimeout
			engineExpansionUnavailableTimeout = tc.unavailableTimeout

			address, server := startStoppableFakeEngineExpandServer(t, tc.engine)
			cli, err := client.NewSPDKClient(address)
			if err != nil {
				t.Fatalf("create client: %v", err)
			}
			t.Cleanup(func() { _ = cli.Close() })
			if tc.stopEngine {
				server.Stop()
			}

			start := time.Now()
			engine, err := waitForEngineExpansion(cli, "engine")
			elapsed := time.Since(start)

			if err == nil {
				t.Fatalf("wait returned engine %+v, want error", engine)
			}
			if !strings.Contains(err.Error(), tc.wantErr) || strings.Contains(err.Error(), "\n") {
				t.Fatalf("wait error = %q, want a single line with %q", err, tc.wantErr)
			}
			if tc.wantCode != grpccodes.OK && grpcstatus.Code(err) != tc.wantCode {
				t.Fatalf("wait error code = %v, want %v: %v", grpcstatus.Code(err), tc.wantCode, err)
			}
			if limit := min(tc.waitTimeout, tc.unavailableTimeout) + 5*time.Second; elapsed > limit {
				t.Fatalf("wait took %v, want less than %v", elapsed, limit)
			}
		})
	}
}
