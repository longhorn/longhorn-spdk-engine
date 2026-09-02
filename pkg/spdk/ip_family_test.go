package spdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestFamilyIsVisibleInObjectResponses(t *testing.T) {
	const family = commonnet.IPFamilyIPv6

	replica := NewReplica(context.Background(), "replica-a", "disk-a", "uuid-a", 1<<20, true, make(chan interface{}, 1), nil)
	replica.ipFamily = family
	if got := ServiceReplicaToProtoReplica(replica).IpFamily; got != string(family) {
		t.Fatalf("Replica response family = %q, want %q", got, family)
	}

	engine := NewEngine("engine-a", "volume-a", lhtypes.FrontendSPDKTCPBlockdev, 1<<20, make(chan interface{}, 1), defaultTestSnapshotMaxCount, family, nil)
	if got := engine.Get().IpFamily; got != string(family) {
		t.Fatalf("Engine response family = %q, want %q", got, family)
	}

	frontend := NewEngineFrontend("frontend-a", "engine-a", "volume-a", lhtypes.FrontendSPDKTCPBlockdev, 1<<20, 0, 0, family, make(chan interface{}, 1), nil)
	if got := frontend.Get().IpFamily; got != string(family) {
		t.Fatalf("EngineFrontend response family = %q, want %q", got, family)
	}
}

func TestIPFamilySurvivesPublicAPIConverters(t *testing.T) {
	replicaProto := &spdkrpc.Replica{IpFamily: string(commonnet.IPFamilyIPv6)}
	if got := api.ProtoReplicaToReplica(replicaProto).IPFamily; got != string(commonnet.IPFamilyIPv6) {
		t.Fatalf("ProtoReplicaToReplica family = %q, want ipv6", got)
	}

	engineProto := &spdkrpc.Engine{IpFamily: string(commonnet.IPFamilyIPv6)}
	if got := api.ProtoEngineToEngine(engineProto).IPFamily; got != string(commonnet.IPFamilyIPv6) {
		t.Fatalf("ProtoEngineToEngine family = %q, want ipv6", got)
	}

	frontendProto := &spdkrpc.EngineFrontend{IpFamily: string(commonnet.IPFamilyIPv6)}
	if got := api.ProtoEngineFrontendToEngineFrontend(frontendProto).IPFamily; got != string(commonnet.IPFamilyIPv6) {
		t.Fatalf("ProtoEngineFrontendToEngineFrontend family = %q, want ipv6", got)
	}
}

func TestEngineFrontendRecordDoesNotPersistFamily(t *testing.T) {
	frontend := NewEngineFrontend("frontend-a", "engine-a", "volume-a", lhtypes.FrontendSPDKTCPNvmf, 1<<20, 0, 0, commonnet.IPFamilyIPv6, make(chan interface{}, 1), nil)
	frontend.NvmeTcpFrontend.TargetIP = "2001:db8::10"
	frontend.NvmeTcpFrontend.TargetPort = 4420

	record := &EngineFrontendRecord{
		Name:       frontend.Name,
		EngineName: frontend.EngineName,
		VolumeName: frontend.VolumeName,
		Frontend:   frontend.Frontend,
		SpecSize:   frontend.SpecSize,
		TargetIP:   frontend.NvmeTcpFrontend.TargetIP,
		TargetPort: frontend.NvmeTcpFrontend.TargetPort,
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "ipFamily") {
		t.Fatalf("EngineFrontend record persisted family: %s", data)
	}
}

func TestUnspecifiedFamilyResponseIsEmpty(t *testing.T) {
	replica := NewReplica(context.Background(), "replica-a", "disk-a", "uuid-a", 1<<20, true, make(chan interface{}, 1), nil)
	got := ServiceReplicaToProtoReplica(replica)
	if got.IpFamily != string(commonnet.IPFamilyUnspecified) {
		t.Fatalf("unspecified Replica response family = %q, want empty", got.IpFamily)
	}

	if got := (&spdkrpc.Replica{IpFamily: ""}).GetIpFamily(); got != "" {
		t.Fatalf("empty protobuf family getter = %q, want empty", got)
	}
}

func TestParseIPFamilyFromAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    commonnet.IPFamily
		ok      bool
	}{
		{name: "ipv4", address: "192.0.2.10", want: commonnet.IPFamilyIPv4, ok: true},
		{name: "ipv4-port", address: "192.0.2.10:8500", want: commonnet.IPFamilyIPv4, ok: true},
		{name: "ipv6-port", address: "[2001:db8::10]:8500", want: commonnet.IPFamilyIPv6, ok: true},
		{name: "invalid", address: "not-an-ip:8500", ok: false},
		{name: "empty", address: "", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseIPFamilyFromAddress(tt.address)
			if tt.ok {
				if err != nil {
					t.Fatalf("parseIPFamilyFromAddress(%q) returned error: %v", tt.address, err)
				}
				if got != tt.want {
					t.Fatalf("parseIPFamilyFromAddress(%q) = %q, want %q", tt.address, got, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("parseIPFamilyFromAddress(%q) accepted invalid input", tt.address)
			}
		})
	}
}

func TestFrontendRecoveryDerivesOppositeFamilyFromPersistedTarget(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   commonnet.IPFamily
	}{
		{name: "first-restart-ipv6", target: "2001:db8::20", want: commonnet.IPFamilyIPv6},
		{name: "second-restart-ipv4", target: "192.0.2.20", want: commonnet.IPFamilyIPv4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			family, err := parseIPFamilyFromAddress(tt.target)
			if err != nil {
				t.Fatal(err)
			}
			frontend := NewEngineFrontend("frontend-a", "engine-a", "volume-a", lhtypes.FrontendSPDKTCPNvmf, 1<<20, 0, 0, family, make(chan interface{}, 1), nil)
			frontend.NvmeTcpFrontend.TargetIP = tt.target
			if got := frontend.Get().IpFamily; got != string(tt.want) {
				t.Fatalf("recovered frontend family = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReplicaCreateRejectsActiveFamilyMismatchBeforeMutation(t *testing.T) {
	replica := NewReplica(context.Background(), "replica-a", "disk-a", "uuid-a", 1<<20, true, make(chan interface{}, 1), nil)
	replica.State = lhtypes.InstanceStateRunning
	replica.ipFamily = commonnet.IPFamilyIPv4

	_, err := replica.Create(nil, commonnet.IPFamilyIPv6, 0, nil, nil)
	if err == nil {
		t.Fatal("Replica.Create accepted an active family mismatch")
	}
	if got := grpcstatus.Code(err); got != grpccodes.FailedPrecondition {
		t.Fatalf("Replica.Create mismatch code = %q, want FailedPrecondition", got)
	}
	if replica.ipFamily != commonnet.IPFamilyIPv4 {
		t.Fatalf("Replica family changed after rejected create: %q", replica.ipFamily)
	}
	if replica.SpecSize != 1<<20 || replica.LvsName != "disk-a" || replica.LvsUUID != "uuid-a" {
		t.Fatalf("Replica metadata changed after rejected create: size=%d lvs=%q uuid=%q", replica.SpecSize, replica.LvsName, replica.LvsUUID)
	}
}

func TestBackupUsesOwningReplicaFamily(t *testing.T) {
	replica := &Replica{ipFamily: commonnet.IPFamily("invalid")}
	_, err := NewBackup(nil, "backup-a", "volume-a", "snapshot-a", replica, nil, nil)
	if err == nil {
		t.Fatal("NewBackup did not use the owning Replica family")
	}
}

func TestReplicaRecoveryVerifiesRetainedNVMfSubsystem(t *testing.T) {
	originalGetSubsystems := replicaGetNvmfSubsystemMap
	originalStopExpose := replicaStopExposeBdev
	defer func() {
		replicaGetNvmfSubsystemMap = originalGetSubsystems
		replicaStopExposeBdev = originalStopExpose
	}()

	retained := func(replica *Replica) map[string]*spdktypes.NvmfSubsystem {
		return map[string]*spdktypes.NvmfSubsystem{replica.Nqn: &spdktypes.NvmfSubsystem{}}
	}
	newReplica := func() *Replica {
		r := NewReplica(context.Background(), "replica-a", "disk-a", "", 1<<20, true, make(chan interface{}, 1), nil)
		r.ipFamily = commonnet.IPFamilyIPv6
		return r
	}
	rebuildMap := func() map[string]*spdktypes.BdevInfo {
		return map[string]*spdktypes.BdevInfo{
			"replica-a": makeBdevLvol("disk-a", "replica-a", "", nil),
		}
	}

	tests := []struct {
		name            string
		initialErr      error
		stopErr         error
		retainAfterStop bool
		wantError       bool
	}{
		{name: "initial-query-error", initialErr: errors.New("query failed"), wantError: true},
		{name: "stop-error", stopErr: errors.New("stop failed"), wantError: true},
		{name: "verification-still-retained", retainAfterStop: true, wantError: true},
		{name: "stopped-and-verified", wantError: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReplica()
			queryCalls := 0
			replicaGetNvmfSubsystemMap = func(*spdkclient.Client) (map[string]*spdktypes.NvmfSubsystem, error) {
				queryCalls++
				if queryCalls == 1 && tt.initialErr != nil {
					return nil, tt.initialErr
				}
				if queryCalls == 1 || tt.retainAfterStop {
					return retained(r), nil
				}
				return map[string]*spdktypes.NvmfSubsystem{}, nil
			}
			stopCalls := 0
			replicaStopExposeBdev = func(*spdkclient.Client, string) error {
				stopCalls++
				return tt.stopErr
			}

			err := r.syncWithBdevLvolMap(&spdkclient.Client{}, rebuildMap())
			if tt.wantError {
				if err == nil {
					t.Fatal("recovery unexpectedly succeeded")
				}
				if r.State != lhtypes.InstanceStateError {
					t.Fatalf("failed recovery state = %q, want error", r.State)
				}
				if r.IsExposed {
					t.Fatal("failed recovery published exposed state")
				}
				if r.ipFamily != commonnet.IPFamilyIPv6 {
					t.Fatalf("failed recovery cleared family: %q", r.ipFamily)
				}
				if tt.initialErr != nil && stopCalls != 0 {
					t.Fatalf("stop called after initial query failure: %d", stopCalls)
				}
				return
			}

			if err != nil {
				t.Fatalf("recovery returned error: %v", err)
			}
			if r.State != lhtypes.InstanceStateStopped || r.IsExposed {
				t.Fatalf("successful recovery state/exposure = %q/%v, want stopped/false", r.State, r.IsExposed)
			}
			if r.ipFamily != commonnet.IPFamilyUnspecified {
				t.Fatalf("successful recovery family = %q, want empty", r.ipFamily)
			}
			if stopCalls != 1 || queryCalls != 2 {
				t.Fatalf("successful recovery calls stop=%d query=%d, want 1/2", stopCalls, queryCalls)
			}
		})
	}
}
