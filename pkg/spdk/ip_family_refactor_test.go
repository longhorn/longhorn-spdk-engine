package spdk

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	commonnet "github.com/longhorn/go-common-libs/net"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestParseIPFamilyKeepsCanonicalWireValues(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		want    commonnet.IPFamily
		wantErr bool
	}{
		{name: "unspecified", value: "", want: commonnet.IPFamilyUnspecified},
		{name: "ipv4", value: "ipv4", want: commonnet.IPFamilyIPv4},
		{name: "ipv6", value: "ipv6", want: commonnet.IPFamilyIPv6},
		{name: "uppercase ipv4", value: "IPV4", wantErr: true},
		{name: "mixed case ipv6", value: "IPv6", wantErr: true},
		{name: "unknown", value: "dual", wantErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseIPFamily(testCase.value)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("parseIPFamily(%q) accepted invalid wire value", testCase.value)
				}
				if code := grpcstatus.Code(err); code != grpccodes.InvalidArgument {
					t.Fatalf("parseIPFamily(%q) code = %s, want InvalidArgument", testCase.value, code)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseIPFamily(%q) returned error: %v", testCase.value, err)
			}
			if got != testCase.want {
				t.Fatalf("parseIPFamily(%q) = %q, want %q", testCase.value, got, testCase.want)
			}
		})
	}
}

func TestParseIPFamilyFromAddressDelegatesToCommonNetworkParser(t *testing.T) {
	testCases := []string{
		"192.0.2.10",
		"192.0.2.10:4420",
		"2001:db8::10",
		"[2001:db8::10]:4420",
		"",
		"example.com:4420",
		"not-an-ip",
	}

	for _, address := range testCases {
		t.Run(address, func(t *testing.T) {
			gotFamily, gotErr := parseIPFamilyFromAddress(address)
			wantFamily, wantErr := commonnet.ParseIPFamilyFromAddress(address)
			if gotFamily != wantFamily {
				t.Fatalf("parseIPFamilyFromAddress(%q) = %q, want %q", address, gotFamily, wantFamily)
			}
			if (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("parseIPFamilyFromAddress(%q) error = %v, want error presence %t", address, gotErr, wantErr != nil)
			}
		})
	}
}

func TestRecoverEngineFrontendsRejectsMalformedTargetIP(t *testing.T) {
	metadataDir := t.TempDir()
	record := &EngineFrontendRecord{
		Name:       "frontend-a",
		EngineName: "engine-a",
		VolumeName: "volume-a",
		Frontend:   lhtypes.FrontendSPDKTCPNvmf,
		SpecSize:   1 << 20,
		TargetIP:   "not-an-ip",
		TargetPort: 4420,
	}
	recordPath := engineFrontendRecordPath(metadataDir, record.VolumeName)
	if err := os.MkdirAll(filepath.Dir(recordPath), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	server := &Server{
		metadataDir:       metadataDir,
		engineFrontendMap: map[string]*EngineFrontend{},
	}
	server.recoverEngineFrontends(context.Background())

	server.RLock()
	_, recovered := server.engineFrontendMap[record.Name]
	server.RUnlock()
	if recovered {
		t.Fatal("malformed TargetIP recovered an engine frontend")
	}
	if _, err := os.Stat(filepath.Dir(recordPath)); !os.IsNotExist(err) {
		t.Fatalf("malformed record directory still exists, stat error: %v", err)
	}
}

func TestReplicaAddressAndIPFamilyIsOneSnapshot(t *testing.T) {
	replica := &Replica{
		IP:        "192.0.2.10",
		PortStart: 4420,
		ipFamily:  commonnet.IPFamilyIPv4,
	}

	replica.Lock()
	resultCh := make(chan struct {
		address string
		family  commonnet.IPFamily
	}, 1)
	go func() {
		address, family := replica.GetAddressAndIPFamily()
		resultCh <- struct {
			address string
			family  commonnet.IPFamily
		}{address: address, family: family}
	}()

	select {
	case <-resultCh:
		replica.Unlock()
		t.Fatal("GetAddressAndIPFamily did not wait for the replica read lock")
	case <-time.After(10 * time.Millisecond):
	}

	replica.IP = "2001:db8::10"
	replica.PortStart = 4421
	replica.ipFamily = commonnet.IPFamilyIPv6
	replica.Unlock()

	select {
	case got := <-resultCh:
		if got.address != "[2001:db8::10]:4421" || got.family != commonnet.IPFamilyIPv6 {
			t.Fatalf("GetAddressAndIPFamily returned mixed snapshot: address=%q family=%q", got.address, got.family)
		}
	case <-time.After(time.Second):
		t.Fatal("GetAddressAndIPFamily did not return after the replica lock was released")
	}

	if got := replica.GetAddress(); got != "[2001:db8::10]:4421" {
		t.Fatalf("GetAddress() = %q, want %q", got, "[2001:db8::10]:4421")
	}
}
