package spdk

import (
	"testing"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	commonnet "github.com/longhorn/go-common-libs/net"
)

func testIPFamily() commonnet.IPFamily {
	return commonnet.IPFamilyIPv4
}

func TestParseIPFamily(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  commonnet.IPFamily
		ok    bool
	}{
		{name: "empty", value: "", want: commonnet.IPFamilyUnspecified, ok: true},
		{name: "ipv4", value: "ipv4", want: commonnet.IPFamilyIPv4, ok: true},
		{name: "ipv6", value: "ipv6", want: commonnet.IPFamilyIPv6, ok: true},
		{name: "invalid", value: "ipv3", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseIPFamily(tt.value)
			if tt.ok {
				if err != nil {
					t.Fatalf("parseIPFamily(%q) returned error: %v", tt.value, err)
				}
				if got != tt.want {
					t.Fatalf("parseIPFamily(%q) = %q, want %q", tt.value, got, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("parseIPFamily(%q) accepted invalid value", tt.value)
			}
			if code := grpcstatus.Code(err); code != grpccodes.InvalidArgument {
				t.Fatalf("parseIPFamily(%q) code = %s, want InvalidArgument", tt.value, code)
			}
		})
	}
}

func TestGetNvmfEndpoint(t *testing.T) {
	tests := []struct {
		name string
		nqn  string
		ip   string
		port int32
		want string
	}{
		{
			name: "ipv4",
			nqn:  "nqn.2019-01.io.longhorn:volume-123",
			ip:   "192.0.2.1",
			port: 4420,
			want: "nvmf://192.0.2.1:4420/nqn.2019-01.io.longhorn:volume-123",
		},
		{
			name: "ipv6",
			nqn:  "nqn.2019-01.io.longhorn:volume-123",
			ip:   "2001:db8::2",
			port: 4420,
			want: "nvmf://[2001:db8::2]:4420/nqn.2019-01.io.longhorn:volume-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetNvmfEndpoint(tt.nqn, tt.ip, tt.port); got != tt.want {
				t.Fatalf("GetNvmfEndpoint(%q, %q, %d) = %q, want %q", tt.nqn, tt.ip, tt.port, got, tt.want)
			}
		})
	}
}
