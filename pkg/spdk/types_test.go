package spdk

import (
	"context"
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"
)

func testIPFamily() commonnet.IPFamily {
	return commonnet.IPFamilyIPv4
}
func TestNewServerRejectsInvalidIPFamily(t *testing.T) {
	_, err := NewServer(context.Background(), 10000, 10001, commonnet.IPFamily("invalid"), nil)
	if err == nil {
		t.Fatal("NewServer accepted an invalid IP family")
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
