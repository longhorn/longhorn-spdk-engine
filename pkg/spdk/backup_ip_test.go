package spdk

import (
	"testing"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
)

func TestNewBackupUsesConfiguredIPFamilyForPodAddress(t *testing.T) {
	tests := []struct {
		name    string
		podIP   string
		replica *Replica
		wantIP  string
		wantErr bool
	}{
		{
			name:   "without replica",
			podIP:  "2001:db8::1",
			wantIP: "2001:db8::1",
		},
		{
			name:  "with replica from different family",
			podIP: "2001:db8::1",
			replica: &Replica{
				ipFamily:  commonnet.IPFamilyIPv4,
				IP:        "192.0.2.1",
				PortStart: 2000,
			},
			wantIP: "2001:db8::1",
		},
		{
			name:    "without IPv6 address does not fall back to IPv4",
			podIP:   "198.51.100.1",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(commonnet.EnvPodIP, test.podIP)

			portAllocator, err := commonbitmap.NewBitmap(1000, 1000)
			if err != nil {
				t.Fatal(err)
			}

			backup, err := NewBackup(nil, "backup", "volume", "snapshot", commonnet.IPFamilyIPv6,
				test.replica, portAllocator, nil)
			if test.wantErr {
				if err == nil {
					t.Fatal("NewBackup() succeeded, want an error when IPv6 is unavailable")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := portAllocator.ReleaseRange(backup.Port, backup.Port); err != nil {
					t.Errorf("failed to release backup port: %v", err)
				}
			})

			if backup.IP != test.wantIP {
				t.Fatalf("backup IP = %q, want %q", backup.IP, test.wantIP)
			}
		})
	}
}
