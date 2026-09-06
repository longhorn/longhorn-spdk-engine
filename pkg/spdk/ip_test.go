package spdk

import (
	"context"
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func TestServerIPFamilyPropagatesToInstances(t *testing.T) {
	serverFamily := commonnet.IPFamilyIPv6
	updateCh := make(chan interface{}, 1)

	engine := newEngine("engine", "volume", types.FrontendEmpty, 1, updateCh, 0, serverFamily, nil)
	replica := newReplica(context.Background(), "replica", "disk", "disk-uuid", 1, true, serverFamily, updateCh, nil)
	frontend := newEngineFrontend("frontend", "engine", "volume", types.FrontendEmpty, 1, 0, 0, serverFamily, updateCh, nil)
	shard := newShard("volume", 0, "disk", "disk-uuid", 1, serverFamily, updateCh)
	shardGroup := newShardGroup(context.Background(), "shard-group", "volume", 1, 1, 1, 64, nil, false, serverFamily, updateCh)

	if engine.ipFamily != serverFamily {
		t.Fatalf("engine family = %q, want %q", engine.ipFamily, serverFamily)
	}
	if replica.ipFamily != serverFamily {
		t.Fatalf("replica family = %q, want %q", replica.ipFamily, serverFamily)
	}
	if frontend.ipFamily != serverFamily {
		t.Fatalf("frontend family = %q, want %q", frontend.ipFamily, serverFamily)
	}
	if shard.ipFamily != serverFamily {
		t.Fatalf("shard family = %q, want %q", shard.ipFamily, serverFamily)
	}
	if shardGroup.ipFamily != serverFamily {
		t.Fatalf("shard group family = %q, want %q", shardGroup.ipFamily, serverFamily)
	}
}

func TestValidateLiveTargetIPFamily(t *testing.T) {
	tests := []struct {
		name       string
		configured commonnet.IPFamily
		address    string
		wantErr    bool
	}{
		{
			name:       "ipv4 target with port",
			configured: commonnet.IPFamilyIPv4,
			address:    "192.0.2.10:4420",
		},
		{
			name:       "ipv6 target with port",
			configured: commonnet.IPFamilyIPv6,
			address:    "[2001:db8::10]:4420",
		},
		{
			name:       "ipv4 rejects ipv6 target",
			configured: commonnet.IPFamilyIPv4,
			address:    "[2001:db8::10]:4420",
			wantErr:    true,
		},
		{
			name:       "ipv6 rejects ipv4 target",
			configured: commonnet.IPFamilyIPv6,
			address:    "192.0.2.10:4420",
			wantErr:    true,
		},
		{
			name:       "unspecified accepts either target",
			configured: commonnet.IPFamilyUnspecified,
			address:    "[2001:db8::10]:4420",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePersistedAddressFamily(test.address, test.configured)
			if (err != nil) != test.wantErr {
				t.Fatalf("validatePersistedAddressFamily() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}
