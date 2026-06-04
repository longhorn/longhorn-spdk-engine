package spdk

import (
	"fmt"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSelectUpstreamFactory(c *C) {
	cases := []struct {
		name     string
		layout   spdkrpc.DataLayoutType
		upName   string
		upAddr   string
		wantType string // "replica" | "shardgroup"; "" => expect InvalidArgument
	}{
		{"replicated builds replicaUpstream", spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_REPLICATED, "r1", "10.0.0.1:1234", "replica"},
		{"sharded builds shardGroupUpstream", spdkrpc.DataLayoutType_DATA_LAYOUT_TYPE_SHARDED, "sg1", "10.0.0.2:1234", "shardgroup"},
		{"unknown layout rejected", spdkrpc.DataLayoutType(42), "", "", ""},
	}

	for _, tc := range cases {
		fmt.Println("Testing selectUpstreamFactory:", tc.name)

		factory, err := selectUpstreamFactory(tc.layout, nil)
		if tc.wantType == "" {
			c.Assert(factory, IsNil, Commentf("case=%s", tc.name))
			c.Assert(err, NotNil, Commentf("case=%s", tc.name))
			c.Assert(grpcstatus.Code(err), Equals, grpccodes.InvalidArgument, Commentf("case=%s", tc.name))
			continue
		}

		c.Assert(err, IsNil, Commentf("case=%s", tc.name))
		c.Assert(factory, NotNil, Commentf("case=%s", tc.name))

		u := factory(tc.upName, tc.upAddr)
		switch tc.wantType {
		case "replica":
			_, ok := u.(*replicaUpstream)
			c.Assert(ok, Equals, true, Commentf("case=%s", tc.name))
		case "shardgroup":
			_, ok := u.(*shardGroupUpstream)
			c.Assert(ok, Equals, true, Commentf("case=%s", tc.name))
		}
		c.Assert(u.Name(), Equals, tc.upName, Commentf("case=%s", tc.name))
		c.Assert(u.Address(), Equals, tc.upAddr, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestIsShardedEngine(c *C) {
	testCases := map[string]struct {
		upstreams map[string]Upstream
		expected  bool
	}{
		"true when a shardGroupUpstream is present": {
			upstreams: map[string]Upstream{"sg1": newShardGroupUpstream("sg1", "10.0.0.1:1234", nil)},
			expected:  true,
		},
		"true when a shardGroupUpstream is mixed with replicaUpstreams": {
			upstreams: map[string]Upstream{
				"r1":  newReplicaUpstream("r1", "10.0.0.1:1234", nil),
				"sg1": newShardGroupUpstream("sg1", "10.0.0.2:1234", nil),
			},
			expected: true,
		},
		"false when all upstreams are replicaUpstream": {
			upstreams: map[string]Upstream{
				"r1": newReplicaUpstream("r1", "10.0.0.1:1234", nil),
				"r2": newReplicaUpstream("r2", "10.0.0.2:1234", nil),
			},
			expected: false,
		},
		"false when upstreams is empty": {
			upstreams: map[string]Upstream{},
			expected:  false,
		},
	}

	for name, tc := range testCases {
		fmt.Println("Testing isShardedEngine:", name)
		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
		e.upstreams = tc.upstreams
		c.Assert(isShardedEngine(e), Equals, tc.expected, Commentf("case %q", name))
	}
}
