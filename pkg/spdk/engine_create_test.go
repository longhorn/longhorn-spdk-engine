package spdk

import (
	"errors"
	"fmt"
	"strings"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeFactoryWithViews returns an BackendFactory that hands out fakeBackend
// instances pre-loaded with the canned BackendView for each name. Used by
// validateReplicas tests so the engine's per-backend Get() returns the
// size and transport we want to assert against.
func fakeFactoryWithViews(views map[string]*BackendView) BackendFactory {
	return func(name, address string) Backend {
		u := newFakeBackend(name, address)
		if v, ok := views[name]; ok {
			u.View = v
		}
		return u
	}
}

func (s *TestSuite) TestValidateReplicas(c *C) {
	tcp := spdktypes.NvmeTransportTypeTCP
	rdma := spdktypes.NvmeTransportTypeRDMA
	cases := []struct {
		name            string
		engineSize      uint64
		engineTransport spdktypes.NvmeTransportType
		sizes           map[string]uint64                      // replica name -> reported SpecSize; also defines the address map
		transports      map[string]spdktypes.NvmeTransportType // replica name -> reported TransportType; unset => ""
		viewErr         error                                  // if set, every backend's Get() returns this
		wantErrSub      string                                 // "" => expect success
	}{
		{"accepts matching sizes", 100, tcp, map[string]uint64{"r1": 100, "r2": 100}, nil, nil, ""},
		{"rejects mismatched sizes", 100, tcp, map[string]uint64{"r1": 100, "r2": 200}, nil, nil, "different replica sizes"},
		{"rejects engine smaller than replicas", 50, tcp, map[string]uint64{"r1": 100}, nil, nil, "smaller than replica size"},
		{"propagates backend Get error", 100, tcp, map[string]uint64{"r1": 100}, nil, errors.New("backend unavailable"), "backend unavailable"},
		{"rejects empty map", 100, tcp, nil, nil, nil, "no replicas"},
		{"accepts matching RDMA transport", 100, rdma, map[string]uint64{"r1": 100, "r2": 100}, map[string]spdktypes.NvmeTransportType{"r1": rdma, "r2": rdma}, nil, ""},
		{"accepts unset replica transport as TCP", 100, tcp, map[string]uint64{"r1": 100}, map[string]spdktypes.NvmeTransportType{"r1": ""}, nil, ""},
		{"rejects TCP replica for RDMA engine", 100, rdma, map[string]uint64{"r1": 100, "r2": 100}, map[string]spdktypes.NvmeTransportType{"r1": rdma, "r2": tcp}, nil, "does not match engine"},
		{"rejects RDMA replica for TCP engine", 100, tcp, map[string]uint64{"r1": 100}, map[string]spdktypes.NvmeTransportType{"r1": rdma}, nil, "does not match engine"},
	}

	for _, tc := range cases {
		fmt.Println("Testing validateReplicas:", tc.name)

		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, tc.engineTransport, tc.engineSize, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

		addrs := map[string]string{}
		views := map[string]*BackendView{}
		for name, size := range tc.sizes {
			addrs[name] = "10.0.0.1:1234"
			views[name] = &BackendView{SpecSize: size, TransportType: tc.transports[name]}
		}

		factory := fakeFactoryWithViews(views)
		if tc.viewErr != nil {
			factory = func(name, address string) Backend {
				u := newFakeBackend(name, address)
				u.ViewErr = tc.viewErr
				return u
			}
		}

		err := e.validateReplicas(addrs, factory)
		if tc.wantErrSub == "" {
			c.Assert(err, IsNil, Commentf("case=%s", tc.name))
			continue
		}
		c.Assert(err, NotNil, Commentf("case=%s", tc.name))
		c.Assert(strings.Contains(err.Error(), tc.wantErrSub), Equals, true, Commentf("case=%s err=%v", tc.name, err))
	}
}
