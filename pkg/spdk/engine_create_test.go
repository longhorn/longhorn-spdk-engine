package spdk

import (
	"errors"
	"fmt"
	"strings"

	. "gopkg.in/check.v1"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeFactoryWithViews returns an BackendFactory that hands out fakeBackend
// instances pre-loaded with the canned BackendView for each name. Used by
// validateReplicaSize tests so the engine's per-backend Get() returns the
// size we want to assert against.
func fakeFactoryWithViews(views map[string]*BackendView) BackendFactory {
	return func(name, address string) Backend {
		u := newFakeBackend(name, address)
		if v, ok := views[name]; ok {
			u.View = v
		}
		return u
	}
}

func (s *TestSuite) TestValidateReplicaSize(c *C) {
	cases := []struct {
		name       string
		engineSize uint64
		sizes      map[string]uint64 // replica name -> reported SpecSize; also defines the address map
		viewErr    error             // if set, every backend's Get() returns this
		wantErrSub string            // "" => expect success
	}{
		{"accepts matching sizes", 100, map[string]uint64{"r1": 100, "r2": 100}, nil, ""},
		{"rejects mismatched sizes", 100, map[string]uint64{"r1": 100, "r2": 200}, nil, "different replica sizes"},
		{"rejects engine smaller than replicas", 50, map[string]uint64{"r1": 100}, nil, "smaller than replica size"},
		{"propagates backend Get error", 100, map[string]uint64{"r1": 100}, errors.New("backend unavailable"), "backend unavailable"},
		{"rejects empty map", 100, nil, nil, "no replicas"},
	}

	for _, tc := range cases {
		fmt.Println("Testing validateReplicaSize:", tc.name)

		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, tc.engineSize, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

		addrs := map[string]string{}
		views := map[string]*BackendView{}
		for name, size := range tc.sizes {
			addrs[name] = "10.0.0.1:1234"
			views[name] = &BackendView{SpecSize: size}
		}

		factory := fakeFactoryWithViews(views)
		if tc.viewErr != nil {
			factory = func(name, address string) Backend {
				u := newFakeBackend(name, address)
				u.ViewErr = tc.viewErr
				return u
			}
		}

		err := e.validateReplicaSize(addrs, factory)
		if tc.wantErrSub == "" {
			c.Assert(err, IsNil, Commentf("case=%s", tc.name))
			continue
		}
		c.Assert(err, NotNil, Commentf("case=%s", tc.name))
		c.Assert(strings.Contains(err.Error(), tc.wantErrSub), Equals, true, Commentf("case=%s err=%v", tc.name, err))
	}
}
