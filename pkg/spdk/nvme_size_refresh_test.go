package spdk

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	. "gopkg.in/check.v1"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeNvmeSizeRefresh replaces the SPDK calls of nvme_size_refresh.go. The reset results of
// each controller are consumed in order and the last one repeats; a controller without
// results accepts every reset.
type fakeNvmeSizeRefresh struct {
	mu         sync.Mutex
	resets     map[string][]fakeResetResult
	resetCalls map[string]int
	bdevSizes  map[string]uint64
	bdevErr    map[string]error
}

func newFakeNvmeSizeRefresh() *fakeNvmeSizeRefresh {
	return &fakeNvmeSizeRefresh{
		resets:     map[string][]fakeResetResult{},
		resetCalls: map[string]int{},
		bdevSizes:  map[string]uint64{},
		bdevErr:    map[string]error{},
	}
}

func (f *fakeNvmeSizeRefresh) install() (restore func()) {
	origReset, origGetBdevs := sizeRefreshBdevNvmeResetController, sizeRefreshBdevGetBdevs

	sizeRefreshBdevNvmeResetController = func(_ *spdkclient.Client, name string) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		call := f.resetCalls[name]
		f.resetCalls[name]++
		results := f.resets[name]
		if len(results) == 0 {
			return true, nil
		}
		r := results[min(call, len(results)-1)]
		return r.success, r.err
	}
	sizeRefreshBdevGetBdevs = func(_ *spdkclient.Client, name string, _ uint64) ([]spdktypes.BdevInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if err := f.bdevErr[name]; err != nil {
			return nil, err
		}
		size, ok := f.bdevSizes[name]
		if !ok {
			return nil, nil
		}
		return []spdktypes.BdevInfo{{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: name, BlockSize: 512, NumBlocks: size / 512}}}, nil
	}

	return func() {
		sizeRefreshBdevNvmeResetController, sizeRefreshBdevGetBdevs = origReset, origGetBdevs
	}
}

func (s *TestSuite) TestResetNvmeControllerForSizeRefresh(c *C) {
	fmt.Println("Testing resetNvmeControllerForSizeRefresh retries a refused reset and stops on a missing controller")

	f := newFakeNvmeSizeRefresh()
	f.resets["ctrl-a"] = []fakeResetResult{{success: false}, {err: errors.New("controller is reconnecting")}, {success: true}}
	f.resets["ctrl-b"] = []fakeResetResult{{err: noSuchDeviceErr()}}
	defer f.install()()

	e := NewEngine("engine-a", "vol-a", types.FrontendSPDKTCPBlockdev, 100, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)

	c.Assert(resetNvmeControllerForSizeRefresh(nil, "ctrl-a", e.log), IsNil)
	c.Assert(f.resetCalls["ctrl-a"], Equals, 3)

	err := resetNvmeControllerForSizeRefresh(nil, "ctrl-b", e.log)
	c.Assert(err, ErrorMatches, "nvme controller ctrl-b no longer exists.*")
	c.Assert(f.resetCalls["ctrl-b"], Equals, 1)
}

func (s *TestSuite) TestCheckBdevSize(c *C) {
	fmt.Println("Testing checkBdevSize")

	f := newFakeNvmeSizeRefresh()
	f.bdevSizes["big"] = 8 << 20
	f.bdevSizes["small"] = 4 << 20
	f.bdevErr["broken"] = errors.New("rpc failed")
	defer f.install()()

	c.Assert(checkBdevSize(nil, "big", 8<<20), IsNil)
	c.Assert(checkBdevSize(nil, "small", 8<<20), ErrorMatches, "bdev small is at size 4194304, requested 8388608")
	c.Assert(checkBdevSize(nil, "missing", 8<<20), ErrorMatches, "bdev missing not found")
	c.Assert(checkBdevSize(nil, "broken", 8<<20), ErrorMatches, "failed to get bdev broken: rpc failed")
}

// The ShardGroup.Expand tests below stop the expansion in step 1 with a missing shard
// controller, so they never reach the EC bdev resize, which needs a real SPDK client.
