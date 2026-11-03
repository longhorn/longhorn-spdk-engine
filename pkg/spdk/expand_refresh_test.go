package spdk

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	. "gopkg.in/check.v1"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// A block size of 1 keeps the test sizes in bytes.
const fakeExpansionBlockSize = 1

type fakeResetResult struct {
	success bool
	err     error
}

// fakeExpansionSPDK replaces the SPDK calls made by RAID1 expansion. The RAID bdev
// keeps raidSize until bdev_raid_resize succeeds, then reports grownSize. Sequences
// of reset results are consumed in order; the last entry repeats. Resize errors
// are returned in order, then the resize succeeds.
type fakeExpansionSPDK struct {
	mu sync.Mutex

	raidSize    uint64
	grownSize   uint64
	raidErr     error
	raidEmpty   bool
	raidNoInfo  bool
	raidCalls   int
	raidMembers map[string]bool

	resizeErrs        []error
	resizeCalls       int
	resizeExpectedMib []uint64

	// Backend bdevs missing from bdevSizes report defaultBdevSize, or are not
	// found when it is 0.
	bdevSizes       map[string]uint64
	defaultBdevSize uint64
	bdevErr         map[string]error

	resets     map[string][]fakeResetResult
	resetCalls map[string]int
	resetDelay time.Duration

	removeErr map[string]error
	removed   []string
}

// newFakeExpansionSPDK returns a fake whose RAID bdev is at raidSize and grows to
// grownSize on bdev_raid_resize. The backend bdevs report grownSize.
func newFakeExpansionSPDK(raidSize, grownSize uint64) *fakeExpansionSPDK {
	return &fakeExpansionSPDK{
		raidSize:        raidSize,
		grownSize:       grownSize,
		resets:          map[string][]fakeResetResult{},
		resetCalls:      map[string]int{},
		removeErr:       map[string]error{},
		raidMembers:     map[string]bool{},
		bdevSizes:       map[string]uint64{},
		defaultBdevSize: grownSize,
		bdevErr:         map[string]error{},
	}
}

func busyErr() error {
	return jsonrpc.JSONClientError{ErrorDetail: &jsonrpc.ResponseError{Code: jsonrpc.RespErrorCodeDeviceOrResourceBusy, Message: "Device or resource busy"}}
}

// setMembers makes the backend bdevs of e the configured members of the RAID bdev.
func (f *fakeExpansionSPDK) setMembers(e *Engine) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range e.backends {
		f.raidMembers[b.BdevName()] = true
	}
}

// evict drops a base bdev from the RAID bdev, as raid1 does when I/O to it fails.
func (f *fakeExpansionSPDK) evict(bdevName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.raidMembers, bdevName)
}

func (f *fakeExpansionSPDK) install() (restore func()) {
	origGet, origRemove, origReset, origGetBdevs, origResize := expansionBdevRaidGet, expansionBdevRaidRemoveBaseBdev, sizeRefreshBdevNvmeResetController, sizeRefreshBdevGetBdevs, expansionBdevRaidResize
	origRetryInterval, origTimeout := raidResizeRetryInterval, raidResizeTimeout
	raidResizeRetryInterval = time.Millisecond
	raidResizeTimeout = 50 * time.Millisecond

	expansionBdevRaidGet = func(_ *spdkclient.Client, name string, _ uint64) ([]spdktypes.BdevInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.raidCalls++
		if f.raidErr != nil {
			return nil, f.raidErr
		}
		if f.raidEmpty {
			return nil, nil
		}
		info := spdktypes.BdevInfo{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: name, BlockSize: fakeExpansionBlockSize, NumBlocks: f.raidSize / fakeExpansionBlockSize}}
		if !f.raidNoInfo {
			raid := &spdktypes.BdevRaidInfo{Name: name}
			for member := range f.raidMembers {
				raid.BaseBdevsList = append(raid.BaseBdevsList, spdktypes.BaseBdev{Name: member, IsConfigured: true})
			}
			// An evicted or removed slot is reported without a name.
			raid.BaseBdevsList = append(raid.BaseBdevsList, spdktypes.BaseBdev{})
			info.DriverSpecific = &spdktypes.BdevDriverSpecific{Raid: raid}
		}
		return []spdktypes.BdevInfo{info}, nil
	}
	expansionBdevRaidRemoveBaseBdev = func(_ *spdkclient.Client, name string) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.removed = append(f.removed, name)
		if err := f.removeErr[name]; err != nil {
			return false, err
		}
		delete(f.raidMembers, name)
		return true, nil
	}
	sizeRefreshBdevNvmeResetController = func(_ *spdkclient.Client, name string) (bool, error) {
		f.mu.Lock()
		delay := f.resetDelay
		f.mu.Unlock()
		time.Sleep(delay)

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
			size = f.defaultBdevSize
		}
		if size == 0 {
			return nil, nil
		}
		return []spdktypes.BdevInfo{{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: name, BlockSize: fakeExpansionBlockSize, NumBlocks: size / fakeExpansionBlockSize}}}, nil
	}
	expansionBdevRaidResize = func(_ *spdkclient.Client, name string, expectedSizeInMib uint64) (spdktypes.BdevRaidResizeResponse, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		call := f.resizeCalls
		f.resizeCalls++
		f.resizeExpectedMib = append(f.resizeExpectedMib, expectedSizeInMib)
		if call < len(f.resizeErrs) && f.resizeErrs[call] != nil {
			return spdktypes.BdevRaidResizeResponse{}, f.resizeErrs[call]
		}
		// Like SPDK, refuse an expected size that the members do not provide.
		if expectedSizeInMib*helpertypes.MiB > f.grownSize {
			return spdktypes.BdevRaidResizeResponse{}, noSpaceErr()
		}
		resp := spdktypes.BdevRaidResizeResponse{Name: name, OldBlockcnt: f.raidSize / fakeExpansionBlockSize}
		if f.grownSize > f.raidSize {
			f.raidSize = f.grownSize
			resp.Resized = true
		}
		resp.NewBlockcnt = f.raidSize / fakeExpansionBlockSize
		return resp, nil
	}

	return func() {
		expansionBdevRaidGet, expansionBdevRaidRemoveBaseBdev, sizeRefreshBdevNvmeResetController, sizeRefreshBdevGetBdevs, expansionBdevRaidResize = origGet, origRemove, origReset, origGetBdevs, origResize
		raidResizeRetryInterval, raidResizeTimeout = origRetryInterval, origTimeout
	}
}

func noSpaceErr() error {
	return jsonrpc.JSONClientError{ErrorDetail: &jsonrpc.ResponseError{Code: -28, Message: "No space left on device"}}
}

func noSuchDeviceErr() error {
	return jsonrpc.JSONClientError{ErrorDetail: &jsonrpc.ResponseError{Code: jsonrpc.RespErrorCodeNoSuchDevice}}
}

// newExpansionTestEngine builds an engine with RW replica backends that report
// replicaSize, each with a backend bdev named "<replica>n1".
func newExpansionTestEngine(frontend string, specSize, replicaSize uint64, factory ServiceClientFactory, names ...string) *Engine {
	e := NewEngine("engine-a", "vol-a", frontend, specSize, make(chan interface{}, 1), defaultTestSnapshotMaxCount, factory)
	e.backends = map[string]Backend{}
	for i, name := range names {
		u := newFakeBackend(name, fmt.Sprintf("10.0.0.%d:1234", i+1))
		u.SetMode(lhtypes.ModeRW)
		u.SetBdevName(name + "n1")
		u.View = &BackendView{SpecSize: replicaSize}
		e.backends[name] = u
	}
	return e
}

func (s *TestSuite) TestRefreshBackendsForExpansionResetsDispatchableBackends(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion resets only dispatchable backends, then resizes the RAID bdev")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2", "r3")
	f.setMembers(e)
	e.backends["r3"].SetMode(lhtypes.ModeERR)

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resetCalls["r1"], Equals, 1)
	c.Assert(f.resetCalls["r2"], Equals, 1)
	c.Assert(f.resetCalls["r3"], Equals, 0)
	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(f.raidSize, Equals, uint64(200*helpertypes.MiB))
	c.Assert(e.lastExpansionError, Equals, "")
}

func (s *TestSuite) TestRefreshBackendsForExpansionRetriesReset(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion retries a refused reset")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.resets["r1"] = []fakeResetResult{{success: false}, {success: true}}
	f.resets["r2"] = []fakeResetResult{{err: errors.New("controller is reconnecting")}, {success: true}}
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resetCalls["r1"], Equals, 2)
	c.Assert(f.resetCalls["r2"], Equals, 2)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
}

func (s *TestSuite) TestRefreshBackendsForExpansionHandlesFailedBackend(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion treats a missing controller as a failed replica")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.resets["r1"] = []fakeResetResult{{err: noSuchDeviceErr()}}
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)
	e.lastExpansionError = "earlier replica failure"

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	// A deleted controller cannot recover, so it is not retried.
	c.Assert(f.resetCalls["r1"], Equals, 1)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(f.removed, DeepEquals, []string{"r1n1"})
	c.Assert(strings.HasPrefix(e.lastExpansionError, "earlier replica failure; "), Equals, true, Commentf("err=%s", e.lastExpansionError))
	c.Assert(strings.Contains(e.lastExpansionError, "r1"), Equals, true)
	// The RAID bdev is resized after the failed replica left it.
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(f.raidSize, Equals, uint64(200*helpertypes.MiB))
}

func (s *TestSuite) TestRefreshBackendsForExpansionAllBackendsFail(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion fails when no backend can be refreshed")

	f := newFakeExpansionSPDK(100, 200)
	f.resets["r1"] = []fakeResetResult{{err: noSuchDeviceErr()}}
	f.resets["r2"] = []fakeResetResult{{err: noSuchDeviceErr()}}
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)

	err := e.refreshBackendsForExpansion(nil, 200)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "all backends failed to refresh"), Equals, true, Commentf("err=%v", err))
	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.raidCalls, Equals, 0)
	c.Assert(f.resizeCalls, Equals, 0)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
}

func (s *TestSuite) TestRefreshBackendsForExpansionRemoveFailure(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion returns a RAID member removal failure")

	f := newFakeExpansionSPDK(100, 200)
	f.resets["r1"] = []fakeResetResult{{err: noSuchDeviceErr()}}
	f.removeErr["r1n1"] = errors.New("raid busy")
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)

	err := e.refreshBackendsForExpansion(nil, 200)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "raid busy"), Equals, true, Commentf("err=%v", err))
	c.Assert(f.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestRefreshBackendsForExpansionUndersizedBackend(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion treats a backend that did not read back the new size as a failed replica")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.bdevSizes["r1n1"] = 100 * helpertypes.MiB
	f.bdevErr["r2n1"] = errors.New("rpc failed")
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2", "r3")
	f.setMembers(e)

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r3"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	removed := map[string]bool{}
	for _, name := range f.removed {
		removed[name] = true
	}
	c.Assert(removed, DeepEquals, map[string]bool{"r1n1": true, "r2n1": true})
	c.Assert(strings.Contains(e.lastExpansionError, fmt.Sprintf("after the controller reset: bdev r1n1 is at size %d, requested %d", 100*helpertypes.MiB, 200*helpertypes.MiB)), Equals, true, Commentf("err=%s", e.lastExpansionError))
	c.Assert(strings.Contains(e.lastExpansionError, "rpc failed"), Equals, true, Commentf("err=%s", e.lastExpansionError))
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(f.raidSize, Equals, uint64(200*helpertypes.MiB))

	// With every backend undersized, the expansion fails without touching the RAID bdev.
	g := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	g.defaultBdevSize = 100 * helpertypes.MiB
	restore := g.install()
	e = newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	g.setMembers(e)
	err = e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	restore()
	c.Assert(err, ErrorMatches, "all backends failed to refresh during expansion.*")
	c.Assert(g.removed, HasLen, 0)
	c.Assert(g.resizeCalls, Equals, 0)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
}

func (s *TestSuite) TestResizeRaidBdevForExpansion(c *C) {
	fmt.Println("Testing resizeRaidBdevForExpansion grows the RAID bdev and checks its size")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)

	err := e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(f.resizeExpectedMib, DeepEquals, []uint64{200})
	c.Assert(f.raidSize, Equals, uint64(200*helpertypes.MiB))

	// Nothing left to grow is not an error.
	err = e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resizeCalls, Equals, 2)
}

func (s *TestSuite) TestResizeRaidBdevForExpansionRetriesBusy(c *C) {
	fmt.Println("Testing resizeRaidBdevForExpansion retries while the RAID bdev is busy")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.resizeErrs = []error{busyErr(), busyErr()}
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1")
	f.setMembers(e)

	err := e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resizeCalls, Equals, 3)
	c.Assert(f.raidSize, Equals, uint64(200*helpertypes.MiB))
}

func (s *TestSuite) TestResizeRaidBdevForExpansionBusyTimeout(c *C) {
	fmt.Println("Testing resizeRaidBdevForExpansion gives up when the RAID bdev stays busy")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()
	busy := make([]error, 1000)
	for i := range busy {
		busy[i] = busyErr()
	}
	f.resizeErrs = busy

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1")
	f.setMembers(e)

	err := e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, ErrorMatches, "timed out resizing raid bdev engine-a.*")
	c.Assert(f.resizeCalls > 1, Equals, true, Commentf("resizeCalls=%d", f.resizeCalls))
	c.Assert(f.raidSize, Equals, uint64(100*helpertypes.MiB))
}

func (s *TestSuite) TestResizeRaidBdevForExpansionRejectsUnalignedSize(c *C) {
	fmt.Println("Testing resizeRaidBdevForExpansion refuses a size that is not a multiple of 1 MiB before calling SPDK")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1")
	f.setMembers(e)

	err := e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB+1)
	c.Assert(err, ErrorMatches, "cannot resize raid bdev engine-a to size 209715201, which is not a multiple of 1 MiB")
	c.Assert(f.resizeCalls, Equals, 0)
	c.Assert(f.raidSize, Equals, uint64(100*helpertypes.MiB))
}

func (s *TestSuite) TestResizeRaidBdevForExpansionFailures(c *C) {
	fmt.Println("Testing resizeRaidBdevForExpansion returns a resize error and a RAID bdev that did not grow")

	// Other errors are not retried.
	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.resizeErrs = []error{noSuchDeviceErr()}
	restore := f.install()
	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1")
	f.setMembers(e)
	err := e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	restore()
	c.Assert(err, ErrorMatches, "failed to resize raid bdev engine-a.*")
	c.Assert(f.resizeCalls, Equals, 1)

	// The RAID bdev would stay smaller than requested: SPDK refuses the expected size with
	// ENOSPC, which is not retried.
	g := newFakeExpansionSPDK(100*helpertypes.MiB, 150*helpertypes.MiB)
	g.defaultBdevSize = 200 * helpertypes.MiB
	g.bdevSizes["r1n1"] = 150 * helpertypes.MiB
	restore = g.install()
	e = newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	g.setMembers(e)
	err = e.resizeRaidBdevForExpansion(nil, 200*helpertypes.MiB)
	restore()
	c.Assert(err, ErrorMatches, "failed to resize raid bdev engine-a.*No space left on device.*")
	c.Assert(g.resizeCalls, Equals, 1)
	c.Assert(g.resizeExpectedMib, DeepEquals, []uint64{200})
	c.Assert(g.raidSize, Equals, uint64(100*helpertypes.MiB))
}

func (s *TestSuite) TestRefreshBackendsForExpansionRaidLookupErrors(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion returns RAID bdev lookup errors")

	fErr := newFakeExpansionSPDK(100, 200)
	fErr.raidErr = errors.New("rpc failed")
	restore := fErr.install()
	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1")
	err := e.refreshBackendsForExpansion(nil, 200)
	restore()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "rpc failed"), Equals, true, Commentf("err=%v", err))
	c.Assert(fErr.resizeCalls, Equals, 0)

	fEmpty := newFakeExpansionSPDK(100, 200)
	fEmpty.raidEmpty = true
	restore = fEmpty.install()
	e = newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1")
	err = e.refreshBackendsForExpansion(nil, 200)
	restore()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not found"), Equals, true, Commentf("err=%v", err))
	c.Assert(fEmpty.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestRemoveFailedReplicasFromRaidBdev(c *C) {
	fmt.Println("Testing removeFailedReplicasFromRaidBdev removes only ERR replicas with a bdev")

	f := newFakeExpansionSPDK(200, 200)
	f.removeErr["r2n1"] = noSuchDeviceErr()
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2", "r3", "r4")
	f.setMembers(e)
	e.backends["r2"].SetMode(lhtypes.ModeERR)
	e.backends["r3"].SetMode(lhtypes.ModeERR)
	e.backends["r4"].SetMode(lhtypes.ModeERR)
	e.backends["r4"].SetBdevName("")

	err := e.removeFailedReplicasFromRaidBdev(nil)
	// An already removed base bdev is not an error.
	c.Assert(err, IsNil)
	removed := map[string]bool{}
	for _, name := range f.removed {
		removed[name] = true
	}
	c.Assert(removed, DeepEquals, map[string]bool{"r2n1": true, "r3n1": true})
	// The removal does not resize the RAID bdev.
	c.Assert(f.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestEngineExpandNoopWhenRaidAtTarget(c *C) {
	fmt.Println("Testing Engine Expand no-op when replicas and RAID bdev are at the target")

	f := newFakeExpansionSPDK(200, 200)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)
	e.lastExpansionError = "stale"
	e.lastExpansionFailedAt = "stale"

	err := e.Expand(nil, 200)
	c.Assert(err, IsNil)
	c.Assert(e.SpecSize, Equals, uint64(200))
	c.Assert(e.lastExpansionError, Equals, "")
	c.Assert(e.lastExpansionFailedAt, Equals, "")
	c.Assert(f.resetCalls, HasLen, 0)
	c.Assert(f.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestEngineExpandNoopRecoversFromExpansionFailure(c *C) {
	fmt.Println("Testing Engine Expand no-op recovers an engine whose expansion failed after the RAID bdev was resized")

	f := newFakeExpansionSPDK(200, 200)
	defer f.install()()

	// For example, the bdev_raid_resize response was lost.
	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)
	e.finishRaid1Expansion(100, 200, errors.New("failed to resize raid bdev engine-a: connection reset"))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.SpecSize, Equals, uint64(100))

	err := e.Expand(nil, 200)
	c.Assert(err, IsNil)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.expansionErrorMsg, Equals, "")
	c.Assert(e.lastExpansionError, Equals, "")
	c.Assert(e.SpecSize, Equals, uint64(200))
	c.Assert(f.resizeCalls, Equals, 0)
}

// raidBdevInfo returns a RAID bdev of the given size for the spec size check.
func raidBdevInfo(size uint64) *spdktypes.BdevInfo {
	return &spdktypes.BdevInfo{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: "engine-a", BlockSize: fakeExpansionBlockSize, NumBlocks: size / fakeExpansionBlockSize}}
}

func (s *TestSuite) TestEngineExpandNoopKeepsSizeMismatchWithoutExpansionFailure(c *C) {
	fmt.Println("Testing Engine Expand no-op keeps a spec size mismatch that no expansion left behind")

	f := newFakeExpansionSPDK(200, 200)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)
	updateRequired := false
	e.applyValidateAndUpdateErrorNoLock(e.validateAndMaybeAdjustSpecSizeNoLock(raidBdevInfo(200)), &updateRequired)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.ErrorMsg, Equals, "engine spec size 100 is smaller than actual raid bdev size 200 for engine engine-a")
	c.Assert(e.expansionErrorMsg, Equals, "")

	err := e.Expand(nil, 200)
	c.Assert(err, IsNil)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestEngineExpandNoopKeepsUnrelatedError(c *C) {
	fmt.Println("Testing Engine Expand no-op keeps an error state that did not come from expansion")

	for _, tc := range []struct {
		name  string
		setUp func(e *Engine)
		msg   string
	}{
		{
			name: "no expansion history",
			setUp: func(e *Engine) {
				e.State = lhtypes.InstanceStateError
				e.ErrorMsg = "no RW replica"
			},
			msg: "no RW replica",
		},
		{
			name: "newer error after a failed expansion",
			setUp: func(e *Engine) {
				e.finishRaid1Expansion(100, 200, errors.New("failed to resize raid bdev"))
				updateRequired := false
				e.applyValidateAndUpdateErrorNoLock(errors.New("bdev lost"), &updateRequired)
			},
			msg: "bdev lost",
		},
		{
			name: "error after a partial expansion",
			setUp: func(e *Engine) {
				e.appendExpansionError("replica r3 failed to expand")
				e.finishRaid1Expansion(100, 200, nil)
				updateRequired := false
				e.applyValidateAndUpdateErrorNoLock(errors.New("bdev lost"), &updateRequired)
			},
			msg: "bdev lost",
		},
		{
			name: "error state set after a failed expansion",
			setUp: func(e *Engine) {
				e.finishRaid1Expansion(100, 200, errors.New("failed to resize raid bdev"))
				e.SetErrorState()
			},
			msg: "failed to resize raid bdev",
		},
	} {
		f := newFakeExpansionSPDK(200, 200)
		restore := f.install()

		e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
		f.setMembers(e)
		tc.setUp(e)

		err := e.Expand(nil, 200)
		restore()
		c.Assert(err, IsNil, Commentf(tc.name))
		c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError), Commentf(tc.name))
		c.Assert(e.ErrorMsg, Equals, tc.msg, Commentf(tc.name))
	}
}

func (s *TestSuite) TestEngineExpandNoopRecoveryMarksEvictedReplicaERR(c *C) {
	fmt.Println("Testing Engine Expand no-op recovery marks a replica evicted from the RAID bdev as ERR and still succeeds")

	f := newFakeExpansionSPDK(200, 200)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	f.setMembers(e)
	e.finishRaid1Expansion(100, 200, errors.New("failed to resize raid bdev engine-a: connection reset"))
	f.evict("r2n1")

	err := e.Expand(nil, 200)
	c.Assert(err, IsNil)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.ModeERR)
	// The eviction is a warning of a successful expansion, without the earlier failure.
	c.Assert(e.SpecSize, Equals, uint64(200))
	c.Assert(strings.HasPrefix(e.lastExpansionError, "the expansion succeeded, but some replica expansion failed: "), Equals, true, Commentf("lastExpansionError=%v", e.lastExpansionError))
	c.Assert(strings.Contains(e.lastExpansionError, "r2n1 was evicted"), Equals, true, Commentf("lastExpansionError=%v", e.lastExpansionError))
	c.Assert(strings.Contains(e.lastExpansionError, "connection reset"), Equals, false, Commentf("lastExpansionError=%v", e.lastExpansionError))
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
	c.Assert(e.expansionErrorMsg, Equals, "")

	// With no RW replica left in the RAID bdev, the engine stays in error and
	// keeps the earlier expansion failure.
	g := newFakeExpansionSPDK(200, 200)
	defer g.install()()
	e = newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")
	e.finishRaid1Expansion(100, 200, errors.New("failed to resize raid bdev engine-a: connection reset"))
	lastExpansionError, lastExpansionFailedAt := e.lastExpansionError, e.lastExpansionFailedAt
	err = e.Expand(nil, 200)
	c.Assert(err, ErrorMatches, "no RW replica is a member of raid bdev engine-a after expansion.*")
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.SpecSize, Equals, uint64(100))
	c.Assert(e.lastExpansionError, Equals, lastExpansionError)
	c.Assert(e.lastExpansionFailedAt, Equals, lastExpansionFailedAt)
	c.Assert(e.TryLock(), Equals, true)
	e.Unlock()
}

func (s *TestSuite) TestEngineExpandResizesUndersizedRaid(c *C) {
	fmt.Println("Testing Engine Expand refreshes the backends and resizes the RAID bdev when replicas grew but the RAID bdev did not")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)
	e.finishRaid1Expansion(100*helpertypes.MiB, 200*helpertypes.MiB, errors.New("failed to resize raid bdev engine-a: busy"))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))

	err := e.Expand(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resetCalls["r1"], Equals, 1)
	c.Assert(f.resetCalls["r2"], Equals, 1)
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(e.SpecSize, Equals, uint64(200*helpertypes.MiB))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.lastExpansionError, Equals, "")
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineExpandKeepsUnrelatedErrorAfterRefresh(c *C) {
	fmt.Println("Testing Engine Expand keeps an unrelated error state when a refresh-only retry succeeds")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)
	e.finishRaid1Expansion(100*helpertypes.MiB, 200*helpertypes.MiB, errors.New("failed to resize raid bdev engine-a: busy"))
	e.SetErrorState()
	errorMsg := e.ErrorMsg
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.expansionErrorMsg, Equals, "")

	err := e.Expand(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(f.resizeCalls, Equals, 1)
	// The RAID bdev grew, so the size advances, but the error state is not the expansion's.
	c.Assert(e.SpecSize, Equals, uint64(200*helpertypes.MiB))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.ErrorMsg, Equals, errorMsg)
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineFinishExpansionKeepsUnrelatedError(c *C) {
	fmt.Println("Testing Engine finishExpansion success keeps an error state that the expansion did not cause")

	for _, tc := range []struct {
		name              string
		errorMsg          string
		expansionErrorMsg string
	}{
		{name: "no expansion error", errorMsg: "bdev lost"},
		{name: "newer error", errorMsg: "bdev lost", expansionErrorMsg: "failed to resize raid bdev"},
		{name: "error without message"},
	} {
		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
		e.State = lhtypes.InstanceStateError
		e.ErrorMsg = tc.errorMsg
		e.expansionErrorMsg = tc.expansionErrorMsg

		e.finishRaid1Expansion(10, 20, nil)

		c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError), Commentf(tc.name))
		c.Assert(e.ErrorMsg, Equals, tc.errorMsg, Commentf(tc.name))
		c.Assert(e.expansionErrorMsg, Equals, "", Commentf(tc.name))
		c.Assert(e.SpecSize, Equals, uint64(20), Commentf(tc.name))
	}
}

func (s *TestSuite) TestRefreshBackendsForExpansionResetsInParallel(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion resets the backends in parallel")

	const delay = 300 * time.Millisecond
	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.resetDelay = delay
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2", "r3", "r4")
	f.setMembers(e)

	start := time.Now()
	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	elapsed := time.Since(start)
	c.Assert(err, IsNil)
	for _, name := range []string{"r1", "r2", "r3", "r4"} {
		c.Assert(f.resetCalls[name], Equals, 1, Commentf("backend %s", name))
	}
	// Four sequential resets would take at least 4 x delay.
	c.Assert(elapsed < 2*delay, Equals, true, Commentf("elapsed=%v", elapsed))
	c.Assert(f.resizeCalls, Equals, 1)
}

func (s *TestSuite) TestEngineExpandPartialFailureSucceedsWithWarning(c *C) {
	fmt.Println("Testing Engine Expand succeeds at the new size when some replicas fail, keeping a warning")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	f.bdevSizes["r2n1"] = 100 * helpertypes.MiB
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)

	err := e.Expand(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.SpecSize, Equals, uint64(200*helpertypes.MiB))
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.ModeERR)
	c.Assert(f.removed, DeepEquals, []string{"r2n1"})
	c.Assert(f.resizeCalls, Equals, 1)
	c.Assert(strings.HasPrefix(e.lastExpansionError, "the expansion succeeded, but some replica expansion failed: "), Equals, true, Commentf("lastExpansionError=%v", e.lastExpansionError))
	c.Assert(strings.Contains(e.lastExpansionError, "r2n1"), Equals, true, Commentf("lastExpansionError=%v", e.lastExpansionError))
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
}

func (s *TestSuite) TestEngineExpandFailsWhenRaidDoesNotGrow(c *C) {
	fmt.Println("Testing Engine Expand fails and keeps the old size when the RAID bdev does not reach the new size")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 150*helpertypes.MiB)
	f.defaultBdevSize = 200 * helpertypes.MiB
	f.resizeErrs = []error{busyErr()}
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2")
	f.setMembers(e)

	err := e.Expand(nil, 200*helpertypes.MiB)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "No space left on device"), Equals, true, Commentf("err=%v", err))
	c.Assert(f.resizeCalls, Equals, 2)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.SpecSize, Equals, uint64(100*helpertypes.MiB))
	c.Assert(e.expansionErrorMsg, Equals, e.ErrorMsg)
}

func (s *TestSuite) TestEngineExpandNoopRaidLookupError(c *C) {
	fmt.Println("Testing Engine Expand returns a RAID lookup error from the no-op check and releases the lock")

	f := newFakeExpansionSPDK(200, 200)
	f.raidErr = errors.New("rpc failed")
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1")
	f.setMembers(e)

	err := e.Expand(nil, 200)
	c.Assert(err, NotNil)
	c.Assert(e.SpecSize, Equals, uint64(100))

	// Expand must release the engine lock on this early return.
	c.Assert(e.TryLock(), Equals, true)
	e.Unlock()
}

func (s *TestSuite) TestEngineExpandReplicaClientFailure(c *C) {
	fmt.Println("Testing Engine Expand keeps the old size when replica clients cannot be created")

	f := newFakeExpansionSPDK(100, 100)
	defer f.install()()

	factory := func(string) (*client.SPDKClient, error) { return nil, errors.New("dial failed") }
	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 100, factory, "r1")
	f.setMembers(e)

	err := e.Expand(nil, 200)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "dial failed"), Equals, true, Commentf("err=%v", err))
	c.Assert(e.SpecSize, Equals, uint64(100))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
	c.Assert(e.lastExpansionError, Not(Equals), "")
	c.Assert(f.resetCalls, HasLen, 0)
	c.Assert(f.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestEngineExpandRejectsUblk(c *C) {
	fmt.Println("Testing Engine Expand rejects the ublk frontend before touching replicas")

	factoryCalled := false
	factory := func(string) (*client.SPDKClient, error) {
		factoryCalled = true
		return nil, errors.New("unexpected")
	}
	e := newExpansionTestEngine(lhtypes.FrontendUBLK, 100, 100, factory, "r1")

	err := e.Expand(nil, 200)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "ublk"), Equals, true, Commentf("err=%v", err))
	c.Assert(factoryCalled, Equals, false)
	c.Assert(e.SpecSize, Equals, uint64(100))
}

func (s *TestSuite) TestRefreshBackendsForExpansionMarksEvictedReplicaERR(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion marks a replica evicted by raid1 as ERR before resizing")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2", "r3")
	f.setMembers(e)
	f.evict("r2n1")

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.ModeERR)
	c.Assert(e.backends["r3"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(strings.Contains(e.lastExpansionError, "r2n1 was evicted"), Equals, true)
	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.resizeCalls, Equals, 1)
}

func (s *TestSuite) TestRefreshBackendsForExpansionKeepsEarlierExpansionError(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion appends an eviction to an earlier partial failure")

	f := newFakeExpansionSPDK(100*helpertypes.MiB, 200*helpertypes.MiB)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100*helpertypes.MiB, 200*helpertypes.MiB, nil, "r1", "r2", "r3")
	f.setMembers(e)
	e.backends["r3"].SetMode(lhtypes.ModeERR)
	e.lastExpansionError = "r3 failed to expand"
	f.evict("r2n1")

	err := e.refreshBackendsForExpansion(nil, 200*helpertypes.MiB)
	c.Assert(err, IsNil)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.ModeERR)
	c.Assert(strings.HasPrefix(e.lastExpansionError, "r3 failed to expand; "), Equals, true)
	c.Assert(strings.Contains(e.lastExpansionError, "r2n1 was evicted"), Equals, true)
}

func (s *TestSuite) TestRefreshBackendsForExpansionAllReplicasEvicted(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion fails when no RW replica is a RAID member")

	f := newFakeExpansionSPDK(100, 200)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1", "r2")

	err := e.refreshBackendsForExpansion(nil, 200)
	c.Assert(err, ErrorMatches, "no RW replica is a member of raid bdev engine-a after expansion.*")
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.ModeRW)
	c.Assert(f.resizeCalls, Equals, 0)
}

func (s *TestSuite) TestRefreshBackendsForExpansionMembershipLookupErrors(c *C) {
	fmt.Println("Testing refreshBackendsForExpansion fails when the RAID members cannot be read")

	f := newFakeExpansionSPDK(100, 200)
	defer f.install()()

	e := newExpansionTestEngine(lhtypes.FrontendSPDKTCPBlockdev, 100, 200, nil, "r1")
	f.raidNoInfo = true
	err := e.refreshBackendsForExpansion(nil, 200)
	c.Assert(err, ErrorMatches, "cannot find the members of raid bdev engine-a")
	c.Assert(f.resizeCalls, Equals, 0)
}

// fakeExpansionDmDevice records the calls that block and unblock the frontend I/O.
type fakeExpansionDmDevice struct {
	calls      []string
	syncErr    error
	suspendErr error
	resumeErr  error
}

func (d *fakeExpansionDmDevice) install() (restore func()) {
	origSync, origSuspend, origResume := expansionSyncFilesystems, expansionSuspendDmDevice, expansionResumeDmDevice
	expansionSyncFilesystems = func() error {
		d.calls = append(d.calls, "sync")
		return d.syncErr
	}
	expansionSuspendDmDevice = func(*EngineFrontend) error {
		d.calls = append(d.calls, "suspend")
		return d.suspendErr
	}
	expansionResumeDmDevice = func(*EngineFrontend) error {
		d.calls = append(d.calls, "resume")
		return d.resumeErr
	}
	return func() {
		expansionSyncFilesystems, expansionSuspendDmDevice, expansionResumeDmDevice = origSync, origSuspend, origResume
	}
}

func (s *TestSuite) TestEngineFrontendResumeIOAfterExpansion(c *C) {
	fmt.Println("Testing EngineFrontend resumeIOAfterExpansion always resumes and keeps the earlier error")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1), nil)

	d := &fakeExpansionDmDevice{}
	restore := d.install()
	c.Assert(ef.resumeIOAfterExpansion(20, nil), IsNil)
	err := ef.resumeIOAfterExpansion(20, errors.New("failed to expand engine"))
	restore()
	c.Assert(err, ErrorMatches, "failed to expand engine")
	c.Assert(d.calls, DeepEquals, []string{"resume", "resume"})

	d = &fakeExpansionDmDevice{resumeErr: errors.New("dmsetup resume failed")}
	restore = d.install()
	err = ef.resumeIOAfterExpansion(20, errors.New("failed to expand engine"))
	restore()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "failed to expand engine"), Equals, true, Commentf("err=%v", err))
	c.Assert(strings.Contains(err.Error(), "failed to resume engine frontend ef-a after expansion to 20: dmsetup resume failed"), Equals, true, Commentf("err=%v", err))
	c.Assert(d.calls, DeepEquals, []string{"resume"})
}

func (s *TestSuite) TestEngineFrontendFinishExpansionWithReplicaFailureAdvancesSize(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion moves to the new size and keeps a replica failure as a warning")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1), nil)
	ef.metadataDir = c.MkDir()

	warning := "the expansion succeeded, but some replica expansion failed: replica expand failed"
	ef.finishExpansion(10, true, 20, nil, warning, "2026-03-10T00:00:00Z", 20)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.ErrorMsg, Equals, "")
	c.Assert(ef.SpecSize, Equals, uint64(20))
	c.Assert(ef.ActualSize, Equals, uint64(20))
	c.Assert(ef.lastExpansionError, Equals, warning)
	c.Assert(ef.lastExpansionFailedAt, Equals, "2026-03-10T00:00:00Z")
	c.Assert(ef.isExpanding, Equals, false)
}

// fakeExplicitResize records the calls that turn explicit_resize on for a RAID bdev.
type fakeExplicitResize struct {
	calls map[string]bool
	err   error
}

func (f *fakeExplicitResize) install() (restore func()) {
	orig := raidBdevSetExplicitResize
	f.calls = map[string]bool{}
	raidBdevSetExplicitResize = func(_ *spdkclient.Client, name string, explicitResize bool) (bool, error) {
		f.calls[name] = explicitResize
		if f.err != nil {
			return false, f.err
		}
		return true, nil
	}
	return func() {
		raidBdevSetExplicitResize = orig
	}
}

func (s *TestSuite) TestEnableExplicitResizeForRaid1Engine(c *C) {
	fmt.Println("Testing a RAID1 engine turns explicit_resize on for its RAID bdev")

	f := &fakeExplicitResize{}
	defer f.install()()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{
		"r1": newReplicaBackend("r1", "10.0.0.1:1234", nil),
		"r2": newReplicaBackend("r2", "10.0.0.2:1234", nil),
	}
	c.Assert(e.enableExplicitResizeNoLock(nil), IsNil)
	c.Assert(f.calls, DeepEquals, map[string]bool{"engine-a": true})
}

func (s *TestSuite) TestEnableExplicitResizeSkipsECEngine(c *C) {
	fmt.Println("Testing an EC-layout engine keeps the automatic resize of its RAID bdev")

	f := &fakeExplicitResize{err: errors.New("must not be called")}
	defer f.install()()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil)}
	c.Assert(e.enableExplicitResizeNoLock(nil), IsNil)
	c.Assert(f.calls, HasLen, 0)
}

func (s *TestSuite) TestEnableExplicitResizeFailureFailsCreation(c *C) {
	fmt.Println("Testing a failure to turn explicit_resize on is returned, like a raid bdev creation failure")

	f := &fakeExplicitResize{err: errors.New("Method not found")}
	defer f.install()()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.backends = map[string]Backend{"r1": newReplicaBackend("r1", "10.0.0.1:1234", nil)}
	err := e.enableExplicitResizeNoLock(nil)
	c.Assert(err, ErrorMatches, "failed to enable explicit resize for raid bdev engine-a: Method not found")
	c.Assert(f.calls, DeepEquals, map[string]bool{"engine-a": true})
}

func (s *TestSuite) TestEngineFinishRaid1ExpansionSuccessClearsExpansionErrorState(c *C) {
	fmt.Println("Testing Engine finishRaid1Expansion success clears the error state an earlier expansion left behind")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.State = lhtypes.InstanceStateError
	e.ErrorMsg = "previous failure"
	e.expansionErrorMsg = e.ErrorMsg

	e.finishRaid1Expansion(10, 20, nil)

	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.SpecSize, Equals, uint64(20))
}

func (s *TestSuite) TestEngineFinishRaid1ExpansionPartialFailureSucceedsWithWarning(c *C) {
	fmt.Println("Testing Engine finishRaid1Expansion partial failure succeeds with a warning")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.lastExpansionError = "replica expand failed"

	e.finishRaid1Expansion(10, 20, nil)

	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.SpecSize, Equals, uint64(20))
	c.Assert(e.lastExpansionError, Equals, "the expansion succeeded, but some replica expansion failed: replica expand failed")
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
}

func (s *TestSuite) TestEngineFinishRaid1ExpansionPartialFailureUsesNewSize(c *C) {
	fmt.Println("Testing Engine finishRaid1Expansion partial failure moves to the new size whatever the spec size was during expand")

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil)
	e.SpecSize = 15
	e.lastExpansionError = "replica expand failed"

	e.finishRaid1Expansion(10, 20, nil)

	c.Assert(e.SpecSize, Equals, uint64(20))
}
