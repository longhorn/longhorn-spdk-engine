package spdk

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	clientpkg "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

const fakeExpansionBlockSize = 512

// fakeExpansionSPDK fakes the SPDK and replica calls of an in-place RAID1 expansion.
type fakeExpansionSPDK struct {
	mu sync.Mutex

	newSize          uint64
	raidSize         uint64
	raidMembers      map[string]bool
	removeScheduled  map[string]bool
	bdevSizes        map[string]uint64
	replicaExpandErr map[string]error
	// staleSize makes the bdev of a replica never read the new size.
	staleSize map[string]bool
	// noticeDelay makes the bdev of a replica read the new size this long after the replica
	// expanded, as on the namespace attribute changed notice of its NVMe-oF target.
	noticeDelay time.Duration
	resizeErrs  []error
	calls       []string

	replicaExpanded map[string]bool
	inFlight        int
	maxInFlight     int
	removed         []string
	resizeCalls     []uint64
	raidCreateCalls int
}

func newFakeExpansionSPDK(oldSize, newSize uint64, replicas ...string) *fakeExpansionSPDK {
	f := &fakeExpansionSPDK{
		newSize:          newSize,
		raidSize:         oldSize,
		raidMembers:      map[string]bool{},
		removeScheduled:  map[string]bool{},
		bdevSizes:        map[string]uint64{},
		replicaExpandErr: map[string]error{},
		staleSize:        map[string]bool{},
		replicaExpanded:  map[string]bool{},
	}
	for _, r := range replicas {
		f.raidMembers[r+"n1"] = true
		f.bdevSizes[r+"n1"] = oldSize
	}
	return f
}

// track records how many replica expansions and controller resets run at the same time.
func (f *fakeExpansionSPDK) track() (done func()) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.mu.Unlock()
	time.Sleep(time.Millisecond)
	return func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}
}

func newSPDKBusyError() error {
	return jsonrpc.JSONClientError{
		Method:      "bdev_raid_resize",
		ErrorDetail: &jsonrpc.ResponseError{Code: jsonrpc.RespErrorCodeDeviceOrResourceBusy, Message: "Device or resource busy"},
	}
}

func (f *fakeExpansionSPDK) install() (restore func()) {
	origCreate, origReplicaExpand, origRaidGet := engineBdevRaidCreate, expansionReplicaExpand, expansionBdevRaidGet
	origRemove, origGetBdevs := expansionBdevRaidRemoveBaseBdev, expansionBdevGetBdevs
	origResize, origInterval, origTimeout := expansionBdevRaidResize, expansionRetryInterval, expansionRetryTimeout

	expansionRetryInterval = time.Millisecond
	expansionRetryTimeout = 100 * time.Millisecond

	engineBdevRaidCreate = func(_ *spdkclient.Client, _ string, _ []string, _ string, _ spdktypes.BdevRaidCreateOptions) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.raidCreateCalls++
		return false, errors.New("the raid bdev must not be recreated")
	}
	expansionReplicaExpand = func(_ *clientpkg.SPDKClient, name string, size uint64) error {
		defer f.track()()
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, "expand "+name)
		if err := f.replicaExpandErr[name]; err != nil {
			return err
		}
		if size != f.newSize {
			return fmt.Errorf("unexpected size %v", size)
		}
		f.replicaExpanded[name] = true
		switch {
		case f.staleSize[name]:
		case f.noticeDelay > 0:
			go func() {
				time.Sleep(f.noticeDelay)
				f.mu.Lock()
				defer f.mu.Unlock()
				f.bdevSizes[name+"n1"] = f.newSize
			}()
		default:
			f.bdevSizes[name+"n1"] = f.newSize
		}
		return nil
	}
	expansionBdevGetBdevs = func(_ *spdkclient.Client, name string) ([]spdktypes.BdevInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		size, ok := f.bdevSizes[name]
		if !ok {
			return nil, fmt.Errorf("bdev %s not found", name)
		}
		return []spdktypes.BdevInfo{{BdevInfoBasic: spdktypes.BdevInfoBasic{Name: name, BlockSize: fakeExpansionBlockSize, NumBlocks: size / fakeExpansionBlockSize}}}, nil
	}
	expansionBdevRaidGet = func(_ *spdkclient.Client, name string) ([]spdktypes.BdevInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		raid := &spdktypes.BdevRaidInfo{}
		for member := range f.raidMembers {
			raid.BaseBdevsList = append(raid.BaseBdevsList, spdktypes.BaseBdev{Name: member, IsConfigured: true, RemoveScheduled: f.removeScheduled[member]})
		}
		return []spdktypes.BdevInfo{{
			BdevInfoBasic:  spdktypes.BdevInfoBasic{Name: name, BlockSize: fakeExpansionBlockSize, NumBlocks: f.raidSize / fakeExpansionBlockSize},
			DriverSpecific: &spdktypes.BdevDriverSpecific{Raid: raid},
		}}, nil
	}
	expansionBdevRaidRemoveBaseBdev = func(_ *spdkclient.Client, name string) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.raidMembers, name)
		f.removed = append(f.removed, name)
		return true, nil
	}
	expansionBdevRaidResize = func(_ *spdkclient.Client, name string, expectedSizeInMib uint64) (spdktypes.BdevRaidResizeResponse, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.resizeCalls = append(f.resizeCalls, expectedSizeInMib)
		if len(f.resizeErrs) > 0 {
			err := f.resizeErrs[0]
			f.resizeErrs = f.resizeErrs[1:]
			return spdktypes.BdevRaidResizeResponse{}, err
		}
		minSize := uint64(0)
		for member := range f.raidMembers {
			if minSize == 0 || f.bdevSizes[member] < minSize {
				minSize = f.bdevSizes[member]
			}
		}
		if minSize < expectedSizeInMib*helpertypes.MiB {
			return spdktypes.BdevRaidResizeResponse{}, fmt.Errorf("no space left on device")
		}
		old := f.raidSize
		f.raidSize = minSize
		return spdktypes.BdevRaidResizeResponse{Name: name, OldBlockcnt: old / fakeExpansionBlockSize,
			NewBlockcnt: minSize / fakeExpansionBlockSize, Resized: old != minSize}, nil
	}

	return func() {
		engineBdevRaidCreate, expansionReplicaExpand, expansionBdevRaidGet = origCreate, origReplicaExpand, origRaidGet
		expansionBdevRaidRemoveBaseBdev, expansionBdevGetBdevs = origRemove, origGetBdevs
		expansionBdevRaidResize, expansionRetryInterval, expansionRetryTimeout = origResize, origInterval, origTimeout
	}
}

func newExpansionTestEngine(size uint64, replicaSizes map[string]uint64) *Engine {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, size, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil, false)
	e.State = lhtypes.InstanceStateRunning
	backends := map[string]Backend{}
	i := 1
	for name, replicaSize := range replicaSizes {
		u := newFakeBackend(name, fmt.Sprintf("10.0.0.%d:1234", i))
		u.SetMode(lhtypes.ModeRW)
		u.SetBdevName(name + "n1")
		u.View = &BackendView{SpecSize: replicaSize}
		backends[name] = u
		i++
	}
	e.backends = backends
	return e
}

func (s *TestSuite) TestEngineExpandGrowsRaidBdevInPlace(c *C) {
	fmt.Println("Testing Engine.Expand grows the RAID bdev in place without recreating it")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2", "r3")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize, "r3": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.raidCreateCalls, Equals, 0)
	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.replicaExpanded, DeepEquals, map[string]bool{"r1": true, "r2": true, "r3": true})
	// The replicas are expanded one at a time.
	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2", "expand r3"})
	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(f.maxInFlight, Equals, 1)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.lastExpansionError, Equals, "")
	c.Assert(e.isExpanding, Equals, false)
}

// The replicas expand their lvols while they stay exposed, so their NVMe bdevs read the new
// size on the namespace attribute changed notice, which the expansion waits for.
func (s *TestSuite) TestEngineExpandWaitsForReplicaBdevSize(c *C) {
	fmt.Println("Testing Engine.Expand waits for the replica bdevs to read the new size")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.noticeDelay = 10 * time.Millisecond
	defer f.install()()
	expansionRetryTimeout = 5 * time.Second

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.lastExpansionError, Equals, "")
}

func (s *TestSuite) TestEngineExpandRemovesReplicaThatFailedToExpand(c *C) {
	fmt.Println("Testing Engine.Expand removes a replica that failed to expand and still grows the RAID bdev")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2", "r3")
	f.replicaExpandErr["r3"] = errors.New("replica expand failed")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize, "r3": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.raidCreateCalls, Equals, 0)
	c.Assert(f.removed, DeepEquals, []string{"r3n1"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.backends["r3"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(strings.Contains(e.lastExpansionError, "replica expand failed"), Equals, true)
	c.Assert(e.lastExpansionFailedAt, Not(Equals), "")
}

func (s *TestSuite) TestEngineExpandRemovesReplicaThatDoesNotReportNewSize(c *C) {
	fmt.Println("Testing Engine.Expand removes a replica whose bdev does not report the new size")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.staleSize["r2"] = true
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.removed, DeepEquals, []string{"r2n1"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(strings.Contains(e.lastExpansionError, "smaller than"), Equals, true)
}

func (s *TestSuite) TestEngineExpandFailsReplicaEvictedFromRaidBdev(c *C) {
	fmt.Println("Testing Engine.Expand fails a RW replica that raid1 evicted on its own")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2", "r3")
	delete(f.raidMembers, "r2n1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize, "r3": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(strings.Contains(e.lastExpansionError, "no longer a member"), Equals, true)
}

// evictOnExpand makes raid1 evict the bdev of replica when it expands, as after a write that
// failed meanwhile. The bdev still reads the new size.
func evictOnExpand(f *fakeExpansionSPDK, replica string) {
	expand := expansionReplicaExpand
	expansionReplicaExpand = func(cli *clientpkg.SPDKClient, name string, size uint64) error {
		err := expand(cli, name, size)
		if name == replica {
			f.mu.Lock()
			delete(f.raidMembers, replica+"n1")
			f.mu.Unlock()
		}
		return err
	}
}

func (s *TestSuite) TestEngineExpandGoesOnAfterEvictionOfOtherReplica(c *C) {
	fmt.Println("Testing Engine.Expand, like V1, expands the remaining replica after the other one was evicted")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	defer f.install()()
	evictOnExpand(f, "r1")

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2"})
	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(strings.Contains(e.lastExpansionError, "no longer a member"), Equals, true)
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineExpandFailsWhenRemainingReplicaDoesNotReachSize(c *C) {
	fmt.Println("Testing Engine.Expand fails, keeping the RAID bdev size, when the replica left after an eviction does not reach the new size")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.staleSize["r1"] = true
	defer f.install()()
	evictOnExpand(f, "r2")

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), ErrorMatches, ".*all replicas failed to expand.*")

	// r2 expanded, but it is out of sync, so the expansion failed and r1, the only member
	// left, is not removed from the RAID bdev.
	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2"})
	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.resizeCalls, HasLen, 0)
	c.Assert(f.raidCreateCalls, Equals, 0)
	c.Assert(f.raidSize, Equals, oldSize)
	// As in V1, only the out-of-sync replica, which raid1 evicted, is marked as ERR.
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.SpecSize, Equals, oldSize)
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineExpandFailsReplicaWhoseRemovalIsPending(c *C) {
	fmt.Println("Testing Engine.Expand fails a replica whose removal from the RAID bdev is pending")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.removeScheduled["r2n1"] = true
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.removed, DeepEquals, []string{"r2n1"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.SpecSize, Equals, newSize)
}

func (s *TestSuite) TestEngineExpandTwoReplicasWithOneEvicted(c *C) {
	fmt.Println("Testing Engine.Expand expands the only member of a RAID bdev whose other replica was evicted before")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	delete(f.raidMembers, "r2n1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.SpecSize, Equals, newSize)
}

func (s *TestSuite) TestEngineExpandContinuesAfterEvictionWithOtherMembers(c *C) {
	fmt.Println("Testing Engine.Expand fails an evicted replica and goes on while other members are left")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2", "r3")
	defer f.install()()
	evictOnExpand(f, "r1")

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize, "r3": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2", "expand r3"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r3"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(strings.Contains(e.lastExpansionError, "no longer a member"), Equals, true)
}

func (s *TestSuite) TestEngineExpandSingleReplica(c *C) {
	fmt.Println("Testing Engine.Expand expands the only replica of a single-replica volume")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.calls, DeepEquals, []string{"expand r1"})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
}

func (s *TestSuite) TestEngineExpandTwoReplicasGoesOnAfterReplicaFailure(c *C) {
	fmt.Println("Testing Engine.Expand of a two-replica volume, like V1, goes on and succeeds after the first replica failed to expand")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.replicaExpandErr["r1"] = errors.New("failed r1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.calls, DeepEquals, []string{"expand r1", "expand r2"})
	c.Assert(f.raidCreateCalls, Equals, 0)
	c.Assert(f.removed, DeepEquals, []string{"r1n1"})
	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(strings.Contains(e.lastExpansionError, "failed r1"), Equals, true)
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineExpandSingleReplicaFailure(c *C) {
	fmt.Println("Testing Engine.Expand keeps the RAID bdev size when the only replica fails to expand")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1")
	f.replicaExpandErr["r1"] = errors.New("failed r1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize})
	err := e.Expand(nil, newSize)
	c.Assert(err, ErrorMatches, ".*all replicas failed to expand.*")

	c.Assert(f.removed, HasLen, 0)
	c.Assert(f.resizeCalls, HasLen, 0)
	c.Assert(e.SpecSize, Equals, oldSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
}

func (s *TestSuite) TestEngineExpandRetryGoesOnAfterEviction(c *C) {
	fmt.Println("Testing Engine.Expand completing an earlier expansion goes on with the other replica after an eviction")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.replicaExpanded["r1"], f.replicaExpanded["r2"] = true, true
	defer f.install()()
	evictOnExpand(f, "r1")

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": newSize, "r2": newSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeERR))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.SpecSize, Equals, newSize)
}

func (s *TestSuite) TestEngineExpandRetriesBusyRaidBdev(c *C) {
	fmt.Println("Testing Engine.Expand retries bdev_raid_resize while the RAID bdev is busy")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.resizeErrs = []error{newSPDKBusyError(), newSPDKBusyError()}
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, HasLen, 3)
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
}

func (s *TestSuite) TestEngineExpandFailsWhenRaidBdevStaysBusy(c *C) {
	fmt.Println("Testing Engine.Expand fails with a single-line error when the RAID bdev stays busy")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	for range 1000 {
		f.resizeErrs = append(f.resizeErrs, newSPDKBusyError())
	}
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	err := e.Expand(nil, newSize)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(e.ErrorMsg, "\n"), Equals, false, Commentf("error: %q", e.ErrorMsg))
	c.Assert(f.raidSize, Equals, oldSize)
	c.Assert(e.SpecSize, Equals, oldSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

// Like the V1 engine controller, StartExpand returns once the expansion has started, and the
// engine expands in the background, holding the engine lock, so that the request does not
// time out while the replicas are expanded one by one.
func (s *TestSuite) TestEngineStartExpandExpandsInTheBackground(c *C) {
	fmt.Println("Testing the engine expands in the background")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	defer f.install()()
	release := make(chan struct{})
	expand := expansionReplicaExpand
	expansionReplicaExpand = func(cli *clientpkg.SPDKClient, name string, size uint64) error {
		<-release
		return expand(cli, name, size)
	}

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	c.Assert(e.StartExpand(nil, newSize), IsNil)
	// The expansion keeps the engine lock until it is done.
	c.Assert(e.TryRLock(), Equals, false)
	c.Assert(e.isExpanding, Equals, true)
	close(release)

	select {
	case <-e.UpdateCh:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for the engine to expand")
	}
	res := e.Get()
	c.Assert(res.IsExpanding, Equals, false)
	c.Assert(res.SpecSize, Equals, newSize)
	c.Assert(res.LastExpansionError, Equals, "")
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.backends["r1"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
	c.Assert(e.backends["r2"].Mode(), Equals, lhtypes.Mode(lhtypes.ModeRW))
}

// StartExpand does not start an expansion that fails the precheck, and reports the failure
// to the request.
func (s *TestSuite) TestEngineStartExpandRejectsExpansion(c *C) {
	fmt.Println("Testing the engine rejects an expansion that fails the precheck")

	oldSize := uint64(10 * helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, oldSize, "r1")
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize})
	c.Assert(e.StartExpand(nil, oldSize/2), ErrorMatches, ".*smaller size.*")
	c.Assert(e.TryLock(), Equals, true)
	e.Unlock()
	c.Assert(e.isExpanding, Equals, false)
}

func (s *TestSuite) TestEngineExpandFailsWhenRaidBdevCannotGrow(c *C) {
	fmt.Println("Testing Engine.Expand fails and keeps the old size when bdev_raid_resize fails")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.resizeErrs = []error{errors.New("resize failed")}
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": oldSize, "r2": oldSize})
	err := e.Expand(nil, newSize)
	c.Assert(err, NotNil)
	c.Assert(f.resizeCalls, HasLen, 1)
	c.Assert(f.raidSize, Equals, oldSize)
	c.Assert(e.SpecSize, Equals, oldSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateError))
}

func (s *TestSuite) TestEngineExpandGrowsRaidBdevAfterReplicasExpanded(c *C) {
	fmt.Println("Testing Engine.Expand grows the RAID bdev when an earlier expansion already expanded the replicas")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(oldSize, newSize, "r1", "r2")
	f.replicaExpanded["r1"], f.replicaExpanded["r2"] = true, true
	defer f.install()()

	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": newSize, "r2": newSize})
	e.State = lhtypes.InstanceStateError
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.raidSize, Equals, newSize)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestEngineExpandNoopWhenRaidBdevAlreadyGrown(c *C) {
	fmt.Println("Testing Engine.Expand does nothing when the engine, the replicas and the RAID bdev are at the requested size")

	newSize := uint64(20 * helpertypes.MiB)
	f := newFakeExpansionSPDK(newSize, newSize, "r1", "r2")
	defer f.install()()

	e := newExpansionTestEngine(newSize, map[string]uint64{"r1": newSize, "r2": newSize})
	e.lastExpansionError = "stale"
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, HasLen, 0)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.lastExpansionError, Equals, "")
}

// An engine created for an offline expansion is at the requested size before its RAID bdev
// is. If the expansion grew the RAID bdev but failed, a retry must not take the no-op
// shortcut, which would leave the engine in the error state.
func (s *TestSuite) TestEngineExpandCompletesFailedExpansionAtRequestedSize(c *C) {
	fmt.Println("Testing Engine.Expand moves an engine in the error state at the requested size back to running")

	newSize := uint64(20 * helpertypes.MiB)
	f := newFakeExpansionSPDK(newSize, newSize, "r1", "r2")
	f.replicaExpanded["r1"], f.replicaExpanded["r2"] = true, true
	defer f.install()()

	e := newExpansionTestEngine(newSize, map[string]uint64{"r1": newSize, "r2": newSize})
	e.State = lhtypes.InstanceStateError
	e.ErrorMsg = "failed to resize raid bdev"
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
}

func (s *TestSuite) TestEngineExpandCompletesExpansionAfterRaidBdevGrew(c *C) {
	fmt.Println("Testing Engine.Expand completes an earlier expansion that failed after the RAID bdev grew")

	oldSize, newSize := uint64(10*helpertypes.MiB), uint64(20*helpertypes.MiB)
	f := newFakeExpansionSPDK(newSize, newSize, "r1", "r2")
	f.replicaExpanded["r1"], f.replicaExpanded["r2"] = true, true
	defer f.install()()

	// finishExpansion of the earlier attempt put the engine in the error state at the old size.
	e := newExpansionTestEngine(oldSize, map[string]uint64{"r1": newSize, "r2": newSize})
	e.State = lhtypes.InstanceStateError
	e.ErrorMsg = "failed to resize raid bdev"
	c.Assert(e.Expand(nil, newSize), IsNil)

	c.Assert(f.resizeCalls, DeepEquals, []uint64{20})
	c.Assert(f.removed, HasLen, 0)
	c.Assert(e.SpecSize, Equals, newSize)
	c.Assert(e.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(e.ErrorMsg, Equals, "")
	c.Assert(e.isExpanding, Equals, false)
}

// fakeRaidCreate records the RAID bdev creations of engines.
type fakeRaidCreate struct {
	calls map[string]spdktypes.BdevRaidCreateOptions
	err   error
}

func (f *fakeRaidCreate) install() (restore func()) {
	orig := engineBdevRaidCreate
	f.calls = map[string]spdktypes.BdevRaidCreateOptions{}
	engineBdevRaidCreate = func(_ *spdkclient.Client, name string, _ []string, _ string, opts spdktypes.BdevRaidCreateOptions) (bool, error) {
		f.calls[name] = opts
		if f.err != nil {
			return false, f.err
		}
		return true, nil
	}
	return func() {
		engineBdevRaidCreate = orig
	}
}

func (s *TestSuite) TestCreateRaidBdevDisablesAutoResizeForRaid1Engine(c *C) {
	fmt.Println("Testing a RAID1 engine creates its RAID bdev with disable_auto_resize")

	f := &fakeRaidCreate{}
	defer f.install()()

	e := newExpansionTestEngine(10, map[string]uint64{"r1": 10, "r2": 10})
	c.Assert(e.createRaidBdevNoLock(nil, []string{"r1n1", "r2n1"}, ""), IsNil)
	c.Assert(f.calls, DeepEquals, map[string]spdktypes.BdevRaidCreateOptions{"engine-a": {DisableAutoResize: true}})
}

func (s *TestSuite) TestCreateRaidBdevKeepsAutoResizeForECEngine(c *C) {
	fmt.Println("Testing an EC-layout engine keeps the automatic resize of its RAID bdev")

	f := &fakeRaidCreate{}
	defer f.install()()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, make(chan interface{}, 1), defaultTestSnapshotMaxCount, nil, false)
	e.backends = map[string]Backend{"sg1": newShardGroupBackend("sg1", "10.0.0.1:1234", nil)}
	c.Assert(e.createRaidBdevNoLock(nil, []string{"sg1n1"}, ""), IsNil)
	c.Assert(f.calls, DeepEquals, map[string]spdktypes.BdevRaidCreateOptions{"engine-a": {}})
}

func (s *TestSuite) TestCreateRaidBdevFailureIsReturned(c *C) {
	fmt.Println("Testing a RAID bdev creation failure, for example from an SPDK without disable_auto_resize, is returned")

	f := &fakeRaidCreate{err: errors.New("spdk_json_decode_object failed")}
	defer f.install()()

	e := newExpansionTestEngine(10, map[string]uint64{"r1": 10})
	err := e.createRaidBdevNoLock(nil, []string{"r1n1"}, "")
	c.Assert(err, ErrorMatches, "spdk_json_decode_object failed")
}

func (s *TestSuite) TestEngineFrontendFinishExpansionWithEngineWarningMovesToNewSize(c *C) {
	fmt.Println("Testing EngineFrontend finish expansion moves to the new size when the engine expanded with a warning")

	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 10, 0, 0, make(chan interface{}, 1), nil, false)
	ef.metadataDir = c.MkDir()

	ef.finishExpansion(10, true, 20, nil, "r3 failed", "t1", 12)

	c.Assert(ef.State, Equals, lhtypes.InstanceState(lhtypes.InstanceStateRunning))
	c.Assert(ef.SpecSize, Equals, uint64(20))
	c.Assert(ef.lastExpansionError, Equals, "r3 failed")
	c.Assert(ef.lastExpansionFailedAt, Equals, "t1")
	c.Assert(ef.isExpanding, Equals, false)
}
