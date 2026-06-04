package spdk

import (
	"sync"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeUpstream is a test-only Upstream that returns preset responses without
// dialing SPDK, so tests can exercise engine code without real replica or
// ShardGroup processes. Mode, BdevName, and Address hold real state so tests
// can check the engine updates them (e.g. marking ERR on an RPC failure).
type fakeUpstream struct {
	mu sync.RWMutex

	name     string
	address  string
	mode     types.Mode
	bdevName string

	// Preset Get response; ViewErr (if set) is returned instead of View.
	View    *UpstreamView
	ViewErr error

	// Per-RPC preset errors (zero value = success).
	SnapshotCreateErr  error
	SnapshotDeleteErr  error
	SnapshotRevertErr  error
	SnapshotPurgeErr   error
	SnapshotHashErr    error
	BackingImageView   *api.BackingImage
	BackingImageGetErr error

	// Recorded call args for assertions.
	SnapshotCreateCalls []fakeSnapshotCreateCall
	SnapshotDeleteCalls []string
	SnapshotRevertCalls []string
	SnapshotPurgeCalls  int
	SnapshotHashCalls   []fakeSnapshotHashCall
	BackingImageCalls   []fakeBackingImageCall
}

type fakeSnapshotCreateCall struct {
	SnapshotName string
	Opts         *api.SnapshotOptions
}

type fakeSnapshotHashCall struct {
	SnapshotName string
	Rehash       bool
}

type fakeBackingImageCall struct {
	Name    string
	LvsUUID string
}

// newFakeUpstream builds a fakeUpstream with default Mode=ModeWO, matching
// newReplicaUpstream / newShardGroupUpstream lifecycle. Tests typically call
// SetMode(ModeRW) afterwards to simulate a connected upstream.
func newFakeUpstream(name, address string) *fakeUpstream {
	return &fakeUpstream{
		name:    name,
		address: address,
		mode:    types.ModeWO,
	}
}

func (u *fakeUpstream) Name() string { return u.name }

func (u *fakeUpstream) Address() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.address
}

func (u *fakeUpstream) Mode() types.Mode {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.mode
}

func (u *fakeUpstream) SetMode(m types.Mode) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mode = m
}

func (u *fakeUpstream) BdevName() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.bdevName
}

func (u *fakeUpstream) SetBdevName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bdevName = name
}

func (u *fakeUpstream) Get() (*UpstreamView, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.ViewErr != nil {
		return nil, u.ViewErr
	}
	return u.View, nil
}

func (u *fakeUpstream) SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotCreateCalls = append(u.SnapshotCreateCalls, fakeSnapshotCreateCall{SnapshotName: snapshotName, Opts: opts})
	return u.SnapshotCreateErr
}

func (u *fakeUpstream) SnapshotDelete(snapshotName string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotDeleteCalls = append(u.SnapshotDeleteCalls, snapshotName)
	return u.SnapshotDeleteErr
}

func (u *fakeUpstream) SnapshotRevert(snapshotName string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotRevertCalls = append(u.SnapshotRevertCalls, snapshotName)
	return u.SnapshotRevertErr
}

func (u *fakeUpstream) SnapshotPurge() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotPurgeCalls++
	return u.SnapshotPurgeErr
}

func (u *fakeUpstream) SnapshotHash(snapshotName string, rehash bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotHashCalls = append(u.SnapshotHashCalls, fakeSnapshotHashCall{SnapshotName: snapshotName, Rehash: rehash})
	return u.SnapshotHashErr
}

func (u *fakeUpstream) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.BackingImageCalls = append(u.BackingImageCalls, fakeBackingImageCall{Name: name, LvsUUID: lvsUUID})
	if u.BackingImageGetErr != nil {
		return nil, u.BackingImageGetErr
	}
	return u.BackingImageView, nil
}

// Compile-time check that fakeUpstream satisfies the Upstream interface.
var _ Upstream = (*fakeUpstream)(nil)
