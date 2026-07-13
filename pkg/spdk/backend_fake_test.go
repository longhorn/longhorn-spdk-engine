package spdk

import (
	"sync"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// fakeBackend is a test-only Backend that returns preset responses without
// dialing SPDK, so tests can exercise engine code without real replica or
// ShardGroup processes. Mode, BdevName, and Address hold real state so tests
// can check the engine updates them (e.g. marking ERR on an RPC failure).
type fakeBackend struct {
	mu sync.RWMutex

	name     string
	address  string
	mode     types.Mode
	bdevName string

	// Preset Get response; ViewErr (if set) is returned instead of View.
	View    *BackendView
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

// newFakeBackend builds a fakeBackend with default Mode=ModeWO, matching
// newReplicaBackend / newShardGroupBackend lifecycle. Tests typically call
// SetMode(ModeRW) afterwards to simulate a connected backend.
func newFakeBackend(name, address string) *fakeBackend {
	return &fakeBackend{
		name:    name,
		address: address,
		mode:    types.ModeWO,
	}
}

func (u *fakeBackend) Name() string { return u.name }

func (u *fakeBackend) Address() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.address
}

func (u *fakeBackend) Mode() types.Mode {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.mode
}

func (u *fakeBackend) SetMode(m types.Mode) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mode = m
}

func (u *fakeBackend) BdevName() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.bdevName
}

func (u *fakeBackend) SetBdevName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bdevName = name
}

func (u *fakeBackend) Get() (*BackendView, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.ViewErr != nil {
		return nil, u.ViewErr
	}
	return u.View, nil
}

func (u *fakeBackend) SnapshotCreate(snapshotName string, opts *api.SnapshotOptions) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotCreateCalls = append(u.SnapshotCreateCalls, fakeSnapshotCreateCall{SnapshotName: snapshotName, Opts: opts})
	return u.SnapshotCreateErr
}

func (u *fakeBackend) SnapshotDelete(snapshotName string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotDeleteCalls = append(u.SnapshotDeleteCalls, snapshotName)
	return u.SnapshotDeleteErr
}

func (u *fakeBackend) SnapshotRevert(snapshotName string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotRevertCalls = append(u.SnapshotRevertCalls, snapshotName)
	return u.SnapshotRevertErr
}

func (u *fakeBackend) SnapshotPurge() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotPurgeCalls++
	return u.SnapshotPurgeErr
}

func (u *fakeBackend) SnapshotHash(snapshotName string, rehash bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.SnapshotHashCalls = append(u.SnapshotHashCalls, fakeSnapshotHashCall{SnapshotName: snapshotName, Rehash: rehash})
	return u.SnapshotHashErr
}

func (u *fakeBackend) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.BackingImageCalls = append(u.BackingImageCalls, fakeBackingImageCall{Name: name, LvsUUID: lvsUUID})
	if u.BackingImageGetErr != nil {
		return nil, u.BackingImageGetErr
	}
	return u.BackingImageView, nil
}

// Compile-time check that fakeBackend satisfies the Backend interface.
var _ Backend = (*fakeBackend)(nil)
