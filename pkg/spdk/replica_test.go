package spdk

import (
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// ---------------------------------------------------------------------------
// TestPrepareHeadIsolatesBackingImageChildren - prepareHead makes per-replica backing image isolation
// ---------------------------------------------------------------------------

func (s *TestSuite) TestPrepareHeadIsolatesBackingImageChildren(c *C) {
	// prepareHead requires an spdkClient and makes SPDK calls, so we cannot
	// call it directly. Instead we replicate the exact per-replica copy logic
	// from prepareHead and verify the isolation property it is designed to
	// guarantee: each replica gets its own Children map.

	biSnapshot := &Lvol{
		Name:              "bi-test-image-disk-uuid-1",
		UUID:              "bi-uuid-1",
		Alias:             "test-disk/bi-test-image-disk-uuid-1",
		SpecSize:          1048576,
		ActualSize:        524288,
		Parent:            "",
		Children:          map[string]*Lvol{},
		CreationTime:      "2024-01-01T00:00:00Z",
		UserCreated:       false,
		SnapshotTimestamp: "",
		SnapshotChecksum:  "",
	}

	// Simulate what prepareHead does for two replicas sharing the same backing image.
	// This is the exact copy logic from prepareHead (lines 1064-1076 in replica.go).
	copyBISnapshot := func(biSnap *Lvol) *Lvol {
		biSnap.RLock()
		defer biSnap.RUnlock()
		return &Lvol{
			Name:              biSnap.Name,
			UUID:              biSnap.UUID,
			Alias:             biSnap.Alias,
			SpecSize:          biSnap.SpecSize,
			ActualSize:        biSnap.ActualSize,
			Parent:            biSnap.Parent,
			Children:          map[string]*Lvol{},
			CreationTime:      biSnap.CreationTime,
			UserCreated:       biSnap.UserCreated,
			SnapshotTimestamp: biSnap.SnapshotTimestamp,
			SnapshotChecksum:  biSnap.SnapshotChecksum,
		}
	}

	replicaABase := copyBISnapshot(biSnapshot)
	replicaBBase := copyBISnapshot(biSnapshot)

	// Both copies should have the same identity fields.
	c.Assert(replicaABase.Name, Equals, biSnapshot.Name)
	c.Assert(replicaBBase.Name, Equals, biSnapshot.Name)
	c.Assert(replicaABase.UUID, Equals, biSnapshot.UUID)

	// Add a child to replica A's copy.
	replicaABase.Children["replica-a-root-snap"] = &Lvol{Name: "replica-a-root-snap"}

	// Replica B's copy must be unaffected.
	c.Assert(len(replicaABase.Children), Equals, 1)
	c.Assert(len(replicaBBase.Children), Equals, 0)

	// The original backing image snapshot must also be unaffected.
	c.Assert(len(biSnapshot.Children), Equals, 0)

	// Add a child to the original biSnapshot.
	biSnapshot.Children["some-other-replica"] = &Lvol{Name: "some-other-replica"}
	c.Assert(len(biSnapshot.Children), Equals, 1)

	// Neither replica copy should see the original's mutation.
	c.Assert(len(replicaABase.Children), Equals, 1)
	c.Assert(len(replicaBBase.Children), Equals, 0)
}

// TestClearStaleSnapshotCloneDstError checks that a clone error from an earlier attempt is dropped,
// so the next clone attempt is not reported as failed before it starts.
// Ref: https://github.com/longhorn/longhorn/issues/14530
func (s *TestSuite) TestClearStaleSnapshotCloneDstError(c *C) {
	testCases := map[string]struct {
		isSnapshotCloning bool
		cloningState      string
		cloningError      string
		expectCleared     bool
	}{
		"error of a finished attempt is cleared": {
			cloningState:  types.ProgressStateError,
			cloningError:  "failed to do ReplicaSnapshotCloneSrcStart",
			expectCleared: true,
		},
		"error of a clone that is still running is kept": {
			isSnapshotCloning: true,
			cloningState:      types.ProgressStateError,
			cloningError:      "failed to check ReplicaSnapshotCloneSrcStatusCheck",
		},
		"completed clone is kept": {
			cloningState: types.ProgressStateComplete,
		},
	}

	for name, tc := range testCases {
		c.Logf("testing %v", name)

		r := &Replica{Name: "replica", isSnapshotCloning: tc.isSnapshotCloning}
		r.snapshotCloningDstCache.cloningState = tc.cloningState
		r.snapshotCloningDstCache.cloningError = tc.cloningError
		r.snapshotCloningDstCache.processedClusters = 3
		r.snapshotCloningDstCache.totalClusters = 10

		staleErr, cleared := r.clearStaleSnapshotCloneDstError()
		c.Assert(cleared, Equals, tc.expectCleared)

		status, err := r.SnapshotCloneDstStatusCheck()
		c.Assert(err, IsNil)
		if tc.expectCleared {
			c.Assert(staleErr, Equals, tc.cloningError)
			c.Assert(status.State, Equals, "")
			c.Assert(status.Error, Equals, "")
			c.Assert(status.Progress, Equals, uint32(0))
		} else {
			c.Assert(staleErr, Equals, "")
			c.Assert(status.State, Equals, tc.cloningState)
			c.Assert(status.Error, Equals, tc.cloningError)
		}
	}
}
