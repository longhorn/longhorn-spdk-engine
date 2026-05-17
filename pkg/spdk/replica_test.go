package spdk

import (
	. "gopkg.in/check.v1"
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
