package spdk

import (
	"fmt"

	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// TestCopyEcCountersFromBdevInfoMapping verifies that each of the 18 EC
// counters lands in its correct proto field. Distinct values per source
// field catch any cross-wiring at the helper boundary; a zero remaining on
// EcStatus after the copy means the helper missed that field.
func (s *TestSuite) TestCopyEcCountersFromBdevInfoMapping(c *C) {
	fmt.Println("Testing copyEcCountersFromBdevInfo maps all 18 EC counters")

	info := &spdktypes.BdevEcInfo{
		UnmapsSubmitted:            101,
		UnmapsCompleted:            102,
		UnmapsDeferredBusy:         103,
		UnmapsViaWriteZeros:        104,
		UnmapFanoutMisses:          105,
		UnmappedStripes:            106,
		DegradedReadEioDirty:       201,
		DegradedReadsReconstructed: 202,
		RmwTotal:                   301,
		RmwDeferredScrub:           302,
		RmwDeferredDirty:           303,
		RmwDeferredInflight:        304,
		FullStripeWrites:           401,
		FullStripeWritesDeferred:   402,
		UnmapsFailed:               501,
		UnmappedReadsSynthesized:   502,
		WritesIntoUnmapped:         503,
		WritesIntoUnmappedFailed:   504,
	}

	status := &spdkrpc.EcStatus{}
	copyEcCountersFromBdevInfo(status, info)

	// UNMAP fan-out
	c.Assert(status.UnmapsSubmitted, Equals, uint64(101))
	c.Assert(status.UnmapsCompleted, Equals, uint64(102))
	c.Assert(status.UnmapsDeferredBusy, Equals, uint64(103))
	c.Assert(status.UnmapsViaWriteZeros, Equals, uint64(104))
	c.Assert(status.UnmapFanoutMisses, Equals, uint64(105))
	c.Assert(status.UnmappedStripes, Equals, uint64(106))

	// Degraded reads - DegradedReadEioDirty is the alert-grade signal
	c.Assert(status.DegradedReadEioDirty, Equals, uint64(201))
	c.Assert(status.DegradedReadsReconstructed, Equals, uint64(202))

	// RMW pressure
	c.Assert(status.RmwTotal, Equals, uint64(301))
	c.Assert(status.RmwDeferredScrub, Equals, uint64(302))
	c.Assert(status.RmwDeferredDirty, Equals, uint64(303))
	c.Assert(status.RmwDeferredInflight, Equals, uint64(304))

	// Full-stripe writes
	c.Assert(status.FullStripeWrites, Equals, uint64(401))
	c.Assert(status.FullStripeWritesDeferred, Equals, uint64(402))

	// UNMAP and write-into-unmapped counters. WritesIntoUnmappedFailed is the
	// one worth alerting on: the write reached disk, but the stripe stayed
	// marked unmapped, so later reads return zeros instead of that data.
	c.Assert(status.UnmapsFailed, Equals, uint64(501))
	c.Assert(status.UnmappedReadsSynthesized, Equals, uint64(502))
	c.Assert(status.WritesIntoUnmapped, Equals, uint64(503))
	c.Assert(status.WritesIntoUnmappedFailed, Equals, uint64(504))
}

// TestCopyEcCountersFromBdevInfoZeroValues exercises the all-zero path
// (fresh array, no traffic yet) to confirm zero is propagated, not skipped.
func (s *TestSuite) TestCopyEcCountersFromBdevInfoZeroValues(c *C) {
	fmt.Println("Testing copyEcCountersFromBdevInfo preserves zero values")

	status := &spdkrpc.EcStatus{
		// Pre-populate with non-zero to verify the helper overwrites.
		UnmapsSubmitted:      999,
		DegradedReadEioDirty: 999,
	}
	info := &spdktypes.BdevEcInfo{}
	copyEcCountersFromBdevInfo(status, info)

	c.Assert(status.UnmapsSubmitted, Equals, uint64(0))
	c.Assert(status.DegradedReadEioDirty, Equals, uint64(0))
	c.Assert(status.RmwTotal, Equals, uint64(0))
	c.Assert(status.FullStripeWrites, Equals, uint64(0))
}
