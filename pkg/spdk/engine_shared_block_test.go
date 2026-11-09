package spdk

import (
	"github.com/cockroachdb/errors"

	. "gopkg.in/check.v1"

	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestGetTargetCntlidRange(c *C) {
	const ip = "10.42.1.21"

	// Without shared block the target keeps accepting a single host.
	for _, port := range []int32{1, 20006, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, false)
		c.Assert(minCntlid, Equals, getTargetCntlid(ip, port))
		c.Assert(maxCntlid, Equals, minCntlid)
	}

	for _, port := range []int32{1, 20006, 20011, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, true)
		c.Assert(minCntlid >= 1, Equals, true)
		c.Assert(maxCntlid <= maxValidCntlid, Equals, true)
		c.Assert(int(maxCntlid)-int(minCntlid)+1, Equals, sharedBlockMaxFrontends)
	}
}

func (s *TestSuite) TestEngineSharedBlockReportedInProto(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false)
	c.Assert(e.getWithoutLock().SharedBlock, Equals, false)

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true)
	c.Assert(e.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockOnlySupportsBlockdev(c *C) {
	for _, frontend := range []string{lhtypes.FrontendSPDKTCPNvmf, lhtypes.FrontendUBLK} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", frontend, 1048576, 0, 0, make(chan interface{}, 1), nil, true)

		err := ef.handleFrontend(nil, "10.0.0.1:20006")
		c.Assert(err, ErrorMatches, ".*not supported for shared block engine frontend.*")
	}

	// The maintenance mode attaches a shared block volume without a frontend.
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	c.Assert(ef.handleFrontend(nil, "10.0.0.1:20006"), IsNil)
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSwitchover(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(errors.Is(err, ErrSwitchOverTargetInvalidInput), Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestSaveAndLoadEngineFrontendRecordPreservesSharedBlock(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-1", "engine-a", "vol-a",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].SharedBlock, Equals, true)
}
