package client

import (
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *TestSuite) TestEngineFrontendSwitchOverWithOptionsUnsupported(c *C) {
	cli := &SPDKClient{}

	err := cli.EngineFrontendSwitchOverWithOptions("ef-a", "engine-b", "10.0.0.2:3000", &EngineFrontendSwitchOverOptions{
		RequestID: "req-1",
	})
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "not supported"), Equals, true)
}
