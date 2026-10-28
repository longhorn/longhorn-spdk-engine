package spdk

import (
	"os"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestGetUblkTargetCPUMask(c *C) {
	if old, ok := os.LookupEnv(types.EnvSPDKPollerCPUMask); ok {
		defer func() { _ = os.Setenv(types.EnvSPDKPollerCPUMask, old) }()
	} else {
		defer func() { _ = os.Unsetenv(types.EnvSPDKPollerCPUMask) }()
	}

	cases := []struct {
		name     string
		set      bool
		value    string
		expected string
	}{
		{name: "unset keeps the SPDK default", set: false, expected: ""},
		{name: "empty keeps the SPDK default", set: true, value: "", expected: ""},
		{name: "blank keeps the SPDK default", set: true, value: "  ", expected: ""},
		{name: "poller cpumask is used", set: true, value: "0xfe", expected: "0xfe"},
		{name: "surrounding space is trimmed", set: true, value: " 0xfe\n", expected: "0xfe"},
	}

	for _, tc := range cases {
		c.Logf("case: %s", tc.name)
		if tc.set {
			c.Assert(os.Setenv(types.EnvSPDKPollerCPUMask, tc.value), IsNil)
		} else {
			c.Assert(os.Unsetenv(types.EnvSPDKPollerCPUMask), IsNil)
		}
		c.Assert(getUblkTargetCPUMask(), Equals, tc.expected)
	}
}
