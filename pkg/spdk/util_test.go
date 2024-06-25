package spdk

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestSplitHostPort(c *C) {
	type testCase struct {
		address      string
		expectedHost string
		expectedPort int32
		expectedErr  error
	}
	testCases := map[string]testCase{
		"splitHostPort(...): normal address": {
			address:      "127.0.0.1:8080",
			expectedHost: "127.0.0.1",
			expectedPort: 8080,
			expectedErr:  nil,
		},
		"splitHostPort(...): only host": {
			address:      "127.0.0.1",
			expectedHost: "127.0.0.1",
			expectedPort: 0,
			expectedErr:  nil,
		},
		"splitHostPort(...): only port": {
			address:      ":8080",
			expectedHost: "",
			expectedPort: 8080,
			expectedErr:  nil,
		},
		"splitHostPort(...): empty address": {
			address:      "",
			expectedHost: "",
			expectedPort: 0,
			expectedErr:  nil,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing splitHostPort.%v", testName)

		hsot, port, err := splitHostPort(testCase.address)
		c.Assert(err, IsNil)
		c.Assert(hsot, Equals, testCase.expectedHost)
		c.Assert(port, Equals, testCase.expectedPort)
	}
}
