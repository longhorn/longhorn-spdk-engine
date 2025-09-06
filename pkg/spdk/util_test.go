package spdk

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) xxTestSplitHostPort(c *C) {
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

func (s *TestSuite) xxTestExtractBackingImageAndDiskUUID(c *C) {
	type testCase struct {
		lvolName         string
		expectedBIName   string
		expectedDiskUUID string
		expectError      bool
	}
	testCases := map[string]testCase{
		"ExtractBackingImageAndDiskUUID(...): only disk with hyphens": {
			lvolName:         "bi-MyBackingImage-disk-12345-abcde",
			expectedBIName:   "MyBackingImage",
			expectedDiskUUID: "12345-abcde",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...): backing image name and disk with hyphens": {
			lvolName:         "bi-My-Backing-Image-disk-12345-abcde-xyz",
			expectedBIName:   "My-Backing-Image",
			expectedDiskUUID: "12345-abcde-xyz",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...): backing image name and disk without hyphens": {
			lvolName:         "bi-MyBackingImage-disk-12345",
			expectedBIName:   "MyBackingImage",
			expectedDiskUUID: "12345",
			expectError:      false,
		},
		"ExtractBackingImageAndDiskUUID(...):  doesn't start with bi- and doesn't contain -disk-": {
			lvolName:         "myWrongPattern",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): doesn't have disk in the lvol name": {
			lvolName:         "bi-MyBackingImage-",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): doesn't have backing image in the lvol name": {
			lvolName:         "-disk-123456-bi-abcdefg",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
		"ExtractBackingImageAndDiskUUID(...): empty string": {
			lvolName:         "",
			expectedBIName:   "",
			expectedDiskUUID: "",
			expectError:      true,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing ExtractBackingImageAndDiskUUID.%v", testName)
		// Call the function being tested
		biName, diskUUID, err := ExtractBackingImageAndDiskUUID(testCase.lvolName)

		if testCase.expectError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(testCase.expectedBIName, Equals, biName)
			c.Assert(testCase.expectedDiskUUID, Equals, diskUUID)
		}
	}
}
