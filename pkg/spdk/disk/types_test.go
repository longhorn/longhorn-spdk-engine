package disk

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestIsVfioPci(c *C) {
	fmt.Println("Testing isVfioPci function with various driver strings")

	testCases := []struct {
		name     string
		driver   string
		expected bool
	}{
		{
			name:     "VfioPci with hyphen",
			driver:   "vfio-pci",
			expected: true,
		},
		{
			name:     "VfioPci with underscore",
			driver:   "vfio_pci",
			expected: true,
		},
		{
			name:     "Non-VfioPci driver",
			driver:   "virtio-pci",
			expected: false,
		},
		{
			name:     "Empty driver",
			driver:   "",
			expected: false,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result := isVfioPci(tc.driver)
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for driver %s, got %v", tc.expected, tc.driver, result))
	}
}

func (s *TestSuite) TestIsUioPciGeneric(c *C) {
	fmt.Println("Testing isUioPciGeneric function with various driver strings")

	testCases := []struct {
		name     string
		driver   string
		expected bool
	}{
		{
			name:     "UioPciGeneric with hyphen",
			driver:   "uio-pci-generic",
			expected: true,
		},
		{
			name:     "UioPciGeneric with underscore",
			driver:   "uio_pci_generic",
			expected: true,
		},
		{
			name:     "Non-UioPciGeneric driver",
			driver:   "virtio-pci",
			expected: false,
		},
		{
			name:     "Empty driver",
			driver:   "",
			expected: false,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result := isUioPciGeneric(tc.driver)
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for driver %s, got %v", tc.expected, tc.driver, result))
	}
}

func (s *TestSuite) TestIsBDF(c *C) {
	fmt.Println("Testing isBDF with various path strings")

	testCases := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "BDF path",
			path:     "0000:00:10.0",
			expected: true,
		},
		{
			name:     "/dev/disk/by-path path",
			path:     "/dev/disk/by-path/pci-0000:00:10.0-nvme-1",
			expected: false,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result := isBDF(tc.path)
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for path %s, got %v", tc.expected, tc.path, result))
	}
}
