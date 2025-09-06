package disk

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) xxTestIsVfioPci(c *C) {
	// Add test case for isVfioPci function
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

func (s *TestSuite) xxTestIsUioPciGeneric(c *C) {
	// Add test case for isUioPciGeneric function
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
