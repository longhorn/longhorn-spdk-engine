package disk

import (
	"fmt"
	"strings"
	"testing"

	. "gopkg.in/check.v1"

	commontypes "github.com/longhorn/go-common-libs/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
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

func (s *TestSuite) TestIsBoundToUserspaceDriver(c *C) {
	fmt.Println("Testing IsBoundToUserspaceDriver with various driver strings")

	testCases := []struct {
		name     string
		driver   string
		expected bool
	}{
		{
			name:     "vfio-pci",
			driver:   "vfio-pci",
			expected: true,
		},
		{
			name:     "vfio_pci",
			driver:   "vfio_pci",
			expected: true,
		},
		{
			name:     "uio-pci-generic",
			driver:   "uio-pci-generic",
			expected: true,
		},
		{
			name:     "kernel nvme driver",
			driver:   "nvme",
			expected: false,
		},
		{
			name:     "no driver",
			driver:   "",
			expected: false,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result := IsBoundToUserspaceDriver(tc.driver)
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for driver %s, got %v", tc.expected, tc.driver, result))
	}
}

func (s *TestSuite) TestIsDetachedFromKernelDriver(c *C) {
	fmt.Println("Testing IsDetachedFromKernelDriver")

	testCases := []struct {
		name     string
		driver   string
		expected bool
	}{
		{
			name:     "vfio-pci",
			driver:   "vfio-pci",
			expected: true,
		},
		{
			name:     "uio_pci_generic",
			driver:   "uio_pci_generic",
			expected: true,
		},
		{
			name:     "no driver reported by the setup script",
			driver:   PciDriverNone,
			expected: true,
		},
		{
			name:     "empty driver",
			driver:   "",
			expected: true,
		},
		{
			name:     "kernel nvme driver",
			driver:   "nvme",
			expected: false,
		},
		{
			name:     "kernel virtio-pci driver",
			driver:   "virtio-pci",
			expected: false,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result := IsDetachedFromKernelDriver(tc.driver)
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for driver %s, got %v", tc.expected, tc.driver, result))
	}
}

// TestGetDriverForDetachedDevice covers longhorn/longhorn#13893: an NVMe device
// the kernel no longer drives after an interrupted disk creation must still be
// resolved to the nvme driver, otherwise the disk can never become ready again.
func (s *TestSuite) TestGetDriverForDetachedDevice(c *C) {
	fmt.Println("Testing getDriverForDetachedDevice with devices the kernel does not drive")

	testCases := []struct {
		name          string
		diskStatus    *helpertypes.DiskStatus
		expected      commontypes.DiskDriver
		expectedError bool
	}{
		{
			name:       "NVMe device bound to vfio-pci",
			diskStatus: &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "NVMe", Driver: "vfio-pci"},
			expected:   commontypes.DiskDriverNvme,
		},
		{
			name:       "NVMe device bound to uio_pci_generic",
			diskStatus: &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "NVMe", Driver: "uio_pci_generic"},
			expected:   commontypes.DiskDriverNvme,
		},
		{
			name:       "NVMe device type reported in lower case",
			diskStatus: &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "nvme", Driver: "vfio-pci"},
			expected:   commontypes.DiskDriverNvme,
		},
		{
			// An interrupted bind releases the kernel driver before the userspace
			// bind succeeds, so the device ends up driven by nothing.
			name:       "NVMe device left without any driver",
			diskStatus: &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "NVMe", Driver: PciDriverNone},
			expected:   commontypes.DiskDriverNvme,
		},
		{
			name:          "virtio device cannot be resolved without a block device",
			diskStatus:    &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "virtio", Driver: "vfio-pci"},
			expected:      commontypes.DiskDriverNone,
			expectedError: true,
		},
		{
			name:          "unknown device type",
			diskStatus:    &helpertypes.DiskStatus{Bdf: "0000:05:00.0", Type: "", Driver: "vfio-pci"},
			expected:      commontypes.DiskDriverNone,
			expectedError: true,
		},
	}
	for _, tc := range testCases {
		c.Logf("Running test case: %s", tc.name)
		result, err := getDriverForDetachedDevice(tc.diskStatus, tc.diskStatus.Bdf)
		if tc.expectedError {
			c.Assert(err, NotNil, Commentf("Expected an error for test case %s", tc.name))
			c.Assert(strings.Contains(err.Error(), "unbind"), Equals, true,
				Commentf("Expected an actionable unbind hint for test case %s, got %v", tc.name, err))
		} else {
			c.Assert(err, IsNil, Commentf("Unexpected error for test case %s: %v", tc.name, err))
		}
		c.Assert(result, Equals, tc.expected, Commentf("Expected %v for test case %s, got %v", tc.expected, tc.name, result))
	}
}
