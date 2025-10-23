package util

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

// Mockable variables for dependency injection in tests
var (
	readDir       = os.ReadDir
	readlink      = os.Readlink
	evalSymlinks  = filepath.EvalSymlinks
	sysBlockConst = "/sys/block"
)

func fakeGetDevNameFromBDF(bdf string) (string, error) {
	entries, err := readDir(sysBlockConst)
	if err != nil {
		return "", errors.Wrap(err, "failed to read /sys/block")
	}

	re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(bdf) + `(/|$)`)

	for _, entry := range entries {
		dev := entry.Name()
		devPath := filepath.Join(sysBlockConst, dev, "device")

		_, err := readlink(devPath)
		if err != nil {
			// Ignore devices without a device symlink (like virtual devices: loop, ramdisk)
			continue
		}

		absPath, err := evalSymlinks(devPath)
		if err != nil {
			continue
		}

		logrus.Infof("Checking %s: resolved path %s", dev, absPath)
		if re.MatchString(absPath) {
			return dev, nil
		}
	}

	return "", fmt.Errorf("device not found for BDF %s", bdf)
}

func (s *TestSuite) TestGetDevNameFromBDF(c *C) {
	type testCase struct {
		name        string
		bdf         string
		expectedDev string
		expectedErr string // error message substring
		setup       func() // optional setup for mocks
	}

	testCases := []testCase{
		{
			name:        "valid BDF matching nvme device",
			bdf:         "0000:02:00.0",
			expectedDev: "nvme0c0n1",
			setup: func() {
				// Mock ReadDir to return real-like entries: loops and nvmess
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					entries := []os.DirEntry{
						mockDirEntry("loop0"),
						mockDirEntry("nvme0c0n1"),
						mockDirEntry("nvme0n1"),
						mockDirEntry("nvme1c1n1"),
					}
					return entries, nil
				}
				// Mock readlink: fail for loop0, succeed for others
				readlink = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "loop0" {
						return "", errors.New("no such file or directory")
					}
					if dev == "nvme0c0n1" {
						return "../../../nvme/nvme0", nil
					}
					if dev == "nvme0n1" {
						return "../../../virtual/nvme-subsystem/nvme-subsys0", nil
					}
					if dev == "nvme1c1n1" {
						return "../../../nvme/nvme1", nil
					}
					return "", errors.New("no link")
				}
				// Mock evalSymlinks: resolve to paths containing or not containing BDF
				evalSymlinks = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "nvme0c0n1" {
						return "/sys/devices/pci0000:00/0000:00:08.0/0000:02:00.0/nvme/nvme0/nvme0c0n1/device", nil
					}
					if dev == "nvme0n1" {
						return "/sys/devices/virtual/nvme-subsystem/nvme-subsys0/nvme0n1/device", nil
					}
					if dev == "nvme1c1n1" {
						return "/sys/devices/pci0000:c8/0000:c8:01.0/0000:c9:00.0/nvme/nvme1/nvme1c1n1/device", nil
					}
					return "", errors.New("eval error")
				}
			},
		},
		{
			name:        "valid BDF matching another nvme device",
			bdf:         "0000:c9:00.0",
			expectedDev: "nvme1c1n1",
			setup: func() {
				// Same setup as above
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					entries := []os.DirEntry{
						mockDirEntry("loop0"),
						mockDirEntry("nvme0c0n1"),
						mockDirEntry("nvme0n1"),
						mockDirEntry("nvme1c1n1"),
					}
					return entries, nil
				}
				readlink = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "loop0" {
						return "", errors.New("no such file or directory")
					}
					if dev == "nvme0c0n1" {
						return "../../../nvme/nvme0", nil
					}
					if dev == "nvme0n1" {
						return "../../../virtual/nvme-subsystem/nvme-subsys0", nil
					}
					if dev == "nvme1c1n1" {
						return "../../../nvme/nvme1", nil
					}
					return "", errors.New("no link")
				}
				evalSymlinks = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "nvme0c0n1" {
						return "/sys/devices/pci0000:00/0000:00:08.0/0000:02:00.0/nvme/nvme0/nvme0c0n1/device", nil
					}
					if dev == "nvme0n1" {
						return "/sys/devices/virtual/nvme-subsystem/nvme-subsys0/nvme0n1/device", nil
					}
					if dev == "nvme1c1n1" {
						return "/sys/devices/pci0000:c8/0000:c8:01.0/0000:c9:00.0/nvme/nvme1/nvme1c1n1/device", nil
					}
					return "", errors.New("eval error")
				}
			},
		},
		{
			name:        "no devices in sys/block",
			bdf:         "0000:02:00.0",
			expectedErr: "device not found for BDF 0000:02:00.0",
			setup: func() {
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					return []os.DirEntry{}, nil
				}
			},
		},
		{
			name:        "only virtual devices no match",
			bdf:         "0000:02:00.0",
			expectedErr: "device not found for BDF 0000:02:00.0",
			setup: func() {
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					return []os.DirEntry{mockDirEntry("loop0"), mockDirEntry("nvme0n1")}, nil
				}
				readlink = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "loop0" {
						return "", errors.New("no such file or directory")
					}
					if dev == "nvme0n1" {
						return "../../../virtual/nvme-subsystem/nvme-subsys0", nil
					}
					return "", errors.New("no link")
				}
				evalSymlinks = func(path string) (string, error) {
					dev := filepath.Base(filepath.Dir(path))
					if dev == "nvme0n1" {
						return "/sys/devices/virtual/nvme-subsystem/nvme-subsys0/nvme0n1/device", nil
					}
					return "", errors.New("eval error")
				}
			},
		},
		{
			name:        "devices but no matching BDF",
			bdf:         "ffff:ff:ff.f",
			expectedErr: "device not found for BDF ffff:ff:ff.f",
			setup: func() {
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					return []os.DirEntry{mockDirEntry("nvme0c0n1")}, nil
				}
				readlink = func(path string) (string, error) {
					return "../../../nvme/nvme0", nil
				}
				evalSymlinks = func(path string) (string, error) {
					return "/sys/devices/pci0000:00/0000:00:08.0/0000:02:00.0/nvme/nvme0/nvme0c0n1/device", nil
				}
			},
		},
		{
			name:        "ReadDir error",
			bdf:         "0000:02:00.0",
			expectedErr: "failed to read /sys/block",
			setup: func() {
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					return nil, errors.New("permission denied")
				}
			},
		},
		{
			name:        "empty BDF",
			bdf:         "",
			expectedErr: "device not found for BDF ",
			setup: func() {
				readDir = func(path string) ([]os.DirEntry, error) {
					c.Assert(path, Equals, sysBlockConst)
					return []os.DirEntry{}, nil
				}
			},
		},
	}

	for _, tc := range testCases {
		c.Logf("testing GetDevNameFromBDF: %s", tc.name)
		// Reset mocks
		resetMocks()
		if tc.setup != nil {
			tc.setup()
		}

		dev, err := fakeGetDevNameFromBDF(tc.bdf)

		if tc.expectedErr != "" {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, ".*"+tc.expectedErr+".*")
			c.Assert(dev, Equals, "")
		} else {
			c.Assert(err, IsNil)
			c.Assert(dev, Equals, tc.expectedDev)
		}

		// Reset after test
		resetMocks()
	}
}

// Helper to reset mocks to original functions
func resetMocks() {
	readDir = os.ReadDir
	readlink = os.Readlink
	evalSymlinks = filepath.EvalSymlinks
}

// Mock DirEntry for simplicity
type mockDirEntry string

func (m mockDirEntry) Name() string               { return string(m) }
func (m mockDirEntry) IsDir() bool                { return false }
func (m mockDirEntry) Type() os.FileMode          { return 0 }
func (m mockDirEntry) Info() (os.FileInfo, error) { return nil, nil }
func (m mockDirEntry) Sys() any                   { return nil }
