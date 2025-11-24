package spdk

import (
	"reflect"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBdevNameGeneration(c *C) {
	tests := []struct {
		input    string
		expected string
	}{
		{"0000:00:1e.0", "0000-00-1e-0"},
		{"0000:5a:04.7", "0000-5a-04-7"},

		{"nvme0n1", "nvme0n1"},
		{"nvme1c2n1", "nvme1c2n1"},
		{"nvme-subsys1", "nvme-subsys1"},

		{"/dev/loop14", "dev-loop14"},
		{"/dev/sda", "dev-sda"},
		{"/dev/nvme0n1p1", "dev-nvme0n1p1"},

		{"/dev/mapper/vg0-lv_home", "dev-mapper-vg0-lv_home"},
		{"/dev/dm-12", "dev-dm-12"},

		{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1", "dev-disk-by-path-pci-0000-00-1e-0-nvme-1"},
		{"/dev/disk/by-id/ata-Samsung_SSD_850", "dev-disk-by-id-ata-Samsung_SSD_850"},

		{"weird#name?with$bad@chars", "weird-name-with-bad-chars"},
		{"--abc..def//ghi__", "abc-def-ghi__"},
		{"name with space", "name-with-space"},

		{"/complex/path/0000:00:1f.0/nvme#n1", "complex-path-0000-00-1f-0-nvme-n1"},
	}

	for _, tt := range tests {
		out := sanitizeBdevComponent(tt.input)
		c.Check(out, Equals, tt.expected,
			Commentf("input=%q", tt.input))
	}
}

func (s *TestSuite) TestMakeLvolBdevName(c *C) {
	tests := []struct {
		diskName string
		diskPath string
		expected string
	}{
		{"disk-1", "0000:00:1e.0", "disk-1-0000-00-1e-0"},
		{"disk-2", "/dev/loop14", "disk-2-dev-loop14"},
		{"disk-x", "/dev/disk/by-path/pci-0000:00:1e.0", "disk-x-dev-disk-by-path-pci-0000-00-1e-0"},
		{"mydisk", "nvme0n1", "mydisk-nvme0n1"},
	}

	for _, tt := range tests {
		out := makeBaseBdevNameFromPath(tt.diskName, tt.diskPath)
		c.Check(out, Equals, tt.expected,
			Commentf("diskName=%q diskPath=%q", tt.diskName, tt.diskPath))
	}
}

func (s *TestSuite) TestSplitPaths(c *C) {
	tests := []struct {
		input string
		want  []string
	}{
		{"a", []string{"a"}},
		{" a ", []string{"a"}},

		{"b;a;c", []string{"a", "b", "c"}},
		{" b ;   a ;  c  ", []string{"a", "b", "c"}},

		{"a;;b", []string{"a", "b"}},
		{"a; ;b", []string{"a", "b"}},

		{";;  ;", nil},

		{"0000:00:1e.0", []string{"0000:00:1e.0"}},
		{"0000:5a:04.7", []string{"0000:5a:04.7"}},

		{"nvme0n1", []string{"nvme0n1"}},
		{"nvme1c2n1", []string{"nvme1c2n1"}},
		{"nvme-subsys1", []string{"nvme-subsys1"}},

		{"/dev/loop14", []string{"/dev/loop14"}},
		{"/dev/sda", []string{"/dev/sda"}},
		{"/dev/nvme0n1p1", []string{"/dev/nvme0n1p1"}},

		{"/dev/mapper/vg0-lv_home", []string{"/dev/mapper/vg0-lv_home"}},
		{"/dev/dm-12", []string{"/dev/dm-12"}},

		{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1",
			[]string{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1"}},
		{"/dev/disk/by-id/ata-Samsung_SSD_850",
			[]string{"/dev/disk/by-id/ata-Samsung_SSD_850"}},

		{"weird#name?with$bad@chars", []string{"weird#name?with$bad@chars"}},
		{"--abc..def//ghi__", []string{"--abc..def//ghi__"}},

		{"name with space", []string{"name with space"}},

		{"/complex/path/0000:00:1f.0/nvme#n1",
			[]string{"/complex/path/0000:00:1f.0/nvme#n1"}},

		{
			"0000:00:1e.0;   0000:00:1f.0",
			[]string{"0000:00:1e.0", "0000:00:1f.0"},
		},
		{
			"nvme0n1; nvme0n1; /dev/nvme0n1p1; /dev/nvme0n1p1",
			[]string{"/dev/nvme0n1p1", "nvme0n1"},
		},
	}

	for _, tt := range tests {
		out := splitPaths(tt.input)
		if !reflect.DeepEqual(out, tt.want) {
			c.Errorf("SplitPaths(%q) = %#v, want %#v", tt.input, out, tt.want)
		}
	}
}
