package spdk

import (
	"testing"
)

func TestBdevNameGeneration(t *testing.T) {
	t.Run("sanitizeBdevComponent", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			// PCI BDF formats
			{"0000:00:1e.0", "0000-00-1e-0"},
			{"0000:5a:04.7", "0000-5a-04-7"},

			// NVMe controller paths
			{"nvme0n1", "nvme0n1"},
			{"nvme1c2n1", "nvme1c2n1"},
			{"nvme-subsys1", "nvme-subsys1"},

			// /dev paths
			{"/dev/loop14", "dev-loop14"},
			{"/dev/sda", "dev-sda"},
			{"/dev/nvme0n1p1", "dev-nvme0n1p1"},

			// Device mapper
			{"/dev/mapper/vg0-lv_home", "dev-mapper-vg0-lv_home"},
			{"/dev/dm-12", "dev-dm-12"},

			// by-path / by-id
			{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1", "dev-disk-by-path-pci-0000-00-1e-0-nvme-1"},
			{"/dev/disk/by-id/ata-Samsung_SSD_850", "dev-disk-by-id-ata-Samsung_SSD_850"},

			// Weird characters
			{"weird#name?with$bad@chars", "weird-name-with-bad-chars"},

			// Multiple invalid chars
			{"--abc..def//ghi__", "abc-def-ghi__"},

			// Spaces
			{"name with space", "name-with-space"},

			// Complex mixed case
			{"/complex/path/0000:00:1f.0/nvme#n1",
				"complex-path-0000-00-1f-0-nvme-n1"},
		}

		for _, tt := range tests {
			out := sanitizeBdevComponent(tt.input)
			if out != tt.expected {
				t.Errorf("sanitizeBdevComponent(%q) = %q, expected %q",
					tt.input, out, tt.expected)
			}
		}
	})

	t.Run("makeBaseBdevNameFromPath", func(t *testing.T) {
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
			if out != tt.expected {
				t.Errorf("makeBaseBdevNameFromPath(%q, %q) = %q, expected %q",
					tt.diskName, tt.diskPath, out, tt.expected)
			}
		}
	})
}
