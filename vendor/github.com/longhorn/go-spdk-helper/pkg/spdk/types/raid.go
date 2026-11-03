package types

type BdevRaidLevel string

const (
	BdevRaidLevel0      = BdevRaidLevel("0")
	BdevRaidLevelRaid0  = BdevRaidLevel("raid0")
	BdevRaidLevel1      = BdevRaidLevel("1")
	BdevRaidLevelRaid1  = BdevRaidLevel("raid1")
	BdevRaidLevel5f     = BdevRaidLevel("5f")
	BdevRaidLevelRaid5f = BdevRaidLevel("raid5f")
	BdevRaidLevelConcat = BdevRaidLevel("concat")
)

type BdevRaidInfo struct {
	Name                    string        `json:"name,omitempty"`
	StripSizeKb             uint32        `json:"strip_size_kb"`
	State                   string        `json:"state"`
	RaidLevel               BdevRaidLevel `json:"raid_level"`
	NumBaseBdevs            uint8         `json:"num_base_bdevs"`
	NumBaseBdevsDiscovered  uint8         `json:"num_base_bdevs_discovered"`
	NumBaseBdevsOperational uint8         `json:"num_base_bdevs_operational,omitempty"`
	BaseBdevsList           []BaseBdev    `json:"base_bdevs_list"`
	Superblock              bool          `json:"superblock"`
}

type BaseBdev struct {
	Name         string `json:"name"`
	UUID         string `json:"uuid"`
	IsConfigured bool   `json:"is_configured"`
	DataOffset   uint64 `json:"data_offset"`
	DataSize     uint64 `json:"data_size"`
}

type BdevRaidCreateRequest struct {
	Name        string        `json:"name"`
	RaidLevel   BdevRaidLevel `json:"raid_level"`
	StripSizeKb uint32        `json:"strip_size_kb"`
	BaseBdevs   []string      `json:"base_bdevs"`
	UUID        string        `json:"uuid,omitempty"`
}

type BdevRaidDeleteRequest struct {
	Name string `json:"name"`
}

type BdevRaidCategory string

const (
	BdevRaidCategoryAll         = "all"
	BdevRaidCategoryOnline      = "online"
	BdevRaidCategoryOffline     = "offline"
	BdevRaidCategoryConfiguring = "configuring"
)

type BdevRaidGetBdevsRequest struct {
	Category BdevRaidCategory `json:"category"`
}

type BdevRaidRemoveBaseBdevRequest struct {
	Name string `json:"name"`
}

type BdevRaidGrowBaseBdevRequest struct {
	RaidName string `json:"raid_name"`
	BaseName string `json:"base_name"`
}

// BdevRaidResizeRequest is the request for bdev_raid_resize.
type BdevRaidResizeRequest struct {
	Name              string `json:"name"`
	ExpectedSizeInMib uint64 `json:"expected_size_in_mib,omitempty"`
}

// BdevRaidResizeResponse is the response for bdev_raid_resize.
type BdevRaidResizeResponse struct {
	Name        string `json:"name"`
	OldBlockcnt uint64 `json:"old_blockcnt"`
	NewBlockcnt uint64 `json:"new_blockcnt"`
	Resized     bool   `json:"resized"`
}

// BdevRaidSetRaidOptionsRequest is the request for bdev_raid_set_raid_options. A nil option
// is not sent and stays unchanged.
type BdevRaidSetRaidOptionsRequest struct {
	Name           string `json:"name"`
	ExplicitResize *bool  `json:"explicit_resize,omitempty"`
}
