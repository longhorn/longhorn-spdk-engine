package spdk

type LonghornVolumeSnapshot struct {
	Name         string `json:"name"`
	SnapshotName string `json:"snapshot_name"`
}

func (l *LonghornVolumeSnapshot) GetMethod() string {
	return "longhorn_volume_snapshot"
}

func NewLonghornVolumeSnapshot(name string, snapshot string) *LonghornVolumeSnapshot {
	return &LonghornVolumeSnapshot{
		Name:         name,
		SnapshotName: snapshot,
	}
}
