package spdk

type LonghornVolumeCreateReplica struct {
	Address string `json:"addr"`
	Port    uint16 `json:"port"`
}

type LonghornVolumeCreate struct {
	Name     string                        `json:"name"`
	Replicas []LonghornVolumeCreateReplica `json:"replicas"`
}
