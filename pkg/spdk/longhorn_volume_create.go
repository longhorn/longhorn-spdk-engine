package spdk

type LonghornVolumeReplica struct {
	Address  string `json:"addr"`
	Lvs      string `json:"lvs"`
	NvmfPort uint16 `json:"nvmf_port"`
	CommPort uint16 `json:"comm_port"`
}

type LonghornVolumeCreate struct {
	Name     string                  `json:"name"`
	Replicas []LonghornVolumeReplica `json:"replicas"`
}

type LonghornVolumeRemoveReplica struct {
	Name    string                `json:"name"`
	Replica LonghornVolumeReplica `json:"replica"`
}

type LonghornVolumeAddReplica struct {
	Name    string                `json:"name"`
	Replica LonghornVolumeReplica `json:"replica"`
}

func NewLonghornVolumeCreate(name string) *LonghornVolumeCreate {
	return &LonghornVolumeCreate{Name: name}
}

func NewLonghornVolumeCreateWithReplicas(name string, replicas []LonghornVolumeReplica) *LonghornVolumeCreate {
	return &LonghornVolumeCreate{
		Name:     name,
		Replicas: replicas,
	}
}

func (c *LonghornVolumeCreate) GetMethod() string {
	return "longhorn_volume_create"
}

func NewLonghornVolumeRemoveReplica(name string, replica LonghornVolumeReplica) *LonghornVolumeRemoveReplica {
	return &LonghornVolumeRemoveReplica{
		Name:    name,
		Replica: replica,
	}
}

func (c *LonghornVolumeRemoveReplica) GetMethod() string {
	return "longhorn_volume_remove_replica"
}

func NewLonghornVolumeAddReplica(name string, replica LonghornVolumeReplica) *LonghornVolumeAddReplica {
	return &LonghornVolumeAddReplica{
		Name:    name,
		Replica: replica,
	}
}

func (c *LonghornVolumeAddReplica) GetMethod() string {
	return "longhorn_volume_add_replica"
}
