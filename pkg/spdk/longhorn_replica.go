package spdk

type LonghornCreateReplica struct {
	Name    string `json:"name"`
	Size    uint64 `json:"size"`
	Lvs     string `json:"lvs"`
	Address string `json:"addr"`
	Port    uint16 `json:"port"`
}

func (lcr *LonghornCreateReplica) GetMethod() string {
	return "longhorn_replica_create"
}

func NewLonghornCreateReplica(name string, size uint64, lvs string, address string, port uint16) *LonghornCreateReplica {
	return &LonghornCreateReplica{
		Name:    name,
		Size:    size,
		Lvs:     lvs,
		Address: address,
		Port:    port,
	}
}
