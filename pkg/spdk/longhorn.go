package spdk

import "strconv"

type LonghornSetExternalAddress struct {
	Address string `json:"addr"`
}

func (lsea *LonghornSetExternalAddress) GetMethod() string {
	return "longhorn_set_external_address"
}

func NewLonghornSetExternalAddress(addr string) *LonghornSetExternalAddress {
	return &LonghornSetExternalAddress{
		Address: addr,
	}
}

type TcpJsonServer struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"`
}

func (t *TcpJsonServer) GetMethod() string {
	return "tcp_json_server"
}

func NewTcpJsonServer(addr string, port uint16) *TcpJsonServer {
	return &TcpJsonServer{
		Address: addr,
		Port:    port,
	}
}

type BdevNvmeAttachController struct {
	Name    string `json:"name"`
	TrType  string `json:"trtype"`
	TrAddr  string `json:"traddr"`
	AdrFam  string `json:"adrfam"`
	TrSvcId string `json:"trsvcid"`
	SubNqn  string `json:"subnqn"`
}

func (b *BdevNvmeAttachController) GetMethod() string {
	return "bdev_nvme_attach_controller"
}

func NewBdevNvmeAttachController(name string, addr string, port uint16, nqn string) *BdevNvmeAttachController {
	return &BdevNvmeAttachController{
		Name:    name,
		TrType:  "tcp",
		TrAddr:  addr,
		AdrFam:  "ipv4",
		TrSvcId: strconv.FormatUint(uint64(port), 10),
		SubNqn:  nqn,
	}
}

type BdevLvolCreateLvstore struct {
	BdevName string `json:"bdev_name"`
	LvsName  string `json:"lvs_name"`
}

func (b *BdevLvolCreateLvstore) GetMethod() string {
	return "bdev_lvol_create_lvstore"
}

func NewBdevLvolCreateLvstore(bdevName string, lvsName string) *BdevLvolCreateLvstore {
	return &BdevLvolCreateLvstore{
		BdevName: bdevName,
		LvsName:  lvsName,
	}
}

type LonghornBdevCompare struct {
	BdevName1 string `json:"bdev1"`
	BdevName2 string `json:"bdev2"`
}

func (b *LonghornBdevCompare) GetMethod() string {
	return "longhorn_bdev_compare"
}

func NewLonghornBdevCompare(bdevName1 string, bdevName2 string) *LonghornBdevCompare {
	return &LonghornBdevCompare{
		BdevName1: bdevName1,
		BdevName2: bdevName2,
	}
}
