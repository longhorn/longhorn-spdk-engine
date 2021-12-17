package spdk

type AioCreate struct {
	Name      string `json:"name"`
	Filename  string `json:"filename"`
	Blocksize uint64 `json:"block_size"`
}

func (ac *AioCreate) GetMethod() string {
	return "bdev_aio_create"
}

func NewAioCreate(name string, filename string, blocksize uint64) *AioCreate {
	return &AioCreate{
		Name:      name,
		Filename:  filename,
		Blocksize: blocksize,
	}
}
