package jsonrpc

type Msg struct {
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	Id      uint32      `json:"id"`
}

func NewMsg(method string, id uint32, params interface{}) *Msg {
	return &Msg{
		Version: "2.0",
		Method:  method,
		Params:  params,
		Id:      id,
	}
}
