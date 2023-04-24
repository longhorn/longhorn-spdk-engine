package jsonrpc

import (
	"fmt"
)

type Message struct {
	ID      uint32      `json:"id"`
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

func NewMessage(id uint32, method string, params interface{}) *Message {
	return &Message{
		ID:      id,
		Version: "2.0",
		Method:  method,
		Params:  params,
	}
}

type RespErrorMsg string

const (
	RespErrorMsgNoSuchDevice = "No such device"
)

type RespErrorCode int32

const (
	RespErrorCodeNoSuchDevice = -19
)

type Response struct {
	ID        uint32         `json:"id"`
	Version   string         `json:"jsonrpc"`
	Result    interface{}    `json:"result,omitempty"`
	ErrorInfo *ResponseError `json:"error,omitempty"`
}

type ResponseError struct {
	Code    RespErrorCode `json:"code"`
	Message RespErrorMsg  `json:"message"`
}

func (re ResponseError) Error() string {
	return fmt.Sprintf("{\n\t\"code\": %d,\n\t\"message\": \"%s\"\n}", re.Code, re.Message)
}

func IsJSONRPCRespErrorNoSuchDevice(err error) bool {
	jsonRPCError, ok := err.(ResponseError)
	if !ok {
		return false
	}
	return jsonRPCError.Code == RespErrorCodeNoSuchDevice && jsonRPCError.Message == RespErrorMsgNoSuchDevice
}
