package jsonrpc

type Command interface {
	GetMethod() string
}
