package jsonrpc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync/atomic"
)

type Client struct {
	conn      net.Conn
	writer    *bufio.Writer
	writer2   *bufio.Writer
	reader    *bufio.Reader
	encoder   *json.Encoder
	encoder2  *json.Encoder
	idCounter uint32

	msgs          chan *msgWrapper
	responseChans map[uint32]chan interface{}
	resps         chan map[string]interface{}
}

type msgWrapper struct {
	method   string
	params   interface{}
	response chan interface{}
}

func NewClient(conn net.Conn) *Client {
	writer := bufio.NewWriter(conn)
	writer2 := bufio.NewWriter(os.Stdin)

	return &Client{
		conn: conn,

		writer:  writer,
		writer2: writer2,
		reader:  bufio.NewReader(conn),

		encoder:  json.NewEncoder(conn),
		encoder2: json.NewEncoder(os.Stdin),

		msgs:          make(chan *msgWrapper, 1024),
		responseChans: make(map[uint32](chan interface{})),
		resps:         make(chan map[string]interface{}, 1024),
		idCounter:     uint32(0),
	}

}

func (c *Client) Init() chan error {
	errChan := make(chan error, 5)
	go c.dispatcher()
	go c.read(errChan)
	return errChan
}

func (c *Client) SendMsgAsync(method string, params interface{}, responseChan chan interface{}) {

	msg := &msgWrapper{method: method,
		params:   params,
		response: responseChan}

	c.msgs <- msg
}

func (c *Client) SendMsg(method string, params interface{}) interface{} {
	responseChan := make(chan interface{}, 5)

	msg := &msgWrapper{method: method,
		params:   params,
		response: responseChan}

	fmt.Printf("sending msg %v\n", method)
	c.msgs <- msg

	return <-responseChan
}

func (c *Client) handleSend(msg *msgWrapper) {
	id := atomic.AddUint32(&c.idCounter, 1)

	m := NewMsg(msg.method, id, msg.params)

	c.responseChans[id] = msg.response

	fmt.Printf("sending msg\n")
	err := c.encoder.Encode(m)

	if err != nil {
		fmt.Printf("error encoding: %v", err)
	}

	c.encoder2.Encode(m)
}

func (c *Client) handleRecv(obj map[string]interface{}) {
	fmt.Printf("received\n")
	fid, ok := obj["id"].(float64)

	if ok {
		id := uint32(fid)
		ch := c.responseChans[id]

		delete(c.responseChans, id)

		ch <- obj["result"]
	} else {
		fmt.Printf("error decoding: %T", obj["id"])
	}

}

func (c *Client) dispatcher() {
	for {
		fmt.Printf("dispatching\n")
		select {
		case msg := <-c.msgs:
			fmt.Printf("dispatching msg\n")
			c.handleSend(msg)
		case resp := <-c.resps:
			fmt.Printf("dispatching resp\n")
			c.handleRecv(resp)

		}
	}
}

func (c *Client) read(errChan chan error) {
	//decoder := json.NewDecoder(c.reader)
	decoder := json.NewDecoder(c.conn)

	for {
		var obj map[string]interface{}
		//var obj interface{}

		err := decoder.Decode(&obj)

		if err != nil {
			fmt.Printf("error decoding: %v", err)
			errChan <- err
		}

		fmt.Printf("received response\n")

		c.resps <- obj

		fmt.Printf("enqueue response\n")

		c.encoder2.Encode(obj)
	}

	errChan <- nil
}
