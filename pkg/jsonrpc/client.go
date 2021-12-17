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

		idCounter: uint32(0),
	}

}

func (c *Client) Init() chan error {
	errChan := make(chan error, 5)
	go c.startServer(errChan)
	return errChan
}

func (c *Client) SendMsg(method string, params interface{}) {
	id := atomic.AddUint32(&c.idCounter, 1)

	msg := NewMsg(method, id, params)

	//fmt.Printf("%v", msg)
	err := c.encoder.Encode(msg)

	if err != nil {
		fmt.Printf("error encoding: %v", err)
	}

	c.encoder2.Encode(msg)
	//c.writer.WriteByte('\n')
}

func (c *Client) startServer(errChan chan error) {
	//decoder := json.NewDecoder(c.reader)
	decoder := json.NewDecoder(c.conn)

	for {
		//var obj map[string]interface{}
		var obj interface{}

		err := decoder.Decode(&obj)

		if err != nil {
			fmt.Printf("error decoding: %v", err)
			errChan <- err
		}

		c.encoder2.Encode(obj)
	}

	errChan <- nil
}
