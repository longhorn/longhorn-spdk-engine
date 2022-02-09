package main

import (
	"fmt"
	"net"

	"github.com/keithalucas/jsonrpc/pkg/jsonrpc"
	"github.com/keithalucas/jsonrpc/pkg/spdk"
)

func main() {

	conn, err := net.Dial("unix", "/var/tmp/spdk2.sock")

	if err != nil {
		fmt.Printf("Error opening socket: %v", err)
		return
	}

	client := jsonrpc.NewClient(conn)

	client.Init()
	ext := spdk.NewLonghornSetExternalAddress("127.0.0.1")
	client.SendMsg(ext.GetMethod(), ext)

	jsonServer := spdk.NewTcpJsonServer("127.0.0.1", 4431)
	client.SendCommand(jsonServer)

	aio := spdk.NewAioCreate("remote", "/root/remote.img", 4096)
	client.SendCommand(aio)

	lvs := spdk.NewBdevLvolCreateLvstore("remote", "longhorn")

	client.SendCommand(lvs)

	r1 := spdk.NewLonghornCreateReplica("test", 4<<30, "longhorn", "127.0.0.1", 4430)
	client.SendCommand(r1)

}
