package main

import (
	"fmt"
	"net"

	"github.com/keithalucas/jsonrpc/pkg/jsonrpc"
	"github.com/keithalucas/jsonrpc/pkg/spdk"
)

func main() {

	conn, err := net.Dial("unix", "/var/tmp/spdk.sock")

	if err != nil {
		fmt.Printf("Error opening socket: %v", err)
		return
	}

	client := jsonrpc.NewClient(conn)

	errChan := client.Init()

	ext := spdk.NewLonghornSetExternalAddress("127.0.0.1")
	client.SendMsg(ext.GetMethod(), ext)

	aio1 := spdk.NewAioCreate("aio1", "/root/aio1.img", 4096)
	aio2 := spdk.NewAioCreate("aio2", "/root/aio2.img", 4096)
	aio3 := spdk.NewAioCreate("aio3", "/root/aio3.img", 4096)

	client.SendCommand(aio1)
	client.SendCommand(aio2)
	client.SendCommand(aio3)

	lvs1 := spdk.NewBdevLvolCreateLvstore("aio1", "longhorn1")
	lvs2 := spdk.NewBdevLvolCreateLvstore("aio2", "longhorn2")
	lvs3 := spdk.NewBdevLvolCreateLvstore("aio3", "longhorn3")

	client.SendCommand(lvs1)
	client.SendCommand(lvs2)
	client.SendCommand(lvs3)

	r1 := spdk.NewLonghornCreateReplica("test", 1<<30, "longhorn1", "", 0)
	r2 := spdk.NewLonghornCreateReplica("test", 1<<30, "longhorn2", "", 0)
	r3 := spdk.NewLonghornCreateReplica("test", 1<<30, "longhorn3", "", 0)

	client.SendCommand(r1)
	client.SendCommand(r2)
	client.SendCommand(r3)

	//lrc := spdk.NewLonghornCreateReplica("replica", 1024*1024*1024, "lvs1", "127.0.0.1", 4420)
	//client.SendMsg(lrc.GetMethod(), lrc)

	longhornCreate := spdk.NewLonghornVolumeCreateWithReplicas(
		"test",
		[]spdk.LonghornVolumeReplica{
			spdk.LonghornVolumeReplica{
				Lvs: "longhorn1",
			},
			spdk.LonghornVolumeReplica{
				Lvs: "longhorn2",
			},
			spdk.LonghornVolumeReplica{
				Lvs: "longhorn3",
			},
		})

	//time.Sleep(time.Second)
	client.SendCommand(longhornCreate)
	//time.Sleep(time.Second)
	client.SendMsg("bdev_get_bdevs", nil)

	<-errChan
}
