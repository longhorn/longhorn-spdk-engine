package main

import (
	"fmt"
	"net"

	"github.com/longhorn/longhorn-spdk/pkg/jsonrpc"
	"github.com/longhorn/longhorn-spdk/pkg/spdk"
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

	aio := spdk.NewAioCreate("aio", "/root/aio.img", 4096)
	client.SendCommand(aio)

	lvs := spdk.NewBdevLvolCreateLvstore("aio", "longhorn")

	client.SendCommand(lvs)

	r1 := spdk.NewLonghornCreateReplica("test", 4<<30, "longhorn", "", 0)
	client.SendCommand(r1)

	longhornCreate := spdk.NewLonghornVolumeCreateWithReplicas(
		"test",
		[]spdk.LonghornVolumeReplica{
			spdk.LonghornVolumeReplica{
				Lvs: "longhorn",
			},
			spdk.LonghornVolumeReplica{
				Address:  "127.0.0.1",
				NvmfPort: 4430,
				CommPort: 4431,
				Lvs:      "longhorn",
			},
		})

	client.SendCommand(longhornCreate)
	<-errChan
}
