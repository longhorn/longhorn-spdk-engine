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

	longhornRemoveReplica := spdk.NewLonghornVolumeRemoveReplica(
		"test", spdk.LonghornVolumeReplica{Lvs: "longhorn3"})

	client.SendCommand(longhornRemoveReplica)

	longhornAddReplica := spdk.NewLonghornVolumeAddReplica(
		"test", spdk.LonghornVolumeReplica{Lvs: "longhorn3"})

	client.SendCommand(longhornAddReplica)

	<-errChan
}
