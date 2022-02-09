package main

import (
	"fmt"
	"net"
	"time"

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

	longhornVolumeSnapshot := spdk.NewLonghornVolumeSnapshot(
		"test", "snapshot_"+time.Now().String())

	client.SendCommand(longhornVolumeSnapshot)

	<-errChan
}
