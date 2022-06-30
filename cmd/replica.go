package cmd

import (
	"github.com/docker/go-units"
	"github.com/longhorn/longhorn-spdk/pkg/spdk"
	"github.com/spf13/cobra"
)

var (
	address string
	port    uint16
)

// replicaCmd represents the replica command
var replicaCmd = &cobra.Command{
	Use:   "replica",
	Short: "Manipulate longhorn replicas",
	Long:  "Manipulate longhorn replicas",
}

var replicaCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a replica",
	Long:  "Create a replica",

	Run: func(cmd *cobra.Command, args []string) {
		client, _ := CreateClient()

		size, err := units.RAMInBytes(args[1])

		if err != nil {

		}

		command := spdk.NewLonghornCreateReplica(args[0], uint64(size), LvsName, address, port)

		client.SendCommand(command)

	},
}

func init() {
	rootCmd.AddCommand(replicaCmd)
	replicaCmd.AddCommand(replicaCreateCmd)

	replicaCreateCmd.Flags().StringVarP(&address, "address", "a", "", "NVMe-oF address")
	replicaCreateCmd.Flags().Uint16VarP(&port, "port", "p", 4420, "NVMe-oF port")

}
