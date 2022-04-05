/*
Copyright © 2022 Keith Lucas keith.lucas@suse.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/docker/go-units"
	"github.com/keithalucas/longhorn-spdk/pkg/spdk"
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
