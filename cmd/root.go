/*
Copyright © 2022 Keith Lucas <keith.lucas@suse.com>

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
	"net"
	"os"

	"github.com/longhorn/longhorn-spdk/pkg/jsonrpc"
	"github.com/spf13/cobra"
)

var (
	SocketPath string
	LvsName    string
	Verbose    bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "longhorn-spdk",
	Short: "A CLI for longhorn with SPDK",
	Long:  "A CLI for longhorn with SPDK",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func CreateClient() (*jsonrpc.Client, chan<- error) {
	conn, err := net.Dial("unix", "/var/tmp/spdk.sock")

	if err != nil {
		return nil, nil
	}

	client := jsonrpc.NewClient(conn)

	errChan := client.Init()

	return client, errChan
}

func init() {

	rootCmd.PersistentFlags().StringVarP(&SocketPath, "socket", "s", "/var/tmp/sock.sock", "socket for SPDK communication")
	rootCmd.PersistentFlags().StringVarP(&LvsName, "lvs", "l", "longhorn", "Logical Volume Store name")
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "verbose")

}
