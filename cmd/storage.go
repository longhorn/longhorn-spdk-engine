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
	"github.com/longhorn/longhorn-spdk/pkg/spdk"
	"github.com/spf13/cobra"
)

var blockSize int

var storageCmd = &cobra.Command{
	Use:   "storage",
	Short: "Configure storage for longhorn",
	Long:  "Configure storage for longhorn",
}

var storageCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Add storage to longhorn",
	Long:  "Add storage to longhorn",

	Run: func(cmd *cobra.Command, args []string) {
		client, _ := CreateClient()

		aioCmd := spdk.NewAioCreate(args[0], args[0], uint64(blockSize))

		client.SendCommand(aioCmd)

		lvsCmd := spdk.NewBdevLvolCreateLvstore(args[0], LvsName)

		client.SendCommand(lvsCmd)

	},
}

func init() {
	rootCmd.AddCommand(storageCmd)
	storageCmd.AddCommand(storageCreateCmd)

	storageCreateCmd.Flags().IntVarP(&blockSize, "blocksize", "b", 4096, "blocksize")
}
