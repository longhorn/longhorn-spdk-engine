/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"strconv"
	"strings"

	"github.com/longhorn/longhorn-spdk/pkg/spdk"
	"github.com/spf13/cobra"
)

var replicas []string

// volumeCmd represents the volume command
var volumeCmd = &cobra.Command{
	Use:   "volume",
	Short: "Create and manipulate longhorn volumes",
	Long:  "Create and manipulate longhorn volumes",
}

var volumeCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a longhorn volume",
	Long:  "Create a longhorn volume",
	Run: func(cmd *cobra.Command, args []string) {
		replicaList := []spdk.LonghornVolumeReplica{}

		for _, arg := range replicas {
			replicaList = append(replicaList, decodeReplica(arg))
		}

		longhornCreate := spdk.NewLonghornVolumeCreateWithReplicas(
			args[0], replicaList)

		client, _ := CreateClient()
		client.SendCommand(longhornCreate)
	},
}

var volumeAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a replica to an existing volume",
	Long:  "Add a replica to an existing volume",
	Run: func(cmd *cobra.Command, args []string) {

		replicaList := []spdk.LonghornVolumeReplica{}

		for _, arg := range replicas {
			replicaList = append(replicaList, decodeReplica(arg))
		}

		longhornCreate := spdk.NewLonghornVolumeCreateWithReplicas(
			args[0], replicaList)

		client, _ := CreateClient()
		client.SendCommand(longhornCreate)
	},
}

func decodeReplica(replicaEncoding string) spdk.LonghornVolumeReplica {
	fields := strings.Split(replicaEncoding, ":")

	var replica spdk.LonghornVolumeReplica

	replica.Lvs = LvsName

	if len(fields) > 0 && fields[0] != "" {
		replica.Lvs = fields[0]
	}

	if len(fields) > 1 {
		replica.Address = fields[1]
	}

	if len(fields) > 2 && replica.Address != "" && fields[2] != "" {
		port, err := strconv.Atoi(fields[2])
		if err != nil {
			replica.NvmfPort = uint16(port)
		}
	}

	if replica.Address != "" && replica.NvmfPort == 0 {
		replica.NvmfPort = uint16(4420)
	}

	if len(fields) > 3 && replica.Address != "" && fields[3] != "" {
		port, err := strconv.Atoi(fields[3])
		if err != nil {
			replica.CommPort = uint16(port)
		}
	}

	if replica.Address != "" && replica.NvmfPort != 0 && replica.CommPort == 0 {
		replica.CommPort = replica.NvmfPort + 1
	}

	return replica
}

func init() {
	rootCmd.AddCommand(volumeCmd)
	volumeCmd.AddCommand(volumeCreateCmd)
	volumeCmd.AddCommand(volumeAddCmd)

	volumeCreateCmd.Flags().StringSliceVarP(&replicas, "replica", "r", []string{}, "replicas")
}
