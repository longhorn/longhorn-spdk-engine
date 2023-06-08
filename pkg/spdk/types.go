package spdk

import (
	"fmt"
	"strings"
)

const (
	DiskTypeFilesystem = "filesystem"
	DiskTypeBlock      = "block"
)

func GetReplicaSnapshotLvolNamePrefix(replicaName string) string {
	return fmt.Sprintf("%s-snap-", replicaName)
}

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s%s", GetReplicaSnapshotLvolNamePrefix(replicaName), snapshotName)
}

func GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, snapLvolName string) string {
	return strings.TrimPrefix(snapLvolName, GetReplicaSnapshotLvolNamePrefix(replicaName))
}

func GetNvmfEndpoint(nqn, ip string, port int32) string {
	return fmt.Sprintf("%s://%s:%d", nqn, ip, port)
}
