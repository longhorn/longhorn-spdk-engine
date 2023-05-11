package spdk

import "fmt"

const (
	DiskTypeFilesystem = "filesystem"
	DiskTypeBlock      = "block"
)

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s-snaps-%s", replicaName, snapshotName)
}
