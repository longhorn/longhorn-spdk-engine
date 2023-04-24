package spdk

import "fmt"

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s-snaps-%s", replicaName, snapshotName)
}
