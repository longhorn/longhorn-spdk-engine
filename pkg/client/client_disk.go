package client

import (
	"context"
	"fmt"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// DiskCreate creates a disk with the given name and path.
// diskUUID is optional, if not provided, it indicates the disk is newly added.
func (c *SPDKClient) DiskCreate(diskName, diskUUID, diskPath, diskDriver string, blockSize int64) (*spdkrpc.Disk, error) {
	if diskName == "" || diskPath == "" {
		return nil, fmt.Errorf("failed to create disk: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskCreate(ctx, &spdkrpc.DiskCreateRequest{
		DiskName:   diskName,
		DiskUuid:   diskUUID,
		DiskPath:   diskPath,
		BlockSize:  blockSize,
		DiskDriver: diskDriver,
	})
}

func (c *SPDKClient) DiskGet(diskName, diskPath, diskDriver string) (*spdkrpc.Disk, error) {
	if diskName == "" {
		return nil, fmt.Errorf("failed to get disk info: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskGet(ctx, &spdkrpc.DiskGetRequest{
		DiskName:   diskName,
		DiskPath:   diskPath,
		DiskDriver: diskDriver,
	})
}

func (c *SPDKClient) DiskDelete(diskName, diskUUID, diskPath, diskDriver string) error {
	if diskName == "" {
		return fmt.Errorf("failed to delete disk: missing required parameter disk name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.DiskDelete(ctx, &spdkrpc.DiskDeleteRequest{
		DiskName:   diskName,
		DiskUuid:   diskUUID,
		DiskPath:   diskPath,
		DiskDriver: diskDriver,
	})
	return err
}

// DiskHealthGet retrieves the health info for a specified disk.
func (c *SPDKClient) DiskHealthGet(diskName, diskPath, diskDriver string) (*spdkrpc.DiskHealthGetResponse, error) {
	if diskName == "" {
		return nil, fmt.Errorf("failed to get disk health: missing required parameter 'disk name'")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	req := &spdkrpc.DiskHealthGetRequest{
		DiskName:   diskName,
		DiskDriver: diskDriver,
		DiskPath:   diskPath,
	}
	return client.DiskHealthGet(ctx, req)
}
