import os
import grpc
import pytest

from rpc.disk.disk_client import DiskClient


def test_disk_create_and_delete(grpc_disk_client):
    disk_type = "block"
    disk_name = "test-disk"
    disk_path = "/dev/im-disk"
    block_size = 4096

    disk = grpc_disk_client.disk_create(disk_type, disk_name, disk_path,
                                        block_size)
    assert disk.type == disk_type
    assert disk_path == disk.path
    assert block_size == disk.block_size

    disk = grpc_disk_client.disk_get(disk_type, disk_name, disk_path)
    assert disk.type == disk_type
    assert disk_path == disk.path
    assert block_size == disk.block_size

    grpc_disk_client.disk_delete(disk_type, disk_name, disk.uuid)

    try:
        disk = grpc_disk_client.disk_get(disk_type, disk_name, disk_path)
    except Exception as e:
        if "NotFound" not in str(e):
            raise e
