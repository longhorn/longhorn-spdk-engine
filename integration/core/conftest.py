import pytest


from rpc.imrpc.process_manager_client import ProcessManagerClient
from rpc.instance.instance_client import InstanceClient
from rpc.spdk.spdk_client import SPDKClient
from rpc.disk.disk_client import DiskClient

from common.constants import (
    DISK_TYPE,
    DISK_NAME,
    DISK_PATH,
    DISK_BLOCK_SIZE,
    INSTANCE_MANAGER_PROCESS_MANAGER,
    INSTANCE_MANAGER_INSTANCE,
    INSTANCE_MANAGER_SPDK,
    INSTANCE_MANAGER_DISK,
)

def delete_disk():
    try:
        disk_client = DiskClient(INSTANCE_MANAGER_DISK)
        disk_client.disk_delete(DISK_TYPE, DISK_NAME, DISK_PATH)
    except Exception as e:
        raise e


@pytest.fixture
def grpc_instance_client(request, address=INSTANCE_MANAGER_INSTANCE):
    try:
        disk_client = DiskClient(INSTANCE_MANAGER_DISK)
        disk_client.disk_create(DISK_TYPE, DISK_NAME, DISK_PATH,
                                DISK_BLOCK_SIZE)
    except Exception as e:
        raise e

    request.addfinalizer(lambda: delete_disk())
    return InstanceClient(address)


@pytest.fixture
def grpc_process_manager_client(request, address=INSTANCE_MANAGER_PROCESS_MANAGER):
    c = ProcessManagerClient(address)
    return c


@pytest.fixture
def grpc_disk_client(request, address=INSTANCE_MANAGER_DISK):
    c = DiskClient(address)
    return c


@pytest.fixture
def grpc_spdk_client(request, address=INSTANCE_MANAGER_SPDK):
    c = SPDKClient(address)
    return c
