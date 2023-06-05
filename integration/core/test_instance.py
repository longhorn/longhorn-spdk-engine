import os
import grpc
import pytest
import socket

from rpc.instance.instance_client import InstanceClient
from rpc.disk.disk_client import DiskClient

from common.constants import (
    DISK_TYPE,
    DISK_NAME,
    DISK_PATH,
    INSTANCE_MANAGER_DISK,
)

from common.util import (
    get_pod_ip,
)


def test_version_get(grpc_process_manager_client, grpc_instance_client):
    """
    Test version_get() method of ProcessManagerClient and InstanceClient.
    The two versions should be the same.
    """
    pm_version = grpc_process_manager_client.version_get()
    instance_version = grpc_instance_client.version_get()
    assert pm_version == instance_version


def test_instance_list(grpc_instance_client):
    """
    Test instance_list() method of InstanceClient.
    """
    instances = grpc_instance_client.instance_list()
    assert len(instances) == 0


def test_replica_create(grpc_instance_client):
    """
    Test replica_create() method of InstanceClient.
    1. Create a disk and a corresponding lvstore.
    2. Create a running replica on the lvstore.
    3. Stop the replica (state: running->stopped)
    4. Resume the replica (state: stopped->running)
    5. Delete the replica
    """
    # Create a disk and a corresponding lvstore
    try:
        disk_client = DiskClient(INSTANCE_MANAGER_DISK)
        disk = disk_client.disk_get(DISK_TYPE, DISK_NAME, DISK_PATH)
        print(disk)
    except Exception as e:
        raise e

    replica_name = "replica-1"
    volume_name = "test-vol"
    # Create a running replica
    replica = grpc_instance_client.instance_create(backend_store_driver="spdk",
                                                   name=replica_name,
                                                   type="replica",
                                                   volume_name=volume_name,
                                                   disk_name=DISK_NAME,
                                                   disk_uuid=disk.uuid,
                                                   size=104857600,
                                                   expose_required=False)
    assert replica.status.state == "running"

    replica = grpc_instance_client.instance_get("spdk",
                                                replica_name, "replica")
    assert replica.status.state == "running"

    instances = grpc_instance_client.instance_list()
    assert len(instances) == 1
    # Stop the replica
    grpc_instance_client.instance_delete(backend_store_driver="spdk",
                                         name=replica_name,
                                         type="replica",
                                         disk_uuid=disk.uuid,
                                         cleanup_required=False)
    replica = grpc_instance_client.instance_get("spdk",
                                                replica_name, "replica")
    assert replica.status.state == "stopped"

    # Resume the replica
    replica = grpc_instance_client.instance_create(backend_store_driver="spdk",
                                                   name=replica_name,
                                                   type="replica",
                                                   volume_name=volume_name,
                                                   disk_name=DISK_NAME,
                                                   disk_uuid=disk.uuid,
                                                   size=104857600,
                                                   expose_required=False)
    assert replica.status.state == "running"

    # Delete the replica
    grpc_instance_client.instance_delete(backend_store_driver="spdk",
                                         name=replica_name,
                                         type="replica",
                                         disk_uuid=disk.uuid,
                                         cleanup_required=True)
    instances = grpc_instance_client.instance_list()
    assert len(instances) == 0


def test_replica_delete(grpc_instance_client):
    """
    Test replica_delete() method of InstanceClient.
    1. Create a disk and a corresponding lvstore.
    2. Create multiple replicas
    3. Delete replicas
    """
    # Create a disk and a corresponding lvstore
    try:
        disk_client = DiskClient(INSTANCE_MANAGER_DISK)
        disk = disk_client.disk_get(DISK_TYPE, DISK_NAME, DISK_PATH)
        print(disk)
    except Exception as e:
        raise e

    replicas = []
    num_replicas = 3

    # Create multiple replicas
    for i in range(0, num_replicas):
        r = grpc_instance_client.instance_create(backend_store_driver="spdk",
                                                 name="replica-{}".format(i),
                                                 type="replica",
                                                 volume_name="test-vol",
                                                 disk_name=DISK_NAME,
                                                 disk_uuid=disk.uuid,
                                                 size=104857600,
                                                 expose_required=False)
        replicas += [r]

    assert len(replicas) == num_replicas

    # Delete replicas
    for i in range(0, num_replicas):
        grpc_instance_client.instance_delete(backend_store_driver="spdk",
                                             name="replica-{}".format(i),
                                             type="replica",
                                             disk_uuid=disk.uuid,
                                             cleanup_required=True)
        replicas = grpc_instance_client.instance_list()
        assert len(replicas) == num_replicas - i - 1


def test_engine_frontend_nvmf(grpc_instance_client):
    """
    Test engine_create() method and "spdk-tcp-nvmf" frontend.
    1. Create a disk and a corresponding lvstore.
    2. Create replicas
    3. Create an engine
    4. Delete the engine
    5. Delete replicas
    """
    # Create a disk and a corresponding lvstore
    try:
        disk_client = DiskClient(INSTANCE_MANAGER_DISK)
        disk = disk_client.disk_get(DISK_TYPE, DISK_NAME, DISK_PATH)
        print(disk)
    except Exception as e:
        raise e

    replica_name = "replica-1"
    volume_name = "test-vol"

    pod_ip = get_pod_ip()
    assert pod_ip != ""

    # Create multiple running replicas
    replica_address_map = {}
    num_replicas = 3
    for i in range(0, num_replicas):
        replica_name = "replica-{}".format(i)
        r = grpc_instance_client.instance_create(backend_store_driver="spdk",
                                                 name=replica_name,
                                                 type="replica",
                                                 volume_name=volume_name,
                                                 disk_name=DISK_NAME,
                                                 disk_uuid=disk.uuid,
                                                 size=104857600,
                                                 expose_required=False)
        assert r.status.state == "running"
        replica_address_map[replica_name] = "{}:{}".format(pod_ip, r.status.port_start) # NOQA

    instances = grpc_instance_client.instance_list()
    assert len(instances) == num_replicas

    # Create a running engine
    frontend = "spdk-tcp-nvmf"
    engine = grpc_instance_client.instance_create(backend_store_driver="spdk",
                                                  name="engine-1",
                                                  type="engine",
                                                  volume_name=volume_name,
                                                  size=104857600,
                                                  replica_address_map=replica_address_map, # NOQA
                                                  frontend=frontend)
    assert engine.status.state == "running"

    instances = grpc_instance_client.instance_list()
    assert len(instances) == num_replicas + 1  # replicas + engine

    # Stop the engine
    grpc_instance_client.instance_delete(backend_store_driver="spdk",
                                         name="engine-1",
                                         type="engine",
                                         disk_uuid="",
                                         cleanup_required=True)
    instances = grpc_instance_client.instance_list()
    assert len(instances) == num_replicas

    # Delete replicas
    for i in range(0, num_replicas):
        grpc_instance_client.instance_delete(backend_store_driver="spdk",
                                             name="replica-{}".format(i),
                                             type="replica",
                                             disk_uuid=disk.uuid,
                                             cleanup_required=True)
        replicas = grpc_instance_client.instance_list()
        assert len(replicas) == num_replicas - i - 1
