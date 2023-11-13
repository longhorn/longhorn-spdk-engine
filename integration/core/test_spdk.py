import os
import grpc
import pytest

from rpc.imrpc.process_manager_client import ProcessManagerClient
from rpc.instance.instance_client import InstanceClient
from rpc.spdk.spdk_client import SPDKClient


def test_version_get(grpc_process_manager_client, grpc_instance_client):
    '''
    Test version_get() method of ProcessManagerClient and InstanceClient.
    The two versions should be the same.
    '''
    pm_version = grpc_process_manager_client.version_get()
    instance_version = grpc_instance_client.version_get()
    assert pm_version == instance_version
