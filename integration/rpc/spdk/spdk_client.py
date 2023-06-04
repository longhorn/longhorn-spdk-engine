import grpc

import spdk_pb2
import spdk_pb2_grpc
from google.protobuf import empty_pb2

class SPDKClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = spdk_pb2_grpc.SPDKServiceStub(self.channel)

    def replica_get(self):
        return self.stub.ReplicaGet(empty_pb2.Empty())

    def replica_list(self):
        return self.stub.ReplicaList(empty_pb2.Empty())

    def engine_get(self):
        return self.stub.EngineGet(empty_pb2.Empty())

    def engine_list(self):
        return self.stub.EngineList(empty_pb2.Empty())