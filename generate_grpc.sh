#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./proto/vendor/protobuf/src/google/protobuf ]; then
    rm -rf ./proto/vendor/protobuf/src/google/protobuf
    DIR="./proto/vendor/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi

# proto lint check
#buf check lint

mkdir -p integration/rpc/spdk
protoc -I proto/spdkrpc/ -I proto/vendor/protobuf/src/ proto/spdkrpc/spdk.proto --go_out=plugins=grpc:proto/spdkrpc/
python3 -m grpc_tools.protoc -I proto/spdkrpc/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/spdk --grpc_python_out=integration/rpc/spdk proto/spdkrpc/spdk.proto
