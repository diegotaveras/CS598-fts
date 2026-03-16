#!/bin/bash
set -e

rm -f network_pb2.py
rm -f network_pb2_grpc.py

echo "Generating new protobuf files..."

python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    network.proto