#!/bin/sh
set -e

cd "$(dirname "$0")/.."

rm -f rag/rag_pb2.py rag/rag_pb2_grpc.py

python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  rag/rag.proto
