#!/bin/sh
set -eu

NODE_ID="${1:-${RAG_WORKER_ID:-}}"
if [ -z "$NODE_ID" ]; then
  echo "usage: $0 <node-id>" >&2
  echo "example: $0 node3" >&2
  exit 2
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONFIG="${RAG_VM_CONFIG:-$REPO_ROOT/deploy/vm_nodes.json}"
ENV_OUT="${RAG_VM_ENV_OUT:-$REPO_ROOT/deploy/generated/$NODE_ID.env}"

if [ ! -f "$CONFIG" ]; then
  echo "missing VM config: $CONFIG" >&2
  echo "copy deploy/vm_nodes.example.json to deploy/vm_nodes.json and edit VM IPs" >&2
  exit 2
fi

python "$REPO_ROOT/deploy/render_env.py" "$NODE_ID" \
  --config "$CONFIG" \
  --output "$ENV_OUT"

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  . "$REPO_ROOT/.env"
  set +a
fi

set -a
. "$ENV_OUT"
set +a

cd "$REPO_ROOT"
exec python -m rag.worker_main
