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
CONFIG="${RAG_VM_CONFIG:-$REPO_ROOT/deploy/vm_workers.json}"
ENV_OUT="${RAG_VM_ENV_OUT:-$REPO_ROOT/deploy/generated/$NODE_ID.docker.env}"
IMAGE="${RAG_DOCKER_IMAGE:-cs598-rag:latest}"
CONTAINER_NAME="${RAG_CONTAINER_NAME:-rag-$NODE_ID}"
CONTAINER_PORT="${RAG_CONTAINER_PORT:-9100}"
HOST_HF_CACHE="${RAG_HOST_HF_CACHE:-$REPO_ROOT/hf_cache/$NODE_ID}"
BENCHMARK_EVENTS_PATH="${RAG_BENCHMARK_EVENTS_PATH:-/app/benchmarking/events/$NODE_ID.jsonl}"
DETACH="${RAG_DOCKER_DETACH:-1}"

if [ ! -f "$CONFIG" ]; then
  echo "missing VM worker config: $CONFIG" >&2
  echo "copy deploy/vm_workers.example.json to deploy/vm_workers.json and edit VM IPs/counts" >&2
  exit 2
fi

python "$REPO_ROOT/deploy/render_env.py" "$NODE_ID" \
  --config "$CONFIG" \
  --format env-file \
  --output "$ENV_OUT"

HOST_PORT=$(
  python - "$ENV_OUT" <<'PY'
import sys
from pathlib import Path

env = {}
for line in Path(sys.argv[1]).read_text().splitlines():
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    env[key] = value
advertise = env["RAG_ADVERTISE_ADDR"]
print(advertise.rsplit(":", 1)[1])
PY
)

LOCAL_DOC_NODE_ID=$(
  python - "$ENV_OUT" "$NODE_ID" <<'PY'
import sys
from pathlib import Path

env = {}
for line in Path(sys.argv[1]).read_text().splitlines():
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    env[key] = value
print(env.get("RAG_LOCAL_DOC_NODE_ID") or sys.argv[2])
PY
)
HOST_DOC_DIR="${RAG_HOST_DOC_DIR:-$REPO_ROOT/rag_docs/$LOCAL_DOC_NODE_ID}"

WORKERS_PER_VM=$(
  python - "$CONFIG" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(int(config.get("workers_per_vm", 1)))
PY
)
LOCAL_DOC_NODE_NUMBER=$(
  python - "$LOCAL_DOC_NODE_ID" <<'PY'
import re
import sys

match = re.fullmatch(r"node(\d+)", sys.argv[1])
print(match.group(1) if match else "0")
PY
)
if [ "$LOCAL_DOC_NODE_NUMBER" -lt 1 ] || [ "$LOCAL_DOC_NODE_NUMBER" -gt "$WORKERS_PER_VM" ]; then
  echo "invalid local doc node id: $LOCAL_DOC_NODE_ID for workers_per_vm=$WORKERS_PER_VM" >&2
  echo "refusing to start $NODE_ID because it would mount the wrong rag_docs folder" >&2
  exit 2
fi

mkdir -p "$HOST_DOC_DIR" "$HOST_HF_CACHE"

docker build -t "$IMAGE" "$REPO_ROOT"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

DOCKER_ENV_ARGS="--env-file $ENV_OUT"
if [ -f "$REPO_ROOT/.env" ]; then
  DOCKER_ENV_ARGS="--env-file $REPO_ROOT/.env $DOCKER_ENV_ARGS"
fi

if [ "$DETACH" = "1" ]; then
  # shellcheck disable=SC2086
  docker run \
    -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -p "$HOST_PORT:$CONTAINER_PORT" \
    $DOCKER_ENV_ARGS \
    -e HF_HOME=/root/.cache/huggingface \
    -e RAG_BENCHMARK_EVENTS_PATH="$BENCHMARK_EVENTS_PATH" \
    -v "$REPO_ROOT:/app" \
    -v "$HOST_DOC_DIR:/data/rag" \
    -v "$HOST_HF_CACHE:/root/.cache/huggingface" \
    "$IMAGE"
  echo "started $CONTAINER_NAME on host port $HOST_PORT"
  echo "worker: $NODE_ID local-doc-node: $LOCAL_DOC_NODE_ID"
  echo "docs: $HOST_DOC_DIR -> /data/rag"
  echo "logs: docker logs -f $CONTAINER_NAME"
else
  # shellcheck disable=SC2086
  exec docker run \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -p "$HOST_PORT:$CONTAINER_PORT" \
    $DOCKER_ENV_ARGS \
    -e HF_HOME=/root/.cache/huggingface \
    -e RAG_BENCHMARK_EVENTS_PATH="$BENCHMARK_EVENTS_PATH" \
    -v "$REPO_ROOT:/app" \
    -v "$HOST_DOC_DIR:/data/rag" \
    -v "$HOST_HF_CACHE:/root/.cache/huggingface" \
    "$IMAGE"
fi
