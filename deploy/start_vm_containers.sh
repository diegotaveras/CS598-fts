#!/bin/sh
set -eu

VM_ID="${1:-${RAG_VM_ID:-}}"
if [ -z "$VM_ID" ]; then
  echo "usage: $0 <vm-id>" >&2
  echo "example: $0 vm3" >&2
  exit 2
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONFIG="${RAG_VM_CONFIG:-$REPO_ROOT/deploy/vm_workers.json}"

if [ ! -f "$CONFIG" ]; then
  echo "missing VM worker config: $CONFIG" >&2
  exit 2
fi

python "$REPO_ROOT/deploy/list_workers.py" --config "$CONFIG" --vm-id "$VM_ID" |
while IFS="$(printf '\t')" read -r NODE_ID _VM_ID _ENDPOINT; do
  [ -n "$NODE_ID" ] || continue
  RAG_VM_CONFIG="$CONFIG" "$REPO_ROOT/deploy/start_worker_container.sh" "$NODE_ID"
done
