# VM RAG Deployment Helper

This helper runs `rag_worker` processes or containers on real VMs.

Current 8-VM testing uses `deploy/vm_workers.json`: 32 global workers, 4
containers per VM.

## 1. Configure Nodes

For one worker per VM, copy the example config and replace the addresses:

```sh
cp deploy/vm_nodes.example.json deploy/vm_nodes.json
```

Each node entry uses:

```json
{"id": "node3", "addr": "10.0.0.13"}
```

If a node uses a non-default port, add `"port": 9101` to that node.

## 2. Put Secrets In `.env`

Keep API keys in the repo root `.env` on each VM:

```sh
PAGEINDEX_API_KEY=...
OPENROUTER_API_KEY=...
RAG_MODEL_NAME=nvidia/nemotron-3-super-120b-a12b:free
```

The generated per-node env files do not include secrets.

## 3. Start A Node

On VM 1:

```sh
./deploy/start_node.sh node1
```

On VM 2:

```sh
./deploy/start_node.sh node2
```

Continue through `node8`.

The script renders `deploy/generated/<node-id>.env`, sources `.env`, sources the generated env, then runs:

```sh
python -m rag.worker_main
```

## 4. Useful Checks

Verify each VM can reach the others on gRPC:

```sh
nc -vz 10.0.0.11 9100
nc -vz 10.0.0.12 9100
```

Render without starting:

```sh
python deploy/render_env.py node3 --config deploy/vm_nodes.json
```

## Notes

- `init_worker_id` is a fallback/bootstrap identity, not the routing root in the VM config.
- With `RAG_FORCE_LEADER_ID` unset, workers use AWS/DynamoDB leader election and join the elected leader as the routing root.
- `deploy/render_env.py` emits `RAG_WORKER_ADDR_MAP` so an elected `nodeN` can be resolved to its real VM host and port.
- `node1` runs the bootstrap query if configured in `bootstrap`.
- `RAG_NEIGHBORS` is currently all other nodes, but chain-hop routing uses the routing-tree closest-user metadata.
- Full tree embedding can make startup slow. The example config uses long retry windows and a delayed bootstrap query for that reason.

## Multiple Containers Per VM

Use `deploy/vm_workers.example.json` when each VM should run a configurable number of worker containers.

```sh
cp deploy/vm_workers.example.json deploy/vm_workers.json
```

Set:

```json
{
  "workers_per_vm": 2,
  "total_workers": 16,
  "vms": [
    {"id": "vm1", "addr": "10.0.0.11"},
    {"id": "vm2", "addr": "10.0.0.12"}
  ]
}
```

Workers are generated globally as `node1..nodeN`. With `workers_per_vm=2`, VM 1 gets `node1,node2`, VM 2 gets `node3,node4`, and so on. Ports increment from `port`, so the first two workers on a VM advertise `10.0.0.11:9100` and `10.0.0.11:9101`.

List generated workers:

```sh
python deploy/list_workers.py --config deploy/vm_workers.json
python deploy/list_workers.py --config deploy/vm_workers.json --vm-id vm1
```

Start the containers assigned to a VM:

```sh
./deploy/start_worker_container.sh node1
./deploy/start_worker_container.sh node2
```

Or start every worker assigned to one VM id:

```sh
./deploy/start_vm_containers.sh vm1
```

Each container listens on port `9100` internally, and the helper maps it to the
generated host port.

Global worker IDs are `node1..nodeN`, but document folders are local to each VM.
With `workers_per_vm=4`, every VM uses only:

```text
rag_docs/node1
rag_docs/node2
rag_docs/node3
rag_docs/node4
```

For example, global `node22` on VM6 reads from VM6's local:

```text
rag_docs/node2
```

The helper prints the mapping at startup:

```text
worker: node22 local-doc-node: node2
docs: /home/diegotav/CS598-fts/rag_docs/node2 -> /data/rag
```

By default, a one-worker-per-VM config reads docs for `node3` from:

```text
rag_docs/node3
```

Override per run if needed:

```sh
RAG_VM_CONFIG=deploy/vm_workers.json \
RAG_HOST_DOC_DIR=/mnt/financebench/node3 \
./deploy/start_worker_container.sh node3
```

Containers start detached by default. Set this to keep one worker in the foreground:

```sh
RAG_DOCKER_DETACH=0 ./deploy/start_worker_container.sh node3
```

## 8-VM Test Checklist

Copy deploy scripts to every VM:

```sh
./deploy/scp_deploy_to_vms.sh
```

Create the local doc folders on every VM:

```sh
./deploy/mkdir_rag_docs_on_vms.sh
```

Copy PDFs according to the generated distribution plan:

```sh
python deploy/scp_pdfs_to_vm_nodes.py \
  --workers-per-vm 4 \
  --total-workers 32 \
  --plan-json deploy/pdf_distribution_plan.json
```

On each VM, start its local worker set:

```sh
./deploy/start_vm_containers.sh vm1
```

Use `vm2`, `vm3`, ... on the other VMs.

Benchmark events are written per worker under:

```text
benchmarking/events/node<N>.jsonl
```

If stale global doc folders were created by an old deploy, archive them:

```sh
./deploy/archive_stale_global_rag_docs_on_vms.sh
```
