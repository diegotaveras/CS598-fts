# CS598-fts

Distributed RAG prototype using PageIndex document trees and Semantica-style
routing. Each worker owns a local document shard, builds a local user embedding
from its PageIndex tree summaries, joins a routing tree, and answers queries by
chain-hopping through nearby users instead of broadcasting to every node.

## What This Repo Does

- Loads local PDFs from `rag_docs/node*/`.
- Uses PageIndex for document tree creation and retrieval.
- Embeds PageIndex tree nodes locally with BERT for routing decisions.
- Builds one user embedding per worker from that worker's local documents.
- Maintains a routing tree with custodian nodes and closest-user metadata.
- Routes queries with chain-hop forwarding.
- Calls OpenRouter/OpenAI-compatible LLMs only for final answer synthesis.
- Emits JSONL benchmark events for routing, PageIndex calls, latency, and failover.
- Supports local Docker tests and multi-VM deployment helpers.

## Key Files

- [rag/worker_main.py](/Users/taver/Desktop/CS598-fts/rag/worker_main.py): gRPC worker, routing layer, PageIndex retrieval, final answer generation.
- [rag/document_store.py](/Users/taver/Desktop/CS598-fts/rag/document_store.py): local docs, PageIndex manifests, node embeddings, user embedding.
- [rag/leader_election.py](/Users/taver/Desktop/CS598-fts/rag/leader_election.py): DynamoDB lease-based leader election.
- [rag/rag.proto](/Users/taver/Desktop/CS598-fts/rag/rag.proto): worker RPC API.
- [benchmarking/](/Users/taver/Desktop/CS598-fts/benchmarking): benchmark generation, runner, summaries, plots.
- [deploy/](/Users/taver/Desktop/CS598-fts/deploy): VM/container deployment helpers.
- [docker-compose.rag.yml](/Users/taver/Desktop/CS598-fts/docker-compose.rag.yml): local 4-worker RAG stack.
- [docker-compose.rag.6.yml](/Users/taver/Desktop/CS598-fts/docker-compose.rag.6.yml), [docker-compose.rag.8.yml](/Users/taver/Desktop/CS598-fts/docker-compose.rag.8.yml): local 6/8-worker election tests.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Create a `.env` with the keys you need:

```bash
PAGE_INDEX_API_KEY=...
OPENROUTER_API_KEY=...
RAG_MODEL_NAME=nvidia/nemotron-3-super-120b-a12b:free
```

Optional AWS/DynamoDB credentials are needed for leader election:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
```

## Document Layout

For local Docker, place PDFs under:

```text
rag_docs/
  node1/
  node2/
  node3/
  node4/
```

For VM deployment with several containers per VM, each VM still uses local
folders like `rag_docs/node1` through `rag_docs/node4`; the deploy config maps
global worker ids to those local document folders.

Each node may also have a `.pageindex_manifest.json` created by the PageIndex
loading path so the worker can reuse existing PageIndex document ids.

## Run Locally

Four workers with fixed `node1` leader:

```bash
docker compose -f docker-compose.rag.yml up -d --build
docker logs -f rag-node1
```

Stop:

```bash
docker compose -f docker-compose.rag.yml down
```

Run with DynamoDB leader election instead of fixed `node1`:

```bash
docker compose -f docker-compose.rag.yml -f docker-compose.rag.election.yml up -d --build
```

Run 6 or 8 local workers:

```bash
docker compose -f docker-compose.rag.yml -f docker-compose.rag.6.yml up -d --build
docker compose -f docker-compose.rag.yml -f docker-compose.rag.8.yml up -d --build
```

When switching sizes, prefer `down` then `up -d` so old containers do not stay
around.

## VM Deployment

Configure workers in:

```bash
cp deploy/vm_workers.example.json deploy/vm_workers.json
```

Send deploy files to the VMs:

```bash
./deploy/scp_deploy_to_vms.sh
```

Start all containers for one VM:

```bash
./deploy/start_vm_containers.sh vm1
```

Skip image rebuilds when dependencies have not changed:

```bash
RAG_BUILD_IMAGE=0 ./deploy/start_vm_containers.sh vm1
```

List configured workers:

```bash
./deploy/list_workers.py
```

Clear benchmark outputs before a new run:

```bash
./deploy/clear_benchmark_run.py
```

## Benchmarking


More benchmarking plots and data from previous runs found within the `benchmarking-data` branch.

Generate benchmark rows from `pdfs/*/rows.jsonl` and the PDF distribution plan:

```bash
python benchmarking/generate_benchmark_jsonl.py \
  --modes chain_hop \
  --initiators random \
  --random-seed 598 \
  --attempts 2 \
  --output benchmarking/benchmark.jsonl
```

Run the benchmark from one machine:

```bash
python benchmarking/run_benchmark.py \
  --benchmark benchmarking/benchmark.jsonl \
  --config deploy/vm_workers.json \
  --delay-seconds 2 \
  --final-wait-seconds 180
```

Summarize worker event logs:

```bash
python benchmarking/summarize_events.py \
  --benchmark benchmarking/benchmark.jsonl \
  --events "benchmarking/events/*.jsonl" \
  --dispatch-events benchmarking/dispatch_events.jsonl
```

Generate plots:

```bash
python benchmarking/plot_evaluation.py
python benchmarking/plot_failover_rebuild.py
```

Useful outputs:

- `benchmarking/attempt_metrics.jsonl`
- `benchmarking/attempt_metrics.csv`
- `benchmarking/summary.json`
- `benchmarking/plots/`

## Leader Failover Test

Start a local election stack, wait for convergence, then kill the current
leader and measure recovery:

```bash
rm -f benchmarking/events/*.jsonl
docker compose -f docker-compose.rag.yml -f docker-compose.rag.8.yml restart
sleep 90
python deploy/test_leader_failover.py --local-docker --restart-old-leader
sleep 30
python benchmarking/summarize_failover_recovery.py \
  --events "benchmarking/events/*.jsonl"
```

The failover summary reports:

- `available_users_joined_seconds`: time until the new root has all currently live nodes, usually `N-1` after one failed leader.
- `all_users_joined_seconds`: time until all expected nodes, including the old leader, have joined.
- `max_joined_count`: largest observed rebuilt membership.

## Important Environment Variables

- `PAGE_INDEX_API_KEY` or `PAGEINDEX_API_KEY`: PageIndex API key.
- `OPENROUTER_API_KEY`: OpenRouter key for final answer generation.
- `RAG_MODEL_NAME`: final generation model.
- `RAG_ROUTING_MODE`: currently `join_tree` for routing-tree chain hop.
- `RAG_CHAIN_HOP_MAX_HOPS`: max query hops.
- `RAG_LOCAL_RETRIEVAL_POLICY`: `top1_probe` or `topk_probe`.
- `RAG_QUERY_DOC_MATCH_THRESHOLD`: document-root match threshold.
- `RAG_QUERY_USER_MATCH_THRESHOLD`: next-user match threshold.
- `RAG_COORDINATOR_SUMMARY_DELAY_SECONDS`: coordinator wait before final answer.
- `RAG_ROUTING_TREE_EXPECTED_USERS`: expected cluster size.
- `RAG_ROUTING_TREE_RECORD_LIMIT`: max peers per routing-tree leaf before split.
- `RAG_ROUTING_TREE_CLOSEST_USERS`: closest-user list size.
- `RAG_LEASE_DURATION_SECONDS`: DynamoDB leader lease duration.
- `RAG_LEADER_MONITOR_SECONDS`: worker poll interval for observing leader changes.
- `RAG_BENCHMARK_EVENTS_PATH`: per-worker JSONL event output.

## Notes

- PageIndex is used for document tree creation and retrieval only, not the
  PageIndex chat API.
- The final answer call is OpenRouter/OpenAI-compatible through
  [rag/llm_client.py](/Users/taver/Desktop/CS598-fts/rag/llm_client.py).
- The raw measured end-to-end benchmark latency includes
  `RAG_COORDINATOR_SUMMARY_DELAY_SECONDS`. The query-latency plot uses the
  modeled no-wait latency based on PageIndex call latency, observed average
  nodes contacted, and final summary latency.
