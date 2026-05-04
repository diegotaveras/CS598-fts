#!/usr/bin/env python3
import argparse
import json
import shlex
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Render a per-VM environment file for a RAG worker."
    )
    parser.add_argument("node_id", help="Node id to render, for example node3.")
    parser.add_argument(
        "--config",
        default="deploy/vm_nodes.json",
        help="Path to VM node config JSON. Default: deploy/vm_nodes.json",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write env output to this file instead of stdout.",
    )
    parser.add_argument(
        "--format",
        choices=["shell", "env-file"],
        default="shell",
        help="Output format. Use env-file for docker --env-file.",
    )
    return parser.parse_args()


def endpoint(node: dict, default_port: int) -> str:
    if node.get("endpoint"):
        return str(node["endpoint"])
    addr = str(node["addr"])
    port = int(node.get("port", default_port))
    if ":" in addr:
        return addr
    return f"{addr}:{port}"


def expand_nodes(config: dict) -> list[dict]:
    if config.get("nodes"):
        return config["nodes"]

    vms = config.get("vms", [])
    if not vms:
        return []

    workers_per_vm = int(config.get("workers_per_vm", 1))
    total_workers = config.get("total_workers")
    total_workers = int(total_workers) if total_workers is not None else len(vms) * workers_per_vm
    base_port = int(config.get("port", 9100))
    listen_port = int(config.get("container_port", config.get("listen_port", 9100)))

    nodes = []
    worker_number = 1
    for vm in vms:
        for local_index in range(workers_per_vm):
            if worker_number > total_workers:
                return nodes
            node_id = f"node{worker_number}"
            host_port = int(vm.get("base_port", base_port)) + local_index
            nodes.append(
                {
                    "id": node_id,
                    "addr": vm["addr"],
                    "port": host_port,
                    "listen_port": listen_port,
                    "vm_id": vm.get("id", f"vm{len(nodes) + 1}"),
                    "local_worker_index": local_index,
                }
            )
            worker_number += 1
    return nodes


def render_env(config: dict, node_id: str) -> dict[str, str]:
    nodes = expand_nodes(config)
    node_by_id = {node["id"]: node for node in nodes}
    if node_id not in node_by_id:
        choices = ", ".join(sorted(node_by_id))
        raise SystemExit(f"Unknown node_id={node_id!r}. Known nodes: {choices}")

    port = int(config.get("port", 9100))
    node = node_by_id[node_id]
    init_worker_id = str(config.get("init_worker_id", "node1"))
    if init_worker_id not in node_by_id:
        raise SystemExit(f"init_worker_id={init_worker_id!r} is not in nodes")

    advertise_addr = endpoint(node, port)
    init_addr = endpoint(node_by_id[init_worker_id], port)
    worker_addr_map = {
        candidate["id"]: endpoint(candidate, port)
        for candidate in nodes
    }
    neighbors = [
        endpoint(candidate, port)
        for candidate in nodes
        if candidate["id"] != node_id
    ]

    env = {
        key: str(value)
        for key, value in config.get("defaults", {}).items()
    }
    env.update(
        {
            "ROLE": env.get("ROLE", "rag_worker"),
            "RAG_WORKER_ID": node_id,
            "RAG_HOST": str(node.get("host", config.get("host", "0.0.0.0"))),
            "RAG_PORT": str(node.get("listen_port", node.get("port", port))),
            "RAG_ADVERTISE_ADDR": advertise_addr,
            "RAG_DOC_DIR": str(node.get("doc_dir", config.get("doc_dir", "/data/rag"))),
            "RAG_INIT_WORKER_ID": init_worker_id,
            "RAG_INIT_ADDR": init_addr,
            "RAG_WORKER_ADDR_MAP": json.dumps(worker_addr_map, separators=(",", ":")),
            "RAG_NEIGHBORS": ",".join(neighbors),
            "RAG_ROUTING_TREE_EXPECTED_USERS": env.get(
                "RAG_ROUTING_TREE_EXPECTED_USERS",
                str(len(nodes)),
            ),
        }
    )

    bootstrap = config.get("bootstrap", {})
    if bootstrap and node_id == bootstrap.get("node_id", init_worker_id):
        env["RAG_BOOTSTRAP_QUERY"] = str(bootstrap.get("query", ""))
        env["RAG_BOOTSTRAP_DELAY_SECONDS"] = str(bootstrap.get("delay_seconds", 180))
    else:
            env["RAG_BOOTSTRAP_QUERY"] = ""
            env["RAG_BOOTSTRAP_DELAY_SECONDS"] = "0"

    if "local_worker_index" in node:
        local_worker_number = int(node["local_worker_index"]) + 1
        env["RAG_LOCAL_WORKER_INDEX"] = str(local_worker_number)
        env["RAG_LOCAL_DOC_NODE_ID"] = f"node{local_worker_number}"
    else:
        env["RAG_LOCAL_WORKER_INDEX"] = ""
        env["RAG_LOCAL_DOC_NODE_ID"] = node_id

    for key, value in node.get("env", {}).items():
        env[key] = str(value)

    return env


def format_env(env: dict[str, str]) -> str:
    lines = [
        "# Generated by deploy/render_env.py. Do not put secrets in this file.",
    ]
    for key in sorted(env):
        lines.append(f"export {key}={shlex.quote(env[key])}")
    return "\n".join(lines) + "\n"


def format_docker_env_file(env: dict[str, str]) -> str:
    lines = [
        "# Generated by deploy/render_env.py. Do not put secrets in this file.",
    ]
    for key in sorted(env):
        value = env[key].replace("\n", "\\n")
        lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    env = render_env(config, args.node_id)
    if args.format == "env-file":
        text = format_docker_env_file(env)
    else:
        text = format_env(env)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
