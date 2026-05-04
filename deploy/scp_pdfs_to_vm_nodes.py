#!/usr/bin/env python3
import argparse
import json
import shlex
import subprocess
from pathlib import Path


DEFAULT_CONFIG = "deploy/vm_workers.json"
DEFAULT_SOURCE_DIR = "pdfs"
DEFAULT_REMOTE_USER = "diegotav"
DEFAULT_REMOTE_REPO = "~/CS598-fts"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Distribute local PDF category folders across VM-backed RAG node folders "
            "and copy them with ssh/scp."
        )
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"VM worker config JSON. Default: {DEFAULT_CONFIG}",
    )
    parser.add_argument(
        "--source-dir",
        default=DEFAULT_SOURCE_DIR,
        help=f"Local directory containing category folders. Default: {DEFAULT_SOURCE_DIR}",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help=(
            "Category folder name to include. Can be passed multiple times. "
            "Defaults to all non-hidden folders under --source-dir."
        ),
    )
    parser.add_argument(
        "--remote-user",
        default=DEFAULT_REMOTE_USER,
        help=f"Remote SSH username. Default: {DEFAULT_REMOTE_USER}",
    )
    parser.add_argument(
        "--remote-repo",
        default=DEFAULT_REMOTE_REPO,
        help=f"Remote repo path. Default: {DEFAULT_REMOTE_REPO}",
    )
    parser.add_argument(
        "--workers-per-vm",
        type=int,
        default=None,
        help="Override workers_per_vm from config, e.g. 4.",
    )
    parser.add_argument(
        "--total-workers",
        type=int,
        default=None,
        help="Override total_workers from config, e.g. 32.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print mkdir/scp commands and assignment plan without running them.",
    )
    parser.add_argument(
        "--plan-json",
        default=None,
        help="Optional path to write the node-to-PDF assignment plan as JSON.",
    )
    parser.add_argument(
        "--remote-node-id-mode",
        choices=["local", "global"],
        default="local",
        help=(
            "Remote rag_docs folder naming. 'local' writes node1..nodeN on each VM; "
            "'global' writes global node ids such as node17. Default: local."
        ),
    )
    return parser.parse_args()


def load_config(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def expand_nodes(config: dict, workers_per_vm: int | None, total_workers: int | None):
    if config.get("nodes") and workers_per_vm is None and total_workers is None:
        return config["nodes"]

    vms = config.get("vms", [])
    if not vms:
        raise ValueError("Config must contain either nodes or vms")

    workers_per_vm = workers_per_vm or int(config.get("workers_per_vm", 1))
    total_workers = total_workers or int(
        config.get("total_workers", len(vms) * workers_per_vm)
    )

    nodes = []
    worker_number = 1
    for vm_index, vm in enumerate(vms, start=1):
        for local_index in range(1, workers_per_vm + 1):
            if worker_number > total_workers:
                return nodes
            nodes.append(
                {
                    "id": f"node{worker_number}",
                    "addr": vm["addr"],
                    "vm_id": vm.get("id", f"vm{vm_index}"),
                    "local_worker_index": local_index,
                }
            )
            worker_number += 1
    return nodes


def category_dirs(source_dir: Path, names: list[str]):
    if names:
        dirs = [source_dir / name for name in names]
    else:
        dirs = [
            path
            for path in source_dir.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ]
    missing = [path for path in dirs if not path.exists()]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing category directories: {missing_text}")
    return sorted(dirs, key=lambda path: path.name)


def discover_pdfs(source_dir: Path, categories: list[str]):
    pdfs = []
    for category_dir in category_dirs(source_dir, categories):
        for pdf_path in sorted(category_dir.rglob("*.pdf")):
            pdfs.append(
                {
                    "category": category_dir.name,
                    "path": pdf_path,
                }
            )
    return pdfs


def build_plan(nodes: list[dict], pdfs: list[dict]):
    if not nodes:
        raise ValueError("No target nodes found")
    if not pdfs:
        raise ValueError("No PDFs found to distribute")

    base_count = len(pdfs) // len(nodes)
    extra_count = len(pdfs) % len(nodes)
    plan = []
    pdf_index = 0
    for node_index, node in enumerate(nodes):
        count = base_count + (1 if node_index < extra_count else 0)
        assigned = pdfs[pdf_index:pdf_index + count]
        pdf_index += count
        plan.append(
            {
                **node,
                "remote_node_id": f"node{node.get('local_worker_index', node_index + 1)}",
                "pdfs": assigned,
            }
        )
    return plan


def printable_plan(plan: list[dict], remote_repo: str = DEFAULT_REMOTE_REPO):
    result = []
    for item in plan:
        result.append(
            {
                "node_id": item["id"],
                "remote_node_id": item.get("remote_node_id", item["id"]),
                "remote_dir": remote_node_dir(
                    remote_repo,
                    item.get("remote_node_id", item["id"]),
                ),
                "vm_id": item.get("vm_id"),
                "addr": item["addr"],
                "pdf_count": len(item["pdfs"]),
                "pdfs": [
                    {
                        "category": pdf["category"],
                        "path": str(pdf["path"]),
                    }
                    for pdf in item["pdfs"]
                ],
            }
        )
    return result


def write_plan(path: str, plan: list[dict], remote_repo: str):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(printable_plan(plan, remote_repo), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"wrote plan to {output_path}")


def run_command(command: list[str], dry_run: bool):
    print(" ".join(shlex.quote(part) for part in command))
    if dry_run:
        return
    subprocess.run(command, check=True)


def remote_node_dir(remote_repo: str, node_id: str):
    return f"{remote_repo}/rag_docs/{node_id}"


def destination_node_id(item: dict, args):
    if args.remote_node_id_mode == "global":
        return item["id"]
    return item.get("remote_node_id", item["id"])


def copy_plan(plan: list[dict], args):
    for item in plan:
        node_id = item["id"]
        destination_node = destination_node_id(item, args)
        pdfs = item["pdfs"]
        if not pdfs:
            print(f"skipping {node_id}: no PDFs assigned")
            continue

        remote = f"{args.remote_user}@{item['addr']}"
        destination = remote_node_dir(args.remote_repo, destination_node)
        print(
            f"\n{node_id} -> remote {destination_node} on "
            f"{item.get('vm_id')} ({item['addr']}): "
            f"{len(pdfs)} PDF(s) -> {destination}"
        )

        run_command(
            [
                "ssh",
                remote,
                f"mkdir -p {shlex.quote(destination)}",
            ],
            args.dry_run,
        )
        run_command(
            [
                "scp",
                *[str(pdf["path"]) for pdf in pdfs],
                f"{remote}:{destination}/",
            ],
            args.dry_run,
        )


def main():
    args = parse_args()
    config = load_config(args.config)
    nodes = expand_nodes(config, args.workers_per_vm, args.total_workers)
    pdfs = discover_pdfs(Path(args.source_dir), args.category)
    plan = build_plan(nodes, pdfs)

    assigned_counts = [len(item["pdfs"]) for item in plan]
    print(
        f"found {len(pdfs)} PDFs; assigning across {len(nodes)} nodes "
        f"counts={assigned_counts}"
    )
    for item in printable_plan(plan, args.remote_repo):
        print(
            f"{item['node_id']}@{item['addr']} -> {item['remote_node_id']}: "
            f"{item['pdf_count']} PDF(s) remote_dir={item['remote_dir']}"
        )

    if args.plan_json:
        write_plan(args.plan_json, plan, args.remote_repo)

    copy_plan(plan, args)


if __name__ == "__main__":
    main()
