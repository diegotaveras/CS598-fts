#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from render_env import endpoint, expand_nodes


def parse_args():
    parser = argparse.ArgumentParser(
        description="List generated RAG workers for a VM/worker config."
    )
    parser.add_argument(
        "--config",
        default="deploy/vm_workers.json",
        help="Path to VM worker config JSON.",
    )
    parser.add_argument(
        "--vm-id",
        default=None,
        help="Only list workers assigned to this VM id.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    default_port = int(config.get("port", 9100))
    for node in expand_nodes(config):
        if args.vm_id and node.get("vm_id") != args.vm_id:
            continue
        print(f"{node['id']}\t{node.get('vm_id', '')}\t{endpoint(node, default_port)}")


if __name__ == "__main__":
    main()
