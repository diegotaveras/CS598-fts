#!/usr/bin/env python3
import argparse
import json
import shutil
import subprocess
from pathlib import Path

from render_env import expand_nodes


DEFAULT_LOCAL_FILES = [
    "benchmarking/dispatch_events.jsonl",
    "benchmarking/attempt_metrics.jsonl",
    "benchmarking/attempt_metrics.csv",
    "benchmarking/summary.json",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Clear benchmark run artifacts locally and on all VMs so the next "
            "benchmark starts with fresh event files."
        )
    )
    parser.add_argument(
        "--config",
        default="deploy/vm_workers.json",
        help="VM worker config JSON. Default: deploy/vm_workers.json",
    )
    parser.add_argument(
        "--ssh-user",
        default="diegotav",
        help="SSH username for VM cleanup. Default: diegotav",
    )
    parser.add_argument(
        "--remote-repo",
        default="~/CS598-fts",
        help="Repo path on each VM. Default: ~/CS598-fts",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Only clear local benchmark artifacts; do not SSH to VMs.",
    )
    parser.add_argument(
        "--remote-only",
        action="store_true",
        help="Only clear VM event files; do not clear local benchmark artifacts.",
    )
    parser.add_argument(
        "--keep-pulled-events",
        action="store_true",
        help="Keep local benchmarking/events/from_vms instead of deleting it.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be removed without deleting anything.",
    )
    parser.add_argument(
        "--sudo",
        action="store_true",
        help="Use sudo for remote event-file deletion. Useful when Docker wrote root-owned files.",
    )
    return parser.parse_args()


def load_vm_hosts(config_path: Path) -> list[str]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    nodes = expand_nodes(config)
    hosts = []
    seen = set()
    for node in nodes:
        host = str(node.get("addr", ""))
        if ":" in host:
            host = host.rsplit(":", 1)[0]
        if host and host not in seen:
            seen.add(host)
            hosts.append(host)
    return hosts


def clear_local_artifacts(repo_root: Path, keep_pulled_events: bool, dry_run: bool):
    for relative_path in DEFAULT_LOCAL_FILES:
        path = repo_root / relative_path
        if dry_run:
            print(f"would remove local file: {path}")
            continue
        if path.exists():
            path.unlink()
            print(f"removed local file: {path}")

    pulled_events = repo_root / "benchmarking/events/from_vms"
    if keep_pulled_events:
        return
    if dry_run:
        print(f"would remove local directory: {pulled_events}")
    elif pulled_events.exists():
        shutil.rmtree(pulled_events)
        print(f"removed local directory: {pulled_events}")


def clear_remote_events(
    hosts: list[str],
    ssh_user: str,
    remote_repo: str,
    dry_run: bool,
    use_sudo: bool,
):
    rm_command = "sudo rm -f" if use_sudo else "rm -f"
    remote_command = (
        f"mkdir -p {remote_repo}/benchmarking/events && "
        f"{rm_command} {remote_repo}/benchmarking/events/*.jsonl"
    )
    for host in hosts:
        target = f"{ssh_user}@{host}"
        if dry_run:
            print(f"would run: ssh {target!r} {remote_command!r}")
            continue
        print(f"clearing remote events on {target}")
        subprocess.run(["ssh", target, remote_command], check=True)


def main():
    args = parse_args()
    if args.local_only and args.remote_only:
        raise SystemExit("--local-only and --remote-only cannot be used together")

    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / args.config

    if not args.remote_only:
        clear_local_artifacts(
            repo_root,
            keep_pulled_events=args.keep_pulled_events,
            dry_run=args.dry_run,
        )

    if not args.local_only:
        hosts = load_vm_hosts(config_path)
        if not hosts:
            raise SystemExit(f"No VM hosts found in {config_path}")
        clear_remote_events(
            hosts,
            ssh_user=args.ssh_user,
            remote_repo=args.remote_repo,
            dry_run=args.dry_run,
            use_sudo=args.sudo,
        )


if __name__ == "__main__":
    main()
