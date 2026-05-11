#!/usr/bin/env python3
import argparse
import glob
import json
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize leader failover and routing-tree rebuild events."
    )
    parser.add_argument(
        "--events",
        nargs="+",
        default=["benchmarking/events/*.jsonl", "benchmarking/events/from_vms/**/*.jsonl"],
        help="Event JSONL files or globs.",
    )
    parser.add_argument(
        "--allowed-missing-users",
        type=int,
        default=1,
        help=(
            "How many users may be missing when reporting available recovery. "
            "Use 1 for a fail-stop leader test where the old leader remains down."
        ),
    )
    return parser.parse_args()


def expand_paths(patterns):
    paths = []
    for pattern in patterns:
        matches = glob.glob(pattern, recursive=True)
        if matches:
            paths.extend(matches)
        elif Path(pattern).exists():
            paths.append(pattern)
    return sorted(set(paths))


def read_events(paths):
    events = []
    for path in paths:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                event["_path"] = path
                events.append(event)
    return sorted(events, key=lambda item: item.get("ts_unix", 0.0))


def event_ts(event):
    return event.get("ts_unix", 0.0)


def joined_progress_events(events, leader_worker_id, start_ts, end_ts):
    progress = []
    for event in events:
        ts = event_ts(event)
        if ts < start_ts or (end_ts is not None and ts >= end_ts):
            continue

        name = event.get("event")
        if name in {
            "routing_tree_rebuild_self_joined",
            "routing_tree_join_accepted",
            "routing_tree_metadata_refreshed",
        }:
            if event.get("worker_id") == leader_worker_id:
                progress.append(event)
        elif name == "routing_tree_joined":
            if event.get("root_worker_id") == leader_worker_id:
                progress.append(event)
    return sorted(progress, key=event_ts)


def first_at_or_above(events, count):
    if count is None:
        return None
    return next(
        (
            event
            for event in events
            if event.get("joined_count") is not None
            and event.get("joined_count", 0) >= count
        ),
        None,
    )


def first_metadata_at_or_above(events, leader_worker_id, count):
    if count is None:
        return None
    return next(
        (
            event
            for event in events
            if event.get("event") == "routing_tree_metadata_refreshed"
            and event.get("worker_id") == leader_worker_id
            and event.get("joined_count") is not None
            and event.get("joined_count", 0) >= count
        ),
        None,
    )


def infer_expected_count(progress):
    for event in progress:
        expected_count = event.get("expected_count")
        if expected_count:
            return expected_count
    return None


def summarize_rebuilds(events, allowed_missing_users):
    rebuilds = [
        event
        for event in events
        if event.get("event") == "routing_tree_rebuild_started"
    ]
    summaries = []
    for index, rebuild in enumerate(rebuilds):
        worker_id = rebuild.get("worker_id")
        start_ts = rebuild.get("ts_unix")
        if not worker_id or start_ts is None:
            continue
        end_ts = (
            rebuilds[index + 1].get("ts_unix")
            if index + 1 < len(rebuilds)
            else None
        )

        progress = joined_progress_events(events, worker_id, start_ts, end_ts)
        self_join = next(
            (event for event in progress if event.get("event") == "routing_tree_rebuild_self_joined"),
            None,
        )
        expected_count = infer_expected_count(progress)
        available_target_count = (
            max(1, expected_count - allowed_missing_users)
            if expected_count is not None
            else None
        )
        complete = first_at_or_above(progress, expected_count)
        available = first_at_or_above(progress, available_target_count)
        metadata_ready = first_metadata_at_or_above(progress, worker_id, expected_count)
        metadata_available = first_metadata_at_or_above(
            progress, worker_id, available_target_count
        )
        max_joined = max(
            (
                event
                for event in progress
                if event.get("joined_count") is not None
            ),
            key=lambda event: event.get("joined_count", 0),
            default=None,
        )

        summaries.append(
            {
                "leader_worker_id": worker_id,
                "reason": rebuild.get("reason"),
                "rebuild_started_ts": start_ts,
                "window_end_ts": end_ts,
                "window_truncated_by_next_rebuild": end_ts is not None,
                "self_join_seconds": (
                    self_join.get("ts_unix") - start_ts if self_join else None
                ),
                "available_users_joined_seconds": (
                    available.get("ts_unix") - start_ts if available else None
                ),
                "all_users_joined_seconds": (
                    complete.get("ts_unix") - start_ts if complete else None
                ),
                "metadata_available_seconds": (
                    metadata_available.get("ts_unix") - start_ts
                    if metadata_available
                    else None
                ),
                "metadata_ready_seconds": (
                    metadata_ready.get("ts_unix") - start_ts if metadata_ready else None
                ),
                "expected_count": expected_count,
                "available_target_count": available_target_count,
                "max_joined_count": (
                    max_joined.get("joined_count") if max_joined else None
                ),
                "max_joined_seconds": (
                    max_joined.get("ts_unix") - start_ts if max_joined else None
                ),
                "max_joined_event": max_joined.get("event") if max_joined else None,
            }
        )
    return summaries


def main():
    args = parse_args()
    paths = expand_paths(args.events)
    events = read_events(paths)
    leader_changes = [
        event for event in events if event.get("event") == "leader_change_observed"
    ]
    summary = {
        "event_file_count": len(paths),
        "event_count": len(events),
        "leader_change_count": len(leader_changes),
        "leader_changes": leader_changes,
        "routing_tree_rebuilds": summarize_rebuilds(
            events,
            allowed_missing_users=args.allowed_missing_users,
        ),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
