#!/usr/bin/env python3
import argparse
import csv
import glob
import json
from collections import defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize benchmark worker JSONL events into per-attempt metrics."
    )
    parser.add_argument("--benchmark", default="benchmarking/benchmark.jsonl")
    parser.add_argument(
        "--events",
        nargs="+",
        default=["benchmarking/events/*.jsonl"],
        help="Worker event JSONL files or glob patterns.",
    )
    parser.add_argument(
        "--dispatch-events",
        default="benchmarking/dispatch_events.jsonl",
        help="Optional benchmark-engine dispatch JSONL.",
    )
    parser.add_argument("--output-jsonl", default="benchmarking/attempt_metrics.jsonl")
    parser.add_argument("--output-csv", default="benchmarking/attempt_metrics.csv")
    parser.add_argument("--summary-json", default="benchmarking/summary.json")
    return parser.parse_args()


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def expand_event_paths(patterns: list[str]):
    paths = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            paths.extend(Path(match) for match in matches)
        else:
            paths.append(Path(pattern))
    return sorted(set(paths))


def load_benchmark(path: Path):
    return {
        row["query_id"]: row
        for row in read_jsonl(path)
    }


def load_events(paths: list[Path]):
    events_by_query = defaultdict(list)
    for path in paths:
        for event in read_jsonl(path):
            query_id = event.get("query_id")
            if query_id:
                event["_event_file"] = str(path)
                events_by_query[query_id].append(event)
    for query_events in events_by_query.values():
        query_events.sort(key=lambda item: item.get("ts_monotonic", item.get("ts_unix", 0.0)))
    return events_by_query


def event_time(event):
    if event is None:
        return None
    return event.get("ts_monotonic") or event.get("ts_unix")


def final_event(events):
    finals = [
        event
        for event in events
        if event.get("event") in {"final_answer", "final_answer_failed"}
    ]
    return finals[-1] if finals else None


def query_started_event(events):
    starts = [
        event
        for event in events
        if event.get("event") in {"query_started", "dispatch_started"}
    ]
    return starts[0] if starts else None


def summarize_query(row: dict, events: list[dict]):
    route_events = [event for event in events if event.get("event") == "route_received"]
    pageindex_started = [
        event for event in events if event.get("event") == "pageindex_retrieval_started"
    ]
    evidence_received = [
        event for event in events if event.get("event") == "evidence_received"
    ]
    final = final_event(events)
    started = query_started_event(events)
    start_time = event_time(started) or (event_time(route_events[0]) if route_events else None)
    final_time = event_time(final)
    nodes_contacted = sorted({event.get("worker_id") for event in route_events if event.get("worker_id")})
    hops = [
        event.get("curr_hop")
        for event in route_events
        if isinstance(event.get("curr_hop"), int)
    ]
    evidence_count = 0
    if evidence_received:
        evidence_count = max(event.get("total_evidence_count", 0) for event in evidence_received)
    answer = final.get("answer", "") if final else ""
    not_found = bool(final.get("not_found")) if final else False
    final_failed = final is not None and final.get("event") == "final_answer_failed"
    source_worker = row.get("source_worker", "")
    return {
        "query_id": row["query_id"],
        "logical_query_id": row.get("logical_query_id", row["query_id"]),
        "base_query_id": row.get("base_query_id", ""),
        "mode": row.get("mode", ""),
        "attempt_index": row.get("attempt_index", 1),
        "attempt_count": row.get("attempt_count", 1),
        "initiator_worker": row.get("initiator_worker", ""),
        "source_worker": source_worker,
        "source_pdf": row.get("source_pdf", ""),
        "topic": row.get("topic", ""),
        "started": started is not None,
        "finalized": final is not None,
        "latency_seconds": (
            final_time - start_time
            if start_time is not None and final_time is not None
            else None
        ),
        "nodes_contacted": len(nodes_contacted),
        "nodes_contacted_list": nodes_contacted,
        "max_hop": max(hops) if hops else None,
        "source_reached": source_worker in nodes_contacted,
        "pageindex_calls": len(pageindex_started),
        "evidence_count": evidence_count,
        "retrieval_success": evidence_count > 0,
        "not_found": not_found,
        "final_failed": final_failed,
        "answer": answer,
    }


def write_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "query_id",
        "logical_query_id",
        "mode",
        "attempt_index",
        "initiator_worker",
        "source_worker",
        "topic",
        "finalized",
        "latency_seconds",
        "nodes_contacted",
        "max_hop",
        "source_reached",
        "pageindex_calls",
        "evidence_count",
        "retrieval_success",
        "not_found",
        "final_failed",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def aggregate(rows: list[dict]):
    by_logical = defaultdict(list)
    by_mode = defaultdict(list)
    for row in rows:
        by_logical[row["logical_query_id"]].append(row)
        by_mode[row["mode"]].append(row)

    best_of_k = []
    for logical_query_id, attempts in by_logical.items():
        successful = [row for row in attempts if row["retrieval_success"]]
        latencies = [
            row["latency_seconds"]
            for row in successful
            if row["latency_seconds"] is not None
        ]
        best_of_k.append(
            {
                "logical_query_id": logical_query_id,
                "mode": attempts[0]["mode"],
                "attempts": len(attempts),
                "best_of_k_success": bool(successful),
                "success_count": len(successful),
                "best_latency_seconds": min(latencies) if latencies else None,
            }
        )

    mode_summary = {}
    for mode, mode_rows in by_mode.items():
        finalized = [row for row in mode_rows if row["finalized"]]
        successes = [row for row in mode_rows if row["retrieval_success"]]
        latencies = [
            row["latency_seconds"]
            for row in finalized
            if row["latency_seconds"] is not None
        ]
        mode_logical = [
            row for row in best_of_k if row["mode"] == mode
        ]
        mode_summary[mode] = {
            "attempt_rows": len(mode_rows),
            "finalized_rows": len(finalized),
            "retrieval_success_rate": len(successes) / len(mode_rows) if mode_rows else 0.0,
            "best_of_k_success_rate": (
                sum(1 for row in mode_logical if row["best_of_k_success"]) / len(mode_logical)
                if mode_logical else 0.0
            ),
            "avg_nodes_contacted": (
                sum(row["nodes_contacted"] for row in mode_rows) / len(mode_rows)
                if mode_rows else 0.0
            ),
            "avg_pageindex_calls": (
                sum(row["pageindex_calls"] for row in mode_rows) / len(mode_rows)
                if mode_rows else 0.0
            ),
            "avg_latency_seconds": (
                sum(latencies) / len(latencies)
                if latencies else None
            ),
        }

    return {
        "modes": mode_summary,
        "logical_queries": len(by_logical),
        "attempt_rows": len(rows),
    }


def main():
    args = parse_args()
    benchmark = load_benchmark(Path(args.benchmark))
    event_paths = expand_event_paths(args.events)
    dispatch_path = Path(args.dispatch_events)
    if dispatch_path.exists():
        event_paths.append(dispatch_path)
    events_by_query = load_events(event_paths)
    rows = [
        summarize_query(row, events_by_query.get(query_id, []))
        for query_id, row in benchmark.items()
    ]
    write_jsonl(Path(args.output_jsonl), rows)
    write_csv(Path(args.output_csv), rows)
    summary = aggregate(rows)
    Path(args.summary_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_json).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
