#!/usr/bin/env python3
import argparse
import json
import random
from pathlib import Path


DEFAULT_MODES = ["chain_hop"]


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a benchmark JSONL file from RepliQA topic rows and the "
            "PDF-to-worker distribution plan."
        )
    )
    parser.add_argument(
        "--pdf-root",
        default="pdfs",
        help="Root folder containing topic subfolders with rows.jsonl files.",
    )
    parser.add_argument(
        "--distribution-plan",
        default="deploy/pdf_distribution_plan.json",
        help="Path to deploy/scp_pdfs_to_vm_nodes.py distribution plan JSON.",
    )
    parser.add_argument(
        "--output",
        default="benchmarking/benchmark.jsonl",
        help="Output benchmark JSONL path.",
    )
    parser.add_argument(
        "--modes",
        default=",".join(DEFAULT_MODES),
        help="Comma-separated routing modes to expand per question.",
    )
    parser.add_argument(
        "--initiators",
        default="node1",
        help=(
            "Comma-separated initiator workers. Use 'all' for every worker in "
            "the distribution plan, 'source' to use the source worker, or "
            "'random' to choose one worker uniformly per logical row."
        ),
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=598,
        help="Seed used when --initiators includes random.",
    )
    parser.add_argument(
        "--topics",
        default="",
        help=(
            "Optional comma-separated topic folder names or document_topic values "
            "to include."
        ),
    )
    parser.add_argument(
        "--limit-questions",
        type=int,
        default=0,
        help="Optional max number of source questions before mode/initiator expansion.",
    )
    parser.add_argument(
        "--questions-per-pdf",
        type=int,
        default=3,
        help=(
            "Maximum questions to keep per source PDF before expansion. "
            "Use 0 to keep all questions."
        ),
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=1,
        help=(
            "Number of repeated attempts per logical question/mode/initiator row. "
            "Use this for best-of-k evaluation."
        ),
    )
    return parser.parse_args()


def load_distribution_plan(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Expected a list in distribution plan: {path}")

    by_doc_id = {}
    workers = []
    for item in data:
        worker_id = item["node_id"]
        workers.append(worker_id)
        for pdf in item.get("pdfs", []):
            pdf_path = Path(pdf["path"])
            doc_id = pdf_path.stem
            by_doc_id[doc_id] = {
                "source_worker": worker_id,
                "source_vm": item.get("vm_id", ""),
                "source_addr": item.get("addr", ""),
                "remote_node_id": item.get("remote_node_id", worker_id),
                "remote_dir": item.get("remote_dir", ""),
                "source_pdf": pdf["path"],
                "source_pdf_name": pdf_path.name,
                "topic_folder": pdf.get("category", ""),
            }
    return by_doc_id, sorted(set(workers), key=worker_sort_key)


def worker_sort_key(worker_id: str):
    if worker_id.startswith("node") and worker_id[4:].isdigit():
        return int(worker_id[4:])
    return worker_id


def normalize_topic(value: str):
    return value.strip().lower().replace(" ", "_")


def iter_repliqa_rows(pdf_root: Path, topics: set[str]):
    for rows_path in sorted(pdf_root.glob("*/rows.jsonl")):
        topic_folder = rows_path.parent.name
        if topics and normalize_topic(topic_folder) not in topics:
            continue
        with rows_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                outer = json.loads(line)
                row = dict(outer.get("row") or {})
                document_topic = row.get("document_topic", "")
                if topics and normalize_topic(document_topic) not in topics:
                    continue
                row["_rows_path"] = str(rows_path)
                row["_rows_line_number"] = line_number
                row["_dataset_split"] = outer.get("dataset_split", "")
                row["_row_index"] = outer.get("row_index", None)
                row["_topic_folder"] = topic_folder
                yield row


def local_source_pdf(row: dict):
    doc_id = row.get("document_id") or Path(str(row.get("document_path", ""))).stem
    dataset_split = row.get("_dataset_split") or Path(str(row.get("document_path", ""))).parent.name
    topic_folder = row["_topic_folder"]
    return Path("pdfs") / topic_folder / dataset_split / f"{doc_id}.pdf"


def build_question_records(rows, plan_by_doc_id, questions_per_pdf: int):
    records = []
    missing_docs = []
    kept_by_doc_id = {}
    for row in rows:
        doc_id = row.get("document_id") or Path(str(row.get("document_path", ""))).stem
        plan_entry = plan_by_doc_id.get(doc_id)
        if plan_entry is None:
            missing_docs.append(doc_id)
            continue
        if questions_per_pdf > 0 and kept_by_doc_id.get(doc_id, 0) >= questions_per_pdf:
            continue

        question_id = str(row.get("question_id") or f"{doc_id}-row{row.get('_row_index')}")
        source_pdf = str(local_source_pdf(row))
        records.append(
            {
                "base_query_id": question_id,
                "question": row.get("question", ""),
                "answer": row.get("answer", ""),
                "long_answer": row.get("long_answer", ""),
                "source_pdf": source_pdf,
                "source_pdf_name": Path(source_pdf).name,
                "source_document_id": doc_id,
                "source_worker": plan_entry["source_worker"],
                "source_vm": plan_entry["source_vm"],
                "source_addr": plan_entry["source_addr"],
                "source_remote_node_id": plan_entry["remote_node_id"],
                "topic": row.get("document_topic", ""),
                "topic_folder": row["_topic_folder"],
                "dataset_split": row.get("_dataset_split", ""),
                "row_index": row.get("_row_index"),
            }
        )
        kept_by_doc_id[doc_id] = kept_by_doc_id.get(doc_id, 0) + 1
    return records, missing_docs


def initiators_for_record(
    value: str,
    workers: list[str],
    source_worker: str,
    rng: random.Random,
):
    selected = parse_csv(value)
    if not selected:
        return ["node1"]
    expanded = []
    for item in selected:
        if item == "all":
            expanded.extend(workers)
        elif item == "source":
            expanded.append(source_worker)
        elif item == "random":
            expanded.append(rng.choice(workers))
        else:
            expanded.append(item)
    return list(dict.fromkeys(expanded))


def expand_records(
    question_records,
    modes: list[str],
    initiators_arg: str,
    workers: list[str],
    attempts: int,
    rng: random.Random,
):
    for record in question_records:
        for mode in modes:
            initiators = initiators_for_record(
                initiators_arg,
                workers,
                record["source_worker"],
                rng,
            )
            for initiator in initiators:
                logical_query_id = f"{record['base_query_id']}::{mode}::{initiator}"
                for attempt_index in range(1, attempts + 1):
                    out = dict(record)
                    out["mode"] = mode
                    out["initiator_worker"] = initiator
                    out["logical_query_id"] = logical_query_id
                    out["attempt_index"] = attempt_index
                    out["attempt_count"] = attempts
                    out["query_id"] = (
                        logical_query_id
                        if attempts == 1
                        else f"{logical_query_id}::attempt{attempt_index}"
                    )
                    yield out


def main():
    args = parse_args()
    pdf_root = Path(args.pdf_root)
    output_path = Path(args.output)
    topics = {normalize_topic(topic) for topic in parse_csv(args.topics)}
    modes = parse_csv(args.modes) or DEFAULT_MODES
    if args.attempts < 1:
        raise SystemExit("--attempts must be >= 1")
    if args.questions_per_pdf < 0:
        raise SystemExit("--questions-per-pdf must be >= 0")

    plan_by_doc_id, workers = load_distribution_plan(Path(args.distribution_plan))
    rng = random.Random(args.random_seed)
    rows = list(iter_repliqa_rows(pdf_root, topics))
    question_records, missing_docs = build_question_records(
        rows,
        plan_by_doc_id,
        args.questions_per_pdf,
    )
    if args.limit_questions and args.limit_questions > 0:
        question_records = question_records[: args.limit_questions]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for record in expand_records(
            question_records,
            modes,
            args.initiators,
            workers,
            args.attempts,
            rng,
        ):
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            written += 1

    print(f"wrote {written} benchmark rows to {output_path}")
    print(f"source questions: {len(question_records)}")
    print(f"questions per PDF cap: {args.questions_per_pdf or 'all'}")
    print(f"attempts per logical row: {args.attempts}")
    if "random" in parse_csv(args.initiators):
        print(f"random initiator seed: {args.random_seed}")
    print(f"workers in plan: {len(workers)}")
    if missing_docs:
        unique_missing = sorted(set(missing_docs))
        preview = ", ".join(unique_missing[:10])
        suffix = " ..." if len(unique_missing) > 10 else ""
        print(
            f"skipped {len(missing_docs)} rows with docs not in distribution plan: "
            f"{preview}{suffix}"
        )


if __name__ == "__main__":
    main()
