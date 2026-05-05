#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.inference_client import InferenceClient
from common.inference_config import InferenceConfig


JUDGE_PROMPT = """\
You are evaluating a question-answering system.

Question: {question}
Reference answer: {answer}
Reference detailed answer: {long_answer}
System answer: {generated_answer}

Does the system answer correctly address the question based on the reference answers?
Reply with a JSON object only, in this exact format: {{"correct": true, "reason": "brief reason"}}"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Grade benchmark attempt metrics using an LLM as a judge."
    )
    parser.add_argument("--metrics", default="benchmarking/attempt_metrics.jsonl")
    parser.add_argument("--benchmark", default="benchmarking/benchmark.jsonl")
    parser.add_argument("--output-jsonl", default="benchmarking/grade_results.jsonl")
    parser.add_argument("--output-csv", default="benchmarking/grade_results.csv")
    parser.add_argument("--summary-json", default="benchmarking/grade_summary.json")
    parser.add_argument("--backend", default="openrouter",
                        choices=["openrouter", "openai_compatible", "sglang"])
    parser.add_argument("--model", default="openai/gpt-4o-mini")
    parser.add_argument("--endpoint", default="https://openrouter.ai/api/v1")
    parser.add_argument("--api-key", default=None,
                        help="API key (defaults to OPENROUTER_API_KEY env var).")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--limit", type=int, default=0,
                        help="Optional cap on rows to grade (0 = all).")
    parser.add_argument("--skip-graded", action="store_true", default=True,
                        help="Skip query_ids already present in output JSONL.")
    parser.add_argument("--no-skip-graded", dest="skip_graded", action="store_false")
    return parser.parse_args()


def read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        return
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
        "llm_grade",
        "llm_grade_reason",
        "llm_grade_model",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def parse_judge_response(text: str) -> tuple[int | None, str]:
    text = text.strip()
    # Try to extract JSON from the response
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        try:
            data = json.loads(text[start:end])
            correct = data.get("correct")
            reason = str(data.get("reason", ""))
            if isinstance(correct, bool):
                return (1 if correct else 0), reason
        except json.JSONDecodeError:
            pass
    # Fallback: scan for true/false
    lower = text.lower()
    if '"correct": true' in lower or "'correct': true" in lower:
        return 1, text
    if '"correct": false' in lower or "'correct': false" in lower:
        return 0, text
    return None, text


async def grade_row(
    row: dict,
    benchmark_row: dict | None,
    client: InferenceClient,
    semaphore: asyncio.Semaphore,
    model: str,
) -> dict:
    result = dict(row)
    result["llm_grade"] = None
    result["llm_grade_reason"] = None
    result["llm_grade_model"] = model

    generated_answer = row.get("answer", "")
    if not row.get("finalized") or not generated_answer:
        return result

    if benchmark_row is None:
        return result

    question = benchmark_row.get("question", "")
    ref_answer = benchmark_row.get("answer", "")
    long_answer = benchmark_row.get("long_answer", "")

    prompt = JUDGE_PROMPT.format(
        question=question,
        answer=ref_answer,
        long_answer=long_answer,
        generated_answer=generated_answer,
    )
    messages = [{"role": "user", "content": prompt}]

    async with semaphore:
        try:
            text = await client.get_text(messages)
        except Exception as exc:
            result["llm_grade_reason"] = f"error: {exc}"
            return result

    grade, reason = parse_judge_response(text)
    result["llm_grade"] = grade
    result["llm_grade_reason"] = reason
    return result


def aggregate(rows: list[dict]) -> dict:
    by_mode = defaultdict(list)
    for row in rows:
        by_mode[row.get("mode", "")].append(row)

    mode_summary = {}
    for mode, mode_rows in by_mode.items():
        graded = [r for r in mode_rows if r.get("llm_grade") is not None]
        correct = [r for r in graded if r.get("llm_grade") == 1]
        mode_summary[mode] = {
            "attempt_rows": len(mode_rows),
            "graded_count": len(graded),
            "skipped_count": len(mode_rows) - len(graded),
            "correct_count": len(correct),
            "accuracy": len(correct) / len(graded) if graded else None,
        }

    total_graded = [r for r in rows if r.get("llm_grade") is not None]
    total_correct = [r for r in total_graded if r.get("llm_grade") == 1]
    return {
        "modes": mode_summary,
        "total_attempt_rows": len(rows),
        "total_graded": len(total_graded),
        "total_skipped": len(rows) - len(total_graded),
        "total_correct": len(total_correct),
        "overall_accuracy": (
            len(total_correct) / len(total_graded) if total_graded else None
        ),
    }


async def main_async():
    args = parse_args()
    api_key = args.api_key or os.environ.get("OPENROUTER_API_KEY")

    benchmark = {
        row["query_id"]: row
        for row in read_jsonl(Path(args.benchmark))
    }
    metrics_rows = list(read_jsonl(Path(args.metrics)))

    already_graded: set[str] = set()
    output_path = Path(args.output_jsonl)
    if args.skip_graded and output_path.exists():
        already_graded = {
            row["query_id"]
            for row in read_jsonl(output_path)
            if "query_id" in row
        }

    rows_to_grade = [
        row for row in metrics_rows
        if row.get("query_id") not in already_graded
    ]
    if args.limit > 0:
        rows_to_grade = rows_to_grade[: args.limit]

    print(f"total metrics rows: {len(metrics_rows)}")
    print(f"already graded (skipping): {len(already_graded)}")
    print(f"rows to grade: {len(rows_to_grade)}")

    config = InferenceConfig(
        backend=args.backend,
        model_name=args.model,
        endpoint=args.endpoint,
        api_key=api_key,
        temperature=0.0,
        max_tokens=args.max_tokens,
    )
    client = InferenceClient(config)
    semaphore = asyncio.Semaphore(args.concurrency)

    tasks = [
        grade_row(row, benchmark.get(row.get("query_id", "")), client, semaphore, args.model)
        for row in rows_to_grade
    ]

    new_results = []
    for index, coro in enumerate(asyncio.as_completed(tasks), start=1):
        result = await coro
        new_results.append(result)
        grade = result.get("llm_grade")
        grade_str = str(grade) if grade is not None else "skip"
        if index % 10 == 0 or index == len(tasks):
            print(f"[{index}/{len(tasks)}] last grade={grade_str}")

    # Merge with already-graded rows when resuming
    if already_graded:
        existing = list(read_jsonl(output_path))
        all_results = existing + new_results
    else:
        all_results = new_results

    write_jsonl(output_path, all_results)
    write_csv(Path(args.output_csv), all_results)
    summary = aggregate(all_results)
    summary_path = Path(args.summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
