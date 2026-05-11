import json
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_FILES = [
    SCRIPT_DIR / "user-transactions.txt",
    SCRIPT_DIR / "user-transactions2.txt",
    SCRIPT_DIR / "user-transactions3.txt",
]

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract token/finish statistics from OpenRouter transaction JSON exports."
    )
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="Optional transaction JSON files. Defaults to benchmarking/user-transactions*.txt.",
    )
    return parser.parse_args()

def walk(obj):
    if isinstance(obj, dict):
        if "generation_id" in obj:
            yield obj
        for v in obj.values():
            yield from walk(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk(item)

def load_json_file(path):
    text = Path(path).read_text(encoding="utf-8")
    return json.loads(text)

def resolve_path(path):
    if path.exists():
        return path
    script_relative = SCRIPT_DIR / path
    if script_relative.exists():
        return script_relative
    return path

args = parse_args()
files = [resolve_path(path) for path in (args.files or DEFAULT_FILES)]
records = []

for filename in files:
    data = load_json_file(filename)
    records.extend(walk(data))

native_prompt_vals = [
    r["native_tokens_prompt"]
    for r in records
    if isinstance(r.get("native_tokens_prompt"), (int, float))
]

native_completion_vals = [
    r["native_tokens_completion"]
    for r in records
    if isinstance(r.get("native_tokens_completion"), (int, float))
]

native_reasoning_vals = [
    r["native_tokens_reasoning"]
    for r in records
    if isinstance(r.get("native_tokens_reasoning"), (int, float))
]

latency_vals = [
    r["latency"]
    for r in records
    if isinstance(r.get("latency"), (int, float))
]

generation_time_vals = [
    r["generation_time"]
    for r in records
    if isinstance(r.get("generation_time"), (int, float))
]

generation_finish = [
    (r.get("generation_id"), r.get("finish_reason"))
    for r in records
    if r.get("generation_id") is not None
]

avg_native_tokens_prompt = (
    sum(native_prompt_vals) / len(native_prompt_vals)
    if native_prompt_vals else None
)

avg_native_tokens_completion = (
    sum(native_completion_vals) / len(native_completion_vals)
    if native_completion_vals else None
)

avg_native_tokens_reasoning = (
    sum(native_reasoning_vals) / len(native_reasoning_vals)
    if native_reasoning_vals else None
)

avg_latency_ms = (
    sum(latency_vals) / len(latency_vals)
    if latency_vals else None
)

avg_generation_time_ms = (
    sum(generation_time_vals) / len(generation_time_vals)
    if generation_time_vals else None
)

print("records:", len(records))
print("average native_tokens_prompt:", avg_native_tokens_prompt)
print("average native_tokens_completion:", avg_native_tokens_completion)
print("average native_tokens_reasoning:", avg_native_tokens_reasoning)
print("average latency_ms:", avg_latency_ms)
print("average latency_seconds:", avg_latency_ms / 1000 if avg_latency_ms is not None else None)
print("average generation_time_ms:", avg_generation_time_ms)
print(
    "average generation_time_seconds:",
    avg_generation_time_ms / 1000 if avg_generation_time_ms is not None else None,
)
print("generation_id, finish_reason:")

for pair in generation_finish:
    print(pair)
