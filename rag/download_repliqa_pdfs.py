import argparse
import json
import re
import time
from pathlib import Path


DEFAULT_DATASET = "ServiceNow/repliqa"
DEFAULT_OUTPUT_DIR = "pdfs"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download RepliQA PDFs from Hugging Face into a local pdfs/ folder."
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help=f"Hugging Face dataset id. Default: {DEFAULT_DATASET}",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            f"Base directory to write PDFs into. Default: {DEFAULT_OUTPUT_DIR}. "
            "When --document-topic is set, a topic subfolder is created under this directory."
        ),
    )
    parser.add_argument(
        "--split",
        action="append",
        default=[],
        help=(
            "PDF folder to include, e.g. repliqa_0. Can be passed multiple times. "
            "Defaults to all repliqa_0..repliqa_4."
        ),
    )
    parser.add_argument(
        "--dataset-split",
        action="append",
        default=[],
        help=(
            "Dataset table split to read, e.g. train or validation. Can be passed "
            "multiple times. Defaults to all dataset splits."
        ),
    )
    parser.add_argument(
        "--document-topic",
        default=None,
        help="Only keep dataset rows whose document_topic exactly matches this value.",
    )
    parser.add_argument(
        "--filter-field",
        action="append",
        default=[],
        help=(
            "Generic exact-match row filter as FIELD=VALUE. Can be passed multiple "
            "times. Example: --filter-field document_topic='Local Technology and Innovation'"
        ),
    )
    parser.add_argument(
        "--save-table-jsonl",
        default=None,
        help="Optional path to save filtered dataset table rows as JSONL.",
    )
    parser.add_argument(
        "--table-only",
        action="store_true",
        help="Save/print filtered table rows without downloading PDFs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional max number of unique PDFs to download. Uses dataset rows to "
            "discover document_path values, then stops after this many PDFs."
        ),
    )
    parser.add_argument(
        "--use-cli",
        action="store_true",
        help=(
            "Use snapshot_download include patterns instead of discovering paths "
            "from dataset rows. This ignores --limit."
        ),
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Use streaming mode when discovering document paths from dataset rows.",
    )
    parser.add_argument(
        "--trust-remote-code",
        action="store_true",
        help="Pass trust_remote_code=True to Hugging Face dataset APIs.",
    )
    parser.add_argument(
        "--download-retries",
        type=int,
        default=5,
        help="Number of attempts for each PDF download.",
    )
    return parser.parse_args()


def topic_slug(topic: str):
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", topic.strip().lower()).strip("_")
    return slug or "document_topic"


def effective_output_dir(args):
    output_dir = Path(args.output_dir)
    if args.document_topic:
        return output_dir / topic_slug(args.document_topic)
    return output_dir


def effective_table_jsonl_path(args):
    if args.save_table_jsonl:
        return Path(args.save_table_jsonl)
    if args.document_topic:
        return effective_output_dir(args) / "rows.jsonl"
    return None


def import_huggingface_hub():
    try:
        from huggingface_hub import hf_hub_download, snapshot_download
    except ImportError as exc:
        raise RuntimeError(
            "This script requires huggingface_hub. Install it with: "
            "python -m pip install huggingface_hub"
        ) from exc
    return hf_hub_download, snapshot_download


def import_datasets():
    try:
        import datasets
    except ImportError as exc:
        raise RuntimeError(
            "Discovering RepliQA PDF paths requires datasets. Install it with: "
            "python -m pip install datasets"
        ) from exc
    return datasets


def requested_splits(args):
    if args.split:
        return args.split
    return [f"repliqa_{index}" for index in range(5)]


def requested_pdf_prefixes(args):
    return [
        f"pdfs/{split}/"
        for split in requested_splits(args)
    ]


def copy_file(source: Path, destination: Path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size == source.stat().st_size:
        return False
    destination.write_bytes(source.read_bytes())
    return True


def download_with_patterns(args, snapshot_download):
    splits = requested_splits(args)
    patterns = [
        f"pdfs/{split}/*.pdf"
        for split in splits
    ]
    output_dir = effective_output_dir(args)
    print(
        f"Downloading {args.dataset} PDFs with patterns={patterns} "
        f"to {output_dir}",
        flush=True,
    )
    local_snapshot = snapshot_download(
        repo_id=args.dataset,
        repo_type="dataset",
        allow_patterns=patterns,
        local_dir=output_dir,
    )
    print(f"Downloaded snapshot files under {local_snapshot}", flush=True)


def dataset_filter_pairs(args):
    pairs = []
    if args.document_topic is not None:
        pairs.append(("document_topic", args.document_topic))
    for item in args.filter_field:
        if "=" not in item:
            raise ValueError(f"Invalid --filter-field {item!r}; expected FIELD=VALUE")
        key, value = item.split("=", 1)
        pairs.append((key, value))
    return pairs


def row_matches_filters(row: dict, filters: list[tuple[str, str]]):
    for key, expected_value in filters:
        if str(row.get(key, "")) != expected_value:
            return False
    return True


def row_matches_pdf_prefix(row: dict, pdf_prefixes: list[str]):
    path = row.get("document_path") or ""
    return any(path.startswith(prefix) for prefix in pdf_prefixes)


def load_dataset(datasets, args, split_name: str | None = None):
    kwargs = {
        "streaming": args.streaming,
    }
    if split_name:
        kwargs["split"] = split_name
    if args.trust_remote_code:
        kwargs["trust_remote_code"] = True
    return datasets.load_dataset(args.dataset, **kwargs)


def loaded_tables(dataset, fallback_name: str | None = None):
    if isinstance(dataset, dict):
        return dataset.items()
    split = getattr(dataset, "split", None)
    return [(fallback_name or str(split or "dataset"), dataset)]


def iter_filtered_rows(args):
    datasets = import_datasets()
    filters = dataset_filter_pairs(args)
    pdf_prefixes = requested_pdf_prefixes(args)
    dataset_splits = args.dataset_split or [None]
    yielded = 0

    for split_name in dataset_splits:
        print(
            f"Reading dataset table split={split_name or '<all>'} "
            f"filters={filters or '<none>'} pdf_prefixes={pdf_prefixes}",
            flush=True,
        )
        dataset = load_dataset(datasets, args, split_name)
        for loaded_split_name, split_dataset in loaded_tables(dataset, split_name):
            for row_index, row in enumerate(split_dataset):
                if not row_matches_filters(row, filters):
                    continue
                if not row_matches_pdf_prefix(row, pdf_prefixes):
                    continue
                yield {
                    "dataset_split": loaded_split_name,
                    "row_index": row_index,
                    "row": row,
                }
                yielded += 1
                if args.limit is not None and yielded >= args.limit:
                    return


def save_table_jsonl(path: str, records: list[dict]):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"Saved {len(records)} filtered table rows to {output_path}", flush=True)


def discover_pdf_paths(args):
    paths = []
    seen = set()
    records = []
    table_jsonl_path = effective_table_jsonl_path(args)
    for record in iter_filtered_rows(args):
        row = record["row"]
        if table_jsonl_path:
            records.append(record)
        path = row.get("document_path")
        if not path or not path.endswith(".pdf") or path in seen:
            continue
        seen.add(path)
        paths.append(path)
    if table_jsonl_path:
        save_table_jsonl(table_jsonl_path, records)
    return paths


def download_pdf(args, hf_hub_download, repo_path: str):
    last_error = None
    for attempt in range(1, args.download_retries + 1):
        try:
            return hf_hub_download(
                repo_id=args.dataset,
                repo_type="dataset",
                filename=repo_path,
            )
        except Exception as exc:
            last_error = exc
            if attempt == args.download_retries:
                raise
            wait_seconds = min(2 ** (attempt - 1), 30)
            print(
                f"Download failed for {repo_path}: {exc}. "
                f"Retrying in {wait_seconds}s [{attempt}/{args.download_retries}]",
                flush=True,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Failed to download {repo_path}: {last_error}")


def download_discovered_paths(args, hf_hub_download):
    output_dir = effective_output_dir(args)
    pdf_paths = discover_pdf_paths(args)
    print(f"Discovered {len(pdf_paths)} unique PDF paths", flush=True)
    if args.table_only:
        print("table-only mode selected; skipping PDF downloads", flush=True)
        return

    downloaded = 0
    reused = 0
    for index, repo_path in enumerate(pdf_paths, start=1):
        print(f"[{index}/{len(pdf_paths)}] downloading {repo_path}", flush=True)
        cached_path = download_pdf(args, hf_hub_download, repo_path)
        destination = output_dir / Path(repo_path).relative_to("pdfs")
        if copy_file(Path(cached_path), destination):
            downloaded += 1
        else:
            reused += 1

    print(
        f"Done. wrote={downloaded} reused={reused} output_dir={output_dir}",
        flush=True,
    )


def main():
    args = parse_args()
    hf_hub_download, snapshot_download = import_huggingface_hub()
    if args.use_cli:
        download_with_patterns(args, snapshot_download)
    else:
        download_discovered_paths(args, hf_hub_download)


if __name__ == "__main__":
    main()
