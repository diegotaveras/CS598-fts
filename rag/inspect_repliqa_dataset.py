import argparse
import json
from pathlib import Path
from pprint import pformat


DEFAULT_DATASET = "ServiceNow/repliqa"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load and inspect the ServiceNow/RepliQA Hugging Face dataset."
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help=f"Hugging Face dataset id. Default: {DEFAULT_DATASET}",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Optional dataset config/name. If omitted, uses the dataset default.",
    )
    parser.add_argument(
        "--split",
        default=None,
        help="Optional split to load. If omitted, loads all available splits.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="Number of examples to print per loaded split.",
    )
    parser.add_argument(
        "--text-max-chars",
        type=int,
        default=800,
        help="Maximum characters to print for long string fields.",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        help="Use Hugging Face streaming mode instead of downloading the full dataset.",
    )
    parser.add_argument(
        "--save-jsonl",
        default=None,
        help="Optional path to save inspected examples as JSONL.",
    )
    parser.add_argument(
        "--trust-remote-code",
        action="store_true",
        help="Pass trust_remote_code=True to Hugging Face datasets APIs.",
    )
    return parser.parse_args()


def import_datasets():
    try:
        import datasets
    except ImportError as exc:
        raise RuntimeError(
            "This script requires the Hugging Face datasets package. "
            "Install it with: python -m pip install datasets"
        ) from exc
    return datasets


def compact_value(value, max_chars: int):
    if isinstance(value, str):
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        if len(value) > max_chars:
            return value[:max_chars] + "...<truncated>"
        return value
    if isinstance(value, list):
        return [compact_value(item, max_chars) for item in value[:10]]
    if isinstance(value, dict):
        return {
            key: compact_value(item, max_chars)
            for key, item in value.items()
        }
    return value


def compact_example(example, max_chars: int):
    return {
        key: compact_value(value, max_chars)
        for key, value in example.items()
    }


def dataset_kwargs(args):
    kwargs = {}
    if args.config:
        kwargs["name"] = args.config
    if args.trust_remote_code:
        kwargs["trust_remote_code"] = True
    return kwargs


def print_configs(datasets, args):
    try:
        configs = datasets.get_dataset_config_names(
            args.dataset,
            trust_remote_code=args.trust_remote_code,
        )
    except Exception as exc:
        print(f"Could not fetch dataset configs: {exc}")
        return

    print("\nConfigs:")
    for config in configs:
        print(f"  - {config}")


def print_builder_info(datasets, args):
    try:
        builder = datasets.load_dataset_builder(
            args.dataset,
            **dataset_kwargs(args),
        )
    except Exception as exc:
        print(f"\nCould not load dataset builder metadata: {exc}")
        return

    info = builder.info
    print("\nBuilder Info:")
    print(f"  dataset_name: {info.dataset_name}")
    print(f"  config_name: {builder.config.name}")
    print(f"  version: {info.version}")
    print(f"  description: {compact_value(info.description or '', args.text_max_chars)}")
    print(f"  homepage: {info.homepage}")
    print(f"  license: {info.license}")
    print(f"  citation: {compact_value(info.citation or '', args.text_max_chars)}")

    if info.features:
        print("\nFeatures:")
        print(pformat(info.features, sort_dicts=False))

    if info.splits:
        print("\nSplits:")
        for split_name, split_info in info.splits.items():
            print(f"  - {split_name}: num_examples={split_info.num_examples}")


def loaded_splits(dataset):
    if isinstance(dataset, dict):
        return dataset.items()
    return [("stream" if not hasattr(dataset, "split") else str(dataset.split), dataset)]


def iter_examples(split_dataset, limit: int):
    iterator = iter(split_dataset)
    for index in range(limit):
        try:
            yield index, next(iterator)
        except StopIteration:
            return


def save_jsonl(path: str, records: list[dict]):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"\nSaved {len(records)} inspected examples to {output_path}")


def main():
    args = parse_args()
    datasets = import_datasets()

    print(f"Dataset: {args.dataset}")
    print(f"Config: {args.config or '<default>'}")
    print(f"Split: {args.split or '<all>'}")
    print(f"Streaming: {args.streaming}")

    print_configs(datasets, args)
    if not args.streaming:
        print_builder_info(datasets, args)

    load_kwargs = dataset_kwargs(args)
    if args.split:
        load_kwargs["split"] = args.split
    if args.streaming:
        load_kwargs["streaming"] = True

    dataset = datasets.load_dataset(
        args.dataset,
        **load_kwargs,
    )

    inspected_records = []
    print("\nExamples:")
    for split_name, split_dataset in loaded_splits(dataset):
        print(f"\n[{split_name}]")
        features = getattr(split_dataset, "features", None)
        if features:
            print("features:")
            print(pformat(features, sort_dicts=False))
        try:
            length = len(split_dataset)
        except TypeError:
            length = None
        if length is not None:
            print(f"num_examples: {length}")

        for index, example in iter_examples(split_dataset, args.limit):
            compacted = compact_example(example, args.text_max_chars)
            print(f"\nexample {index}:")
            print(json.dumps(compacted, indent=2, sort_keys=True, ensure_ascii=False))
            inspected_records.append(
                {
                    "split": split_name,
                    "index": index,
                    "example": compacted,
                }
            )

    if args.save_jsonl:
        save_jsonl(args.save_jsonl, inspected_records)


if __name__ == "__main__":
    main()
