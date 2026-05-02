import argparse
import json
import os
from pathlib import Path

import numpy as np

from rag.bert_embedder import BertEmbedder, DEFAULT_BERT_MODEL, cosine_similarity
from rag.document_store import read_pdf_text


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compare a whole-document embedding against the average embedding of "
            "all PageIndex tree nodes."
        )
    )
    parser.add_argument("--pdf", required=True, help="Path to the source PDF.")
    parser.add_argument(
        "--pageindex-json",
        default=None,
        help="Path to PageIndex structure JSON for the same PDF.",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Path to .pageindex_manifest.json. If --pageindex-json is omitted, "
            "the script fetches the tree from the PageIndex API using this manifest. "
            "Defaults to <pdf parent>/.pageindex_manifest.json."
        ),
    )
    parser.add_argument(
        "--doc-key",
        default=None,
        help=(
            "Manifest key for the PDF. Defaults to the PDF path relative to the "
            "manifest directory, then falls back to the PDF basename."
        ),
    )
    parser.add_argument("--model", default=DEFAULT_BERT_MODEL)
    parser.add_argument("--device", default=None)
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument(
        "--query",
        default=None,
        help="Optional query to compare against both document embeddings.",
    )
    parser.add_argument(
        "--whole-doc-mode",
        choices=["truncated", "chunk-average"],
        default="chunk-average",
        help=(
            "truncated embeds the first max-length tokens only; chunk-average "
            "splits text into character chunks and averages embeddings."
        ),
    )
    parser.add_argument("--chunk-chars", type=int, default=3000)
    return parser.parse_args()


def pageindex_api_key():
    return os.getenv("PAGE_INDEX_API_KEY", os.getenv("PAGEINDEX_API_KEY", ""))


def infer_manifest_key(pdf_path: Path, manifest_path: Path, manifest: dict, doc_key: str | None):
    if doc_key:
        return doc_key

    try:
        relative_key = pdf_path.resolve().relative_to(manifest_path.parent.resolve()).as_posix()
        if relative_key in manifest:
            return relative_key
    except ValueError:
        pass

    if pdf_path.name in manifest:
        return pdf_path.name

    matches = [
        key
        for key in manifest
        if Path(key).name == pdf_path.name
    ]
    if len(matches) == 1:
        return matches[0]

    choices = ", ".join(sorted(manifest))
    raise KeyError(
        f"Could not infer manifest key for {pdf_path}. "
        f"Pass --doc-key explicitly. Manifest keys: {choices}"
    )


def fetch_pageindex_tree_from_manifest(pdf_path: Path, manifest_arg: str | None, doc_key_arg: str | None):
    manifest_path = Path(manifest_arg) if manifest_arg else pdf_path.parent / ".pageindex_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    doc_key = infer_manifest_key(pdf_path, manifest_path, manifest, doc_key_arg)
    doc_id = manifest[doc_key].get("doc_id")
    if not doc_id:
        raise KeyError(f"Manifest entry {doc_key!r} does not contain doc_id")

    api_key = pageindex_api_key()
    if not api_key:
        raise RuntimeError("PAGE_INDEX_API_KEY or PAGEINDEX_API_KEY is required")

    from pageindex import PageIndexClient

    client = PageIndexClient(api_key=api_key)
    tree_result = client.get_tree(doc_id, node_summary=True)
    if tree_result.get("status") != "completed":
        raise RuntimeError(f"PageIndex tree is not completed for doc_id={doc_id}: {tree_result}")

    return {
        "structure": tree_result.get("result") or [],
        "manifest_path": str(manifest_path),
        "manifest_key": doc_key,
        "doc_id": doc_id,
    }


def iter_pageindex_nodes(value):
    if isinstance(value, list):
        for item in value:
            yield from iter_pageindex_nodes(item)
    elif isinstance(value, dict):
        if "title" in value or "text" in value or "summary" in value:
            yield value
        for child in value.get("nodes", []) or []:
            yield from iter_pageindex_nodes(child)


def pageindex_node_text(node):
    parts = []
    title = node.get("title") or node.get("name")
    summary = node.get("summary") or node.get("description") or node.get("node_summary")
    text = node.get("text") or node.get("content")
    if title:
        parts.append(f"Title: {title}")
    if summary:
        parts.append(f"Summary: {summary}")
    if text and text != summary:
        parts.append(f"Text: {text}")
    return "\n\n".join(parts).strip()


def chunk_text(text, chunk_chars):
    text = text.strip()
    return [
        text[index:index + chunk_chars]
        for index in range(0, len(text), chunk_chars)
        if text[index:index + chunk_chars].strip()
    ]


def average_embeddings(embeddings):
    matrix = np.asarray(embeddings, dtype=np.float64)
    return matrix.mean(axis=0).astype(float).tolist()


def embed_texts_average(embedder, texts, max_length):
    embeddings = [
        embedder.embed_text(text, max_length=max_length).embedding
        for text in texts
        if text.strip()
    ]
    if not embeddings:
        raise ValueError("No non-empty texts to embed")
    return average_embeddings(embeddings)


def main():
    args = parse_args()
    embedder = BertEmbedder(model_name=args.model, device=args.device)

    pdf_text = read_pdf_text(Path(args.pdf))
    if args.whole_doc_mode == "truncated":
        whole_embedding = embedder.embed_text(
            pdf_text,
            max_length=args.max_length,
        ).embedding
        whole_units = 1
    else:
        chunks = chunk_text(pdf_text, args.chunk_chars)
        whole_embedding = embed_texts_average(
            embedder,
            chunks,
            max_length=args.max_length,
        )
        whole_units = len(chunks)

    pageindex_source = {
        "source": "json",
        "pageindex_json": args.pageindex_json,
        "manifest_path": None,
        "manifest_key": None,
        "doc_id": None,
    }
    if args.pageindex_json:
        structure = json.loads(Path(args.pageindex_json).read_text(encoding="utf-8"))
    else:
        fetched = fetch_pageindex_tree_from_manifest(
            Path(args.pdf),
            args.manifest,
            args.doc_key,
        )
        structure = fetched["structure"]
        pageindex_source = {
            "source": "pageindex_api",
            "pageindex_json": None,
            "manifest_path": fetched["manifest_path"],
            "manifest_key": fetched["manifest_key"],
            "doc_id": fetched["doc_id"],
        }
    node_texts = [
        text
        for text in (pageindex_node_text(node) for node in iter_pageindex_nodes(structure))
        if text
    ]
    pageindex_embedding = embed_texts_average(
        embedder,
        node_texts,
        max_length=args.max_length,
    )

    score = cosine_similarity(whole_embedding, pageindex_embedding)
    query_scores = {}
    if args.query:
        query_embedding = embedder.embed_text(
            args.query,
            max_length=args.max_length,
        ).embedding
        query_scores = {
            "query": args.query,
            "query_to_whole_doc_cosine": cosine_similarity(query_embedding, whole_embedding),
            "query_to_pageindex_avg_cosine": cosine_similarity(query_embedding, pageindex_embedding),
        }

    print(json.dumps(
        {
            "pdf": args.pdf,
            **pageindex_source,
            "model": embedder.model_name,
            "device": embedder.device,
            "max_length": args.max_length,
            "whole_doc_mode": args.whole_doc_mode,
            "whole_doc_units": whole_units,
            "pageindex_node_count": len(node_texts),
            "cosine_similarity": score,
            **query_scores,
        },
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
