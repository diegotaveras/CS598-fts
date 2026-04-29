import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from rag.bert_embedder import cosine_similarity

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def read_pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError(
            "PDF input requires pypdf. Install dependencies with: pip install -r requirements.txt"
        ) from exc

    reader = PdfReader(str(path))
    pages = []
    for index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        if text.strip():
            pages.append(f"# Page {index}\n\n{text.strip()}")

    if not pages:
        raise RuntimeError(f"No extractable text found in PDF: {path}")

    return "\n\n".join(pages)


def read_document_text(path: Path, encoding: str) -> str:
    if path.suffix.lower() == ".pdf":
        return read_pdf_text(path)
    return path.read_text(encoding=encoding)

@dataclass
class PageIndexNode:
    node_id: str
    title: str
    description: str
    content: str
    metadata: dict
    parent_id: str | None = None
    child_ids: list[str] = field(default_factory=list)
    embedding: list[float] | None = None

    def preview(self, max_chars: int = 500) -> str:
        text = re.sub(r"\s+", " ", self.content or self.description).strip()
        return text[:max_chars]

    def metadata_json(self) -> str:
        return json.dumps(self.metadata, sort_keys=True)


class LocalDocumentStore:
    def __init__(self, worker_id: str, doc_dir: str, encoding: str = "utf-8"):
        self.worker_id = worker_id
        self.doc_dir = Path(doc_dir)
        self.encoding = encoding
        self.pageindex_api_key = os.getenv(
            "PAGE_INDEX_API_KEY",
            os.getenv("PAGEINDEX_API_KEY", ""),
        )
        self.pageindex_wait = os.getenv("RAG_PAGEINDEX_WAIT", "1") == "1"
        self.pageindex_timeout_seconds = int(os.getenv("RAG_PAGEINDEX_TIMEOUT_SECONDS", "600"))
        self.pageindex_poll_seconds = int(os.getenv("RAG_PAGEINDEX_POLL_SECONDS", "10"))
        self.manifest_path = self.doc_dir / ".pageindex_manifest.json"
        self.nodes: dict[str, PageIndexNode] = {}

    def load(self):
        self.nodes.clear()

        if not self.doc_dir.exists():
            self.doc_dir.mkdir(parents=True, exist_ok=True)
            return

        paths = [
            path
            for path in sorted(self.doc_dir.rglob("*"))
            if path.is_file() and path.suffix.lower() in {".md", ".txt", ".pdf"}
        ]

        manifest = self._load_manifest()
        for path in paths:
            loaded = self._load_pageindex_tree(path, manifest)
            if not loaded:
                self._load_file_node(path)
        self._save_manifest(manifest)

    def _load_file_node(self, path: Path):
        text = read_document_text(path, self.encoding)
        doc_key = path.relative_to(self.doc_dir).as_posix()
        self._add_node(
            PageIndexNode(
                node_id=f"{doc_key}:file",
                title=doc_key,
                description=self._file_description(doc_key, text),
                content=text,
                metadata={
                    "source_path": doc_key,
                    "node_type": "file",
                    "suffix": path.suffix.lower(),
                },
                parent_id=None,
                child_ids=[],
            )
        )

    def _file_description(self, doc_key: str, text: str) -> str:
        preview = re.sub(r"\s+", " ", text).strip()[:300]
        if preview:
            return f"File node for {doc_key}. Preview: {preview}"
        return f"File node for {doc_key}."

    def _add_node(self, node: PageIndexNode):
        self.nodes[node.node_id] = node

    def root_pageindex_nodes(self) -> list[PageIndexNode]:
        return [
            node
            for node in self.nodes.values()
            if node.parent_id is None
            and node.metadata.get("node_type") == "pageindex_document_root"
        ]

    def embed_root_pageindex_nodes(self, embedder, max_length: int = 512):
        roots = self.root_pageindex_nodes()
        for node in roots:
            text = self._embedding_text_for_root(node)
            result = embedder.embed_text(text, max_length=max_length)
            node.embedding = result.embedding
            node.metadata["embedding_model"] = result.model_name
            node.metadata["embedding_dimension"] = len(result.embedding)
            print(
                f"[rag-store {self.worker_id}] embedded PageIndex root "
                f"node_id={node.node_id} dimension={len(result.embedding)}",
                flush=True,
            )
        return roots

    def _embedding_text_for_root(self, node: PageIndexNode) -> str:
        parts = [
            f"Title: {node.title}",
            f"Description: {node.description}",
        ]
        for child_id in node.child_ids:
            child = self.nodes.get(child_id)
            if child is None:
                continue
            parts.append(
                f"Child: {child.title}\nSummary: {child.description}"
            )
        return "\n\n".join(parts)

    def _load_manifest(self):
        if not self.manifest_path.exists():
            return {}
        try:
            return json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_manifest(self, manifest):
        self.doc_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _load_pageindex_tree(self, path: Path, manifest) -> bool:
        if not self.pageindex_api_key:
            print(
                f"[rag-store {self.worker_id}] PAGE_INDEX_API_KEY is missing; "
                f"using local file node for {path}",
                flush=True,
            )
            return False

        try:
            from pageindex import PageIndexClient
        except ImportError:
            print(
                f"[rag-store {self.worker_id}] pageindex SDK is not installed; "
                f"using local file node for {path}",
                flush=True,
            )
            return False

        doc_key = path.relative_to(self.doc_dir).as_posix()
        manifest_entry = manifest.get(doc_key, {})

        client = PageIndexClient(api_key=self.pageindex_api_key)
        doc_id = manifest_entry.get("doc_id")
        try:
            if not doc_id:
                print(f"[rag-store {self.worker_id}] submitting {doc_key} to PageIndex", flush=True)
                result = client.submit_document(str(path))
                doc_id = result["doc_id"]
                manifest[doc_key] = {
                    "doc_id": doc_id,
                }

            tree_result = client.get_tree(doc_id, node_summary=True)
        except Exception as exc:
            print(
                f"[rag-store {self.worker_id}] PageIndex failed for {doc_key}: {exc}; "
                f"using local file node",
                flush=True,
            )
            return False
        if tree_result.get("status") != "completed":
            if not self.pageindex_wait:
                print(
                    f"[rag-store {self.worker_id}] PageIndex doc_id={doc_id} "
                    f"status={tree_result.get('status')}; using local file node for now",
                    flush=True,
                )
                return False
            tree_result = self._wait_for_pageindex_tree(client, doc_id)

        raw_nodes = tree_result.get("result") or []
        if isinstance(raw_nodes, dict):
            raw_nodes = [raw_nodes]
        if not raw_nodes:
            print(
                f"[rag-store {self.worker_id}] PageIndex returned no tree nodes for {doc_key}; "
                f"using local file node",
                flush=True,
            )
            return False

        root_id = f"{doc_key}:pageindex-root"
        child_ids = [self._pageindex_node_id(doc_key, raw_node) for raw_node in raw_nodes]
        self._add_node(
            PageIndexNode(
                node_id=root_id,
                title=doc_key,
                description=f"PageIndex document root for {doc_key}.",
                content="",
                metadata={
                    "source_path": doc_key,
                    "node_type": "pageindex_document_root",
                    "doc_id": doc_id,
                    "suffix": path.suffix.lower(),
                },
                parent_id=None,
                child_ids=child_ids,
            )
        )
        self._print_head_node(self.nodes[root_id])
        for raw_node in raw_nodes:
            self._add_pageindex_node(doc_key, doc_id, raw_node, parent_id=root_id)

        print(
            f"[rag-store {self.worker_id}] loaded PageIndex tree for {doc_key} doc_id={doc_id}",
            flush=True,
        )
        return True

    def _print_head_node(self, node: PageIndexNode):
        print(
            f"[rag-store {self.worker_id}] PageIndex head node "
            f"node_id={node.node_id} title={node.title} "
            f"children={len(node.child_ids)} metadata={node.metadata_json()}",
            flush=True,
        )

    def _wait_for_pageindex_tree(self, client, doc_id):
        deadline = time.monotonic() + self.pageindex_timeout_seconds
        last_result = None

        while time.monotonic() < deadline:
            tree_result = client.get_tree(doc_id, node_summary=True)
            last_result = tree_result
            status = tree_result.get("status")
            print(
                f"[rag-store {self.worker_id}] PageIndex doc_id={doc_id} status={status}",
                flush=True,
            )
            if status == "completed":
                return tree_result
            if status in {"failed", "error"}:
                raise RuntimeError(f"PageIndex processing failed: {tree_result}")
            time.sleep(self.pageindex_poll_seconds)

        raise TimeoutError(f"Timed out waiting for PageIndex doc_id={doc_id}: {last_result}")

    def _pageindex_node_id(self, doc_key: str, raw_node: dict) -> str:
        raw_id = (
            raw_node.get("node_id")
            or raw_node.get("id")
            or raw_node.get("name")
            or raw_node.get("title")
            or "node"
        )
        return f"{doc_key}:pi:{raw_id}"

    def _pageindex_children(self, raw_node: dict) -> list[dict]:
        children = (
            raw_node.get("nodes")
            or raw_node.get("children")
            or raw_node.get("sub_nodes")
            or raw_node.get("subnodes")
            or []
        )
        return children if isinstance(children, list) else []

    def _add_pageindex_node(self, doc_key: str, doc_id: str, raw_node: dict, parent_id: str):
        node_id = self._pageindex_node_id(doc_key, raw_node)
        children = self._pageindex_children(raw_node)
        child_ids = [self._pageindex_node_id(doc_key, child) for child in children]
        content = raw_node.get("text") or raw_node.get("content") or ""
        description = (
            raw_node.get("summary")
            or raw_node.get("description")
            or raw_node.get("node_summary")
            or content[:300]
        )
        raw_metadata = raw_node.get("metadata") if isinstance(raw_node.get("metadata"), dict) else {}
        metadata = {
            **raw_metadata,
            "source_path": doc_key,
            "node_type": "pageindex_node",
            "doc_id": doc_id,
            "raw_node_id": raw_node.get("node_id") or raw_node.get("id"),
        }
        self._add_node(
            PageIndexNode(
                node_id=node_id,
                title=raw_node.get("title") or raw_node.get("name") or node_id,
                description=description,
                content=content,
                metadata=metadata,
                parent_id=parent_id,
                child_ids=child_ids,
            )
        )
        for child in children:
            self._add_pageindex_node(doc_key, doc_id, child, parent_id=node_id)

    def route(self, query: str, top_k: int, embedder=None, max_length: int = 512):
        if embedder is None:
            nodes = sorted(self.nodes.values(), key=lambda node: node.node_id)
            return [(1.0, node) for node in nodes[:top_k]]

        query_embedding = embedder.embed_text(query, max_length=max_length).embedding
        return self._find_closest_nodes(query_embedding, top_k)

    def _find_closest_nodes(self, query_embedding: list[float], top_k: int):
        scored = []
        for node in self.root_pageindex_nodes():
            if node.embedding is None:
                continue
            score = cosine_similarity(query_embedding, node.embedding)
            scored.append((score, node))

        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[:top_k]

    def get_nodes(self, node_ids: list[str]):
        return [
            self.nodes[node_id]
            for node_id in node_ids
            if node_id in self.nodes
        ]
