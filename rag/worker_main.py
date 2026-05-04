import asyncio
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import grpc
import numpy as np
from grpc_reflection.v1alpha import reflection

from rag import rag_pb2, rag_pb2_grpc
from rag.bert_embedder import BertEmbedder, DEFAULT_BERT_MODEL, cosine_similarity
from rag.document_store import LocalDocumentStore
from rag.leader_election import LeaderElection
from rag.llm_client import build_inference_client_from_env


class RoutingMode(str, Enum):
    JOIN_TREE = "join_tree"

    @classmethod
    def from_env(cls, value: str):
        normalized = value.strip().lower()
        for mode in cls:
            if mode.value == normalized:
                return mode
        valid_modes = ", ".join(mode.value for mode in cls)
        raise ValueError(f"Invalid RAG_ROUTING_MODE={value!r}; expected one of: {valid_modes}")


class BenchmarkEventLogger:
    def __init__(self, path: str):
        self.path = Path(path) if path else None
        self.lock = threading.Lock()
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields):
        if not self.path:
            return
        payload = {
            "event": event,
            "ts_unix": time.time(),
            "ts_monotonic": time.monotonic(),
            "worker_id": WORKER_ID,
            **fields,
        }
        line = json.dumps(payload, sort_keys=True)
        with self.lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")


@dataclass
class RoutingTreeNode:
    node_id: str
    kind: str
    depth: int
    parent_id: str | None = None
    custodian_worker_id: str = ""
    custodian_addr: str = ""
    member_worker_ids: list[str] = field(default_factory=list)
    left_child_id: str = ""
    right_child_id: str = ""
    left_custodian_worker_id: str = ""
    left_custodian_addr: str = ""
    right_custodian_worker_id: str = ""
    right_custodian_addr: str = ""
    centroid_left: list[float] = field(default_factory=list)
    centroid_right: list[float] = field(default_factory=list)


def _embedding_array(embedding) -> np.ndarray:
    arr = np.asarray(embedding, dtype=np.float64)
    if arr.ndim != 1:
        arr = arr.reshape(-1)
    norm = np.linalg.norm(arr)
    if norm == 0.0:
        return arr
    return arr / norm


def _two_means(embeddings: np.ndarray, max_iter: int = 25):
    if embeddings.shape[0] < 2:
        labels = np.zeros(embeddings.shape[0], dtype=int)
        return labels, np.vstack([embeddings[0], embeddings[0]])

    centroid_left = embeddings[0].copy()
    centroid_right = embeddings[int(np.argmax(np.linalg.norm(embeddings - centroid_left, axis=1)))].copy()
    labels = np.zeros(embeddings.shape[0], dtype=int)
    for _ in range(max_iter):
        left_distances = np.linalg.norm(embeddings - centroid_left, axis=1)
        right_distances = np.linalg.norm(embeddings - centroid_right, axis=1)
        next_labels = (right_distances < left_distances).astype(int)
        if np.array_equal(labels, next_labels):
            break
        labels = next_labels
        if np.any(labels == 0):
            centroid_left = embeddings[labels == 0].mean(axis=0)
        if np.any(labels == 1):
            centroid_right = embeddings[labels == 1].mean(axis=0)
    return labels, np.vstack([centroid_left, centroid_right])


WORKER_ID = os.getenv("RAG_WORKER_ID", os.getenv("HOSTNAME", "rag-worker"))
HOST = os.getenv("RAG_HOST", "0.0.0.0")
PORT = int(os.getenv("RAG_PORT", "9100"))
ADVERTISE_ADDR = os.getenv("RAG_ADVERTISE_ADDR", f"{WORKER_ID}:{PORT}")
DOC_DIR = os.getenv("RAG_DOC_DIR", "/data/rag")
BERT_MODEL = os.getenv("RAG_BERT_MODEL", DEFAULT_BERT_MODEL)
BERT_DEVICE = os.getenv("RAG_BERT_DEVICE") or None
BERT_MAX_LENGTH = int(os.getenv("RAG_BERT_MAX_LENGTH", "512"))
PAGEINDEX_QUERY_TOP_K = int(os.getenv("RAG_PAGEINDEX_QUERY_TOP_K", "3"))
QUERY_DOC_MATCH_THRESHOLD = float(
    os.getenv(
        "RAG_QUERY_DOC_MATCH_THRESHOLD",
        os.getenv("RAG_LOCAL_MATCH_THRESHOLD", "0.73"),
    )
)
LOCAL_RETRIEVAL_POLICY = os.getenv("RAG_LOCAL_RETRIEVAL_POLICY", "top1_probe").strip().lower()
QUERY_USER_MATCH_THRESHOLD = float(os.getenv("RAG_QUERY_USER_MATCH_THRESHOLD", "0.7"))
NOT_FOUND_ANSWER = "The answer is not found in the documents."
PAGEINDEX_RETRIEVAL_POLL_SECONDS = float(
    os.getenv("RAG_PAGEINDEX_RETRIEVAL_POLL_SECONDS", "2.0")
)
PAGEINDEX_RETRIEVAL_TIMEOUT_SECONDS = float(
    os.getenv("RAG_PAGEINDEX_RETRIEVAL_TIMEOUT_SECONDS", "60.0")
)
PAGEINDEX_RETRIEVAL_DEBUG_CHARS = int(
    os.getenv("RAG_PAGEINDEX_RETRIEVAL_DEBUG_CHARS", "4000")
)
COORDINATOR_SUMMARY_DELAY_SECONDS = float(
    os.getenv("RAG_COORDINATOR_SUMMARY_DELAY_SECONDS", "3.0")
)
COORDINATOR_CONTEXT_MAX_CHARS = int(
    os.getenv("RAG_COORDINATOR_CONTEXT_MAX_CHARS", "16000")
)
NEIGHBORS = [
    neighbor.strip()
    for neighbor in os.getenv("RAG_NEIGHBORS", "").split(",")
    if neighbor.strip()
]
ROUTING_TREE_EXPECTED_USERS = int(
    os.getenv("RAG_ROUTING_TREE_EXPECTED_USERS", str(len(NEIGHBORS) + 1))
)
ROUTING_TREE_RECORD_LIMIT = int(os.getenv("RAG_ROUTING_TREE_RECORD_LIMIT", "2"))
ROUTING_TREE_DELTA = float(os.getenv("RAG_ROUTING_TREE_DELTA", "0.0005"))
ROUTING_TREE_CLOSEST_USERS = int(os.getenv("RAG_ROUTING_TREE_CLOSEST_USERS", "2"))
ROUTING_TREE_CANDIDATE_USERS = int(os.getenv("RAG_ROUTING_TREE_CANDIDATE_USERS", "0"))
ROUTING_MODE = RoutingMode.from_env(os.getenv("RAG_ROUTING_MODE", RoutingMode.JOIN_TREE.value))
INIT_WORKER_ID = os.getenv("RAG_INIT_WORKER_ID", "node1")
INIT_ADDR = os.getenv("RAG_INIT_ADDR", "node1:9100")
USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS = int(
    os.getenv("RAG_USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS", "12")
)
USER_EMBEDDING_REGISTER_RETRY_SECONDS = float(
    os.getenv("RAG_USER_EMBEDDING_REGISTER_RETRY_SECONDS", "5.0")
)
USER_EMBEDDING_SYNC_RETRY_ATTEMPTS = int(
    os.getenv("RAG_USER_EMBEDDING_SYNC_RETRY_ATTEMPTS", "12")
)
USER_EMBEDDING_SYNC_RETRY_SECONDS = float(
    os.getenv("RAG_USER_EMBEDDING_SYNC_RETRY_SECONDS", "5.0")
)
ROUTING_TREE_INSTALL_RETRY_ATTEMPTS = int(
    os.getenv("RAG_ROUTING_TREE_INSTALL_RETRY_ATTEMPTS", "6")
)
ROUTING_TREE_INSTALL_RETRY_SECONDS = float(
    os.getenv("RAG_ROUTING_TREE_INSTALL_RETRY_SECONDS", "2.0")
)
CHAIN_HOP_MAX_HOPS = int(os.getenv("RAG_CHAIN_HOP_MAX_HOPS", "4"))
BOOTSTRAP_QUERY = os.getenv("RAG_BOOTSTRAP_QUERY", "")
BOOTSTRAP_DELAY_SECONDS = float(os.getenv("RAG_BOOTSTRAP_DELAY_SECONDS", "5.0"))
BOOTSTRAP_QUERY_ID = os.getenv("RAG_BOOTSTRAP_QUERY_ID", "")
BOOTSTRAP_WAIT_FOR_ROUTING_TREE = (
    os.getenv("RAG_BOOTSTRAP_WAIT_FOR_ROUTING_TREE", "1") == "1"
)
BOOTSTRAP_ROUTING_TREE_TIMEOUT_SECONDS = float(
    os.getenv("RAG_BOOTSTRAP_ROUTING_TREE_TIMEOUT_SECONDS", "600.0")
)
MULTICAST_RETRY_ATTEMPTS = int(os.getenv("RAG_MULTICAST_RETRY_ATTEMPTS", "6"))
MULTICAST_RETRY_SECONDS = float(os.getenv("RAG_MULTICAST_RETRY_SECONDS", "5.0"))
LEASE_DURATION_SECONDS = int(os.getenv("RAG_LEASE_DURATION_SECONDS", "15"))
# Set RAG_FORCE_LEADER_ID to skip DynamoDB election and statically assign a leader.
# e.g. RAG_FORCE_LEADER_ID=node1  → node1 is always root; remove var to use DynamoDB election.
FORCE_LEADER_ID: str | None = os.getenv("RAG_FORCE_LEADER_ID")


def _parse_worker_addr_map() -> dict[str, str]:
    raw = os.getenv("RAG_WORKER_ADDR_MAP", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(
            f"[rag-worker {WORKER_ID}] invalid RAG_WORKER_ADDR_MAP JSON: {exc}",
            flush=True,
        )
        return {}
    if not isinstance(parsed, dict):
        print(
            f"[rag-worker {WORKER_ID}] invalid RAG_WORKER_ADDR_MAP: expected object",
            flush=True,
        )
        return {}
    return {str(worker_id): str(addr) for worker_id, addr in parsed.items()}


WORKER_ADDR_MAP = _parse_worker_addr_map()


def addr_for_worker_id(worker_id: str | None) -> str:
    if not worker_id:
        return INIT_ADDR
    if worker_id in WORKER_ADDR_MAP:
        return WORKER_ADDR_MAP[worker_id]
    if worker_id == INIT_WORKER_ID:
        return INIT_ADDR
    return f"{worker_id}:{PORT}"


BENCHMARK_EVENTS_PATH = os.getenv("RAG_BENCHMARK_EVENTS_PATH", "")
benchmark_events = BenchmarkEventLogger(BENCHMARK_EVENTS_PATH)

leader_election: LeaderElection | None = None


class RagWorkerServicer(rag_pb2_grpc.RagServiceServicer):
    def __init__(
        self,
        worker_id: str,
        store: LocalDocumentStore,
        neighbors: list[str],
        bert_embedder: BertEmbedder,
    ):
        self.worker_id = worker_id
        self.store = store
        self.neighbors = neighbors
        self.bert_embedder = bert_embedder
        self.background_tasks = set()
        self.evidence_by_query = {}
        self.query_by_id = {}
        self.summary_tasks = {}
        self.llm_client = None
        self.user_embedding_registry = {}
        self.routing_epoch = 0
        self.routing_tree_result = None
        self.routing_tree_built_for_workers = set()
        self.closest_user_entries_by_worker = {}
        self.joined_tree_users = {}
        self.routing_tree_nodes = {}
        self.routing_tree_inserted_workers = set()
        self.next_routing_tree_node_id = 0
        self.routing_tree_root_node_id = "root"
        self.assigned_routing_tree_node_id = ""
        self.assigned_routing_tree_custodian_worker_id = ""
        self.assigned_routing_tree_custodian_addr = ""

    async def Ping(self, request, context):
        return rag_pb2.RagPingReply(worker_id=self.worker_id, status="alive")

    async def JoinTree(self, request, context):
        tree_node_id = request.tree_node_id or self.routing_tree_root_node_id
        if not self.can_handle_routing_tree_node(tree_node_id):
            print(
                f"[rag-worker {self.worker_id}] rejected JoinTree "
                f"from={request.worker_id}; unknown_or_remote_tree_node={tree_node_id}",
                flush=True,
            )
            return rag_pb2.JoinTreeReply(
                root_worker_id=self.current_root_worker_id(),
                accepted=False,
                joined_count=0,
                expected_count=ROUTING_TREE_EXPECTED_USERS,
                routing_epoch=self.routing_epoch,
                tree_node_id=tree_node_id,
                custodian_worker_id=self.worker_id,
                custodian_addr=ADVERTISE_ADDR,
            )

        invalid_join_reason = self.invalid_join_embedding_reason(request)
        if invalid_join_reason:
            print(
                f"[rag-worker {self.worker_id}] rejected JoinTree "
                f"from={request.worker_id} tree_node_id={tree_node_id}: "
                f"{invalid_join_reason} source_roots={request.source_root_count} "
                f"source_nodes={list(request.source_node_ids)[:5]}",
                flush=True,
            )
            return rag_pb2.JoinTreeReply(
                root_worker_id=self.current_root_worker_id(),
                accepted=False,
                joined_count=len(self.joined_tree_users),
                expected_count=ROUTING_TREE_EXPECTED_USERS,
                routing_epoch=self.routing_epoch,
                tree_node_id=tree_node_id,
                custodian_worker_id=self.worker_id,
                custodian_addr=ADVERTISE_ADDR,
            )

        reply = await self.handle_routing_tree_join(request, tree_node_id)
        return reply

    async def InstallRoutingTreeNode(self, request, context):
        node = self.routing_tree_node_from_state(request.node)
        if node.custodian_worker_id and node.custodian_worker_id != self.worker_id:
            print(
                f"[rag-worker {self.worker_id}] rejected routing-tree node install "
                f"node_id={node.node_id} custodian={node.custodian_worker_id}",
                flush=True,
            )
            return rag_pb2.InstallRoutingTreeNodeReply(
                worker_id=self.worker_id,
                accepted=False,
                node_id=node.node_id,
            )

        self.routing_tree_nodes[node.node_id] = node
        for record in request.node.members:
            self.record_user_embedding_record(record)
            self.joined_tree_users[record.worker_id] = self.user_embedding_registry[record.worker_id]
            self.routing_tree_inserted_workers.add(record.worker_id)
            if record.worker_id == self.worker_id:
                self.assigned_routing_tree_node_id = node.node_id
                self.assigned_routing_tree_custodian_worker_id = node.custodian_worker_id
                self.assigned_routing_tree_custodian_addr = node.custodian_addr
        self.refresh_routing_tree_metadata()
        print(
            f"[rag-worker {self.worker_id}] installed routing-tree node "
            f"node_id={node.node_id} kind={node.kind} members={node.member_worker_ids} "
            f"from={request.sender_worker_id}",
            flush=True,
        )
        return rag_pb2.InstallRoutingTreeNodeReply(
            worker_id=self.worker_id,
            accepted=True,
            node_id=node.node_id,
        )

    def can_handle_routing_tree_node(self, tree_node_id: str):
        if tree_node_id == self.routing_tree_root_node_id and self.is_root_custodian():
            return True
        node = self.routing_tree_nodes.get(tree_node_id)
        return node is not None and node.custodian_worker_id == self.worker_id

    def current_root_worker_id(self):
        if FORCE_LEADER_ID:
            return FORCE_LEADER_ID
        if leader_election is not None:
            return leader_election.get_leader_id() or INIT_WORKER_ID
        return INIT_WORKER_ID

    def current_root_addr(self):
        return addr_for_worker_id(self.current_root_worker_id())

    def is_root_custodian(self):
        if FORCE_LEADER_ID:
            return self.worker_id == FORCE_LEADER_ID
        if leader_election is not None:
            return leader_election.is_leader()
        return self.worker_id == INIT_WORKER_ID

    def invalid_join_embedding_reason(self, request):
        embedding_len = len(request.embedding)
        if embedding_len == 0:
            return "empty user embedding"
        declared_dimension = request.embedding_dimension or embedding_len
        if declared_dimension != embedding_len:
            return (
                f"embedding dimension mismatch declared={declared_dimension} "
                f"actual={embedding_len}"
            )
        return ""

    async def handle_routing_tree_join(self, request, tree_node_id: str):
        self.ensure_routing_tree_if_root(tree_node_id)
        node = self.routing_tree_nodes.get(tree_node_id)
        if node is None:
            return rag_pb2.JoinTreeReply(
                root_worker_id=self.current_root_worker_id(),
                accepted=False,
                joined_count=len(self.joined_tree_users),
                expected_count=ROUTING_TREE_EXPECTED_USERS,
                routing_epoch=self.routing_epoch,
                tree_node_id=tree_node_id,
                custodian_worker_id=self.worker_id,
                custodian_addr=ADVERTISE_ADDR,
            )

        if node.kind == "split":
            reply = await self.forward_join_to_closest_child(request, node)
            if reply.accepted and request.worker_id not in self.joined_tree_users:
                entry = self.user_embedding_entry_from_registration(request)
                self.joined_tree_users[request.worker_id] = entry
                self.user_embedding_registry[request.worker_id] = entry
            return self.with_local_join_counts(reply)

        assigned_tree_node_id = await self.record_routing_tree_join(
            request,
            tree_node_id=tree_node_id,
        )
        assigned_node = self.routing_tree_nodes.get(assigned_tree_node_id)
        custodian_worker_id = self.worker_id
        custodian_addr = ADVERTISE_ADDR
        if assigned_node is not None:
            custodian_worker_id = assigned_node.custodian_worker_id or custodian_worker_id
            custodian_addr = assigned_node.custodian_addr or custodian_addr
        closest_entries = self.closest_user_entries_by_worker.get(request.worker_id, [])
        return rag_pb2.JoinTreeReply(
            root_worker_id=self.current_root_worker_id(),
            accepted=True,
            joined_count=len(self.joined_tree_users),
            expected_count=ROUTING_TREE_EXPECTED_USERS,
            routing_epoch=self.routing_epoch,
            tree_node_id=assigned_tree_node_id,
            custodian_worker_id=custodian_worker_id,
            custodian_addr=custodian_addr,
            closest_users=[
                self.user_embedding_record_from_entry(entry)
                for entry in closest_entries
            ],
        )

    async def RegisterUserEmbedding(self, request, context):
        self.record_user_embedding(request)
        return rag_pb2.RegisterUserEmbeddingReply(
            worker_id=self.worker_id,
            registered_count=len(self.user_embedding_registry),
        )

    async def GetUserEmbeddingRegistry(self, request, context):
        entries = self.registry_entries_for_requester(request.requester_worker_id)
        return rag_pb2.GetUserEmbeddingRegistryReply(
            worker_id=self.worker_id,
            users=[
                self.user_embedding_record_from_entry(entry)
                for entry in entries
            ],
        )

    def registry_entries_for_requester(self, requester_worker_id: str):
        closest_entries = self.closest_user_entries_by_worker.get(requester_worker_id)
        if closest_entries is not None:
            print(
                f"[rag-worker {self.worker_id}] serving routing closest-users "
                f"requester={requester_worker_id} epoch={self.routing_epoch} "
                f"count={len(closest_entries)}",
                flush=True,
            )
            return closest_entries

        if self.routing_epoch == 0:
            print(
                f"[rag-worker {self.worker_id}] routing closest-users not ready "
                f"requester={requester_worker_id} "
                f"joined={len(self.joined_tree_users)}/{ROUTING_TREE_EXPECTED_USERS}",
                flush=True,
            )
            return []

        print(
            f"[rag-worker {self.worker_id}] serving full user embedding registry "
            f"requester={requester_worker_id} count={len(self.user_embedding_registry)} "
            f"routing_epoch={self.routing_epoch}",
            flush=True,
        )
        return list(self.user_embedding_registry.values())

    def record_user_embedding(self, request):
        embedding = list(request.embedding)
        dimension = request.embedding_dimension or len(embedding)
        self.user_embedding_registry[request.worker_id] = {
            "worker_id": request.worker_id,
            "advertise_addr": request.advertise_addr,
            "embedding": embedding,
            "embedding_model": request.embedding_model,
            "embedding_dimension": dimension,
            "source_root_count": request.source_root_count,
            "source_node_ids": list(request.source_node_ids),
        }
        print(
            f"[rag-worker {self.worker_id}] registered user embedding "
            f"worker_id={request.worker_id} addr={request.advertise_addr} "
            f"dimension={dimension} source_roots={request.source_root_count} "
            f"registry_size={len(self.user_embedding_registry)}",
            flush=True,
        )

    async def record_routing_tree_join(self, request, tree_node_id: str | None = None):
        entry = self.user_embedding_entry_from_registration(request)
        already_joined = request.worker_id in self.joined_tree_users
        assigned_tree_node_id = self.routing_tree_leaf_id_for_worker(
            request.worker_id,
            tree_node_id or self.routing_tree_root_node_id,
        )
        self.joined_tree_users[request.worker_id] = entry
        self.user_embedding_registry[request.worker_id] = entry
        print(
            f"[rag-worker {self.worker_id}] JoinTree accepted "
            f"worker_id={request.worker_id} addr={request.advertise_addr} "
            f"dimension={entry['embedding_dimension']} "
            f"joined={len(self.joined_tree_users)}/{ROUTING_TREE_EXPECTED_USERS} "
            f"already_joined={already_joined}",
            flush=True,
        )
        if not already_joined:
            assigned_tree_node_id = await self.insert_routing_tree_peer(
                entry,
                tree_node_id=tree_node_id or self.routing_tree_root_node_id,
            )
        return assigned_tree_node_id

    def record_user_embedding_record(self, record):
        self.user_embedding_registry[record.worker_id] = {
            "worker_id": record.worker_id,
            "advertise_addr": record.advertise_addr,
            "embedding": list(record.embedding),
            "embedding_model": record.embedding_model,
            "embedding_dimension": record.embedding_dimension or len(record.embedding),
            "source_root_count": record.source_root_count,
            "source_node_ids": list(record.source_node_ids),
        }

    def user_embedding_entry_from_registration(self, request):
        embedding = list(request.embedding)
        return {
            "worker_id": request.worker_id,
            "advertise_addr": request.advertise_addr,
            "embedding": embedding,
            "embedding_model": request.embedding_model,
            "embedding_dimension": request.embedding_dimension or len(embedding),
            "source_root_count": request.source_root_count,
            "source_node_ids": list(request.source_node_ids),
        }

    def build_user_embedding_registration(self):
        return rag_pb2.RegisterUserEmbeddingRequest(
            worker_id=self.worker_id,
            advertise_addr=ADVERTISE_ADDR,
            embedding=self.store.user_embedding or [],
            embedding_model=self.store.user_embedding_model or "",
            embedding_dimension=self.store.user_embedding_dimension,
            source_root_count=len(self.store.user_embedding_source_node_ids),
            source_node_ids=self.store.user_embedding_source_node_ids,
        )

    def build_routing_tree_join_request(self):
        return rag_pb2.JoinTreeRequest(
            worker_id=self.worker_id,
            advertise_addr=ADVERTISE_ADDR,
            embedding=self.store.user_embedding or [],
            embedding_model=self.store.user_embedding_model or "",
            embedding_dimension=self.store.user_embedding_dimension,
            source_root_count=len(self.store.user_embedding_source_node_ids),
            source_node_ids=self.store.user_embedding_source_node_ids,
        )

    def ensure_routing_tree(self):
        if self.routing_tree_nodes:
            return self.routing_tree_nodes[self.routing_tree_root_node_id]

        root = RoutingTreeNode(
            node_id=self.routing_tree_root_node_id,
            kind="leaf",
            depth=0,
            custodian_worker_id=self.worker_id,
            custodian_addr=ADVERTISE_ADDR,
        )
        self.routing_tree_nodes[root.node_id] = root
        print(
            f"[rag-worker {self.worker_id}] initialized empty routing-tree root leaf "
            f"tree_node_id={root.node_id} custodian={root.custodian_worker_id} "
            f"record_limit={ROUTING_TREE_RECORD_LIMIT} delta={ROUTING_TREE_DELTA}",
            flush=True,
        )
        return root

    def ensure_routing_tree_if_root(self, tree_node_id: str):
        if tree_node_id == self.routing_tree_root_node_id and self.is_root_custodian():
            self.ensure_routing_tree()

    async def insert_routing_tree_peer(self, entry, tree_node_id: str):
        if not entry.get("embedding"):
            print(
                f"[rag-worker {self.worker_id}] skipping routing-tree insert "
                f"worker_id={entry.get('worker_id')} because embedding is empty",
                flush=True,
            )
            return tree_node_id
        if entry["worker_id"] in self.routing_tree_inserted_workers:
            return self.routing_tree_leaf_id_for_worker(entry["worker_id"], tree_node_id)

        leaf = self.routing_tree_nodes.get(tree_node_id)
        if leaf is None:
            print(
                f"[rag-worker {self.worker_id}] skipping routing-tree insert "
                f"worker_id={entry['worker_id']} missing_leaf={tree_node_id}",
                flush=True,
            )
            return tree_node_id
        if leaf.kind != "leaf":
            raise ValueError(f"Cannot insert directly into non-leaf tree_node_id={tree_node_id}")

        leaf.member_worker_ids.append(entry["worker_id"])
        self.routing_tree_inserted_workers.add(entry["worker_id"])
        print(
            f"[rag-worker {self.worker_id}] inserted routing-tree peer "
            f"worker_id={entry['worker_id']} leaf={leaf.node_id} "
            f"leaf_size={len(leaf.member_worker_ids)}/{ROUTING_TREE_RECORD_LIMIT}",
            flush=True,
        )
        if len(leaf.member_worker_ids) > ROUTING_TREE_RECORD_LIMIT:
            await self.split_routing_tree_leaf(leaf)
        self.refresh_routing_tree_metadata()
        return self.routing_tree_leaf_id_for_worker(entry["worker_id"], leaf.node_id)

    def routing_tree_leaf_id_for_worker(self, worker_id: str, fallback_node_id: str):
        for node_id, node in self.routing_tree_nodes.items():
            if node.kind == "leaf" and worker_id in node.member_worker_ids:
                return node_id
        return fallback_node_id

    def find_routing_tree_leaf(self, embedding) -> RoutingTreeNode:
        node = self.routing_tree_nodes[self.routing_tree_root_node_id]
        emb = _embedding_array(embedding)
        while node.kind == "split":
            left_distance = np.linalg.norm(emb - np.asarray(node.centroid_left, dtype=np.float64))
            right_distance = np.linalg.norm(emb - np.asarray(node.centroid_right, dtype=np.float64))
            next_node_id = node.left_child_id if left_distance <= right_distance else node.right_child_id
            print(
                f"[rag-worker {self.worker_id}] routing-tree traversal "
                f"at={node.node_id} left_distance={left_distance:.4f} "
                f"right_distance={right_distance:.4f} next={next_node_id}",
                flush=True,
            )
            node = self.routing_tree_nodes[next_node_id]
        return node

    async def split_routing_tree_leaf(self, leaf: RoutingTreeNode):
        member_ids = list(dict.fromkeys(leaf.member_worker_ids))
        embeddings = []
        for worker_id in member_ids:
            entry = self.joined_tree_users.get(worker_id)
            if entry is not None and entry.get("embedding"):
                embeddings.append(_embedding_array(entry["embedding"]))
        if len(embeddings) < 2:
            return

        labels, centroids = _two_means(np.asarray(embeddings, dtype=np.float64))
        left_members = [worker_id for worker_id, label in zip(member_ids, labels) if label == 0]
        right_members = [worker_id for worker_id, label in zip(member_ids, labels) if label == 1]
        if not left_members or not right_members:
            print(
                f"[rag-worker {self.worker_id}] routing-tree split skipped "
                f"leaf={leaf.node_id} left={left_members} right={right_members}",
                flush=True,
            )
            return

        left_custodian = self.closest_member_to_centroid(left_members, centroids[0])
        right_custodian = self.closest_member_to_centroid(right_members, centroids[1])
        left_id = self.next_routing_tree_child_id(leaf.node_id, "L")
        right_id = self.next_routing_tree_child_id(leaf.node_id, "R")

        leaf.kind = "split"
        leaf.member_worker_ids = []
        leaf.left_child_id = left_id
        leaf.right_child_id = right_id
        leaf.left_custodian_worker_id = left_custodian["worker_id"]
        leaf.left_custodian_addr = left_custodian["advertise_addr"]
        leaf.right_custodian_worker_id = right_custodian["worker_id"]
        leaf.right_custodian_addr = right_custodian["advertise_addr"]
        leaf.centroid_left = centroids[0].astype(float).tolist()
        leaf.centroid_right = centroids[1].astype(float).tolist()

        self.routing_tree_nodes[left_id] = RoutingTreeNode(
            node_id=left_id,
            kind="leaf",
            depth=leaf.depth + 1,
            parent_id=leaf.node_id,
            custodian_worker_id=left_custodian["worker_id"],
            custodian_addr=left_custodian["advertise_addr"],
            member_worker_ids=left_members,
        )
        self.routing_tree_nodes[right_id] = RoutingTreeNode(
            node_id=right_id,
            kind="leaf",
            depth=leaf.depth + 1,
            parent_id=leaf.node_id,
            custodian_worker_id=right_custodian["worker_id"],
            custodian_addr=right_custodian["advertise_addr"],
            member_worker_ids=right_members,
        )
        print(
            f"[rag-worker {self.worker_id}] routing-tree leaf split "
            f"leaf={leaf.node_id} left={left_id} left_members={left_members} "
            f"left_custodian={left_custodian['worker_id']}@{left_custodian['advertise_addr']} "
            f"right={right_id} right_members={right_members} "
            f"right_custodian={right_custodian['worker_id']}@{right_custodian['advertise_addr']}",
            flush=True,
        )
        await asyncio.gather(
            self.install_routing_tree_node_on_custodian(self.routing_tree_nodes[left_id]),
            self.install_routing_tree_node_on_custodian(self.routing_tree_nodes[right_id]),
        )

    def closest_member_to_centroid(self, member_ids: list[str], centroid: np.ndarray):
        best_entry = None
        best_distance = None
        for worker_id in member_ids:
            entry = self.joined_tree_users[worker_id]
            distance = np.linalg.norm(_embedding_array(entry["embedding"]) - centroid)
            if best_distance is None or distance < best_distance:
                best_entry = entry
                best_distance = distance
        return best_entry

    def routing_tree_node_state(self, node: RoutingTreeNode):
        return rag_pb2.RoutingTreeNodeState(
            node_id=node.node_id,
            kind=node.kind,
            depth=node.depth,
            parent_id=node.parent_id or "",
            custodian_worker_id=node.custodian_worker_id,
            custodian_addr=node.custodian_addr,
            members=[
                self.user_embedding_record_from_entry(self.joined_tree_users[worker_id])
                for worker_id in node.member_worker_ids
                if worker_id in self.joined_tree_users
            ],
            left_child_id=node.left_child_id,
            right_child_id=node.right_child_id,
            left_custodian_worker_id=node.left_custodian_worker_id,
            left_custodian_addr=node.left_custodian_addr,
            right_custodian_worker_id=node.right_custodian_worker_id,
            right_custodian_addr=node.right_custodian_addr,
            centroid_left=node.centroid_left,
            centroid_right=node.centroid_right,
        )

    def routing_tree_node_from_state(self, state):
        return RoutingTreeNode(
            node_id=state.node_id,
            kind=state.kind,
            depth=state.depth,
            parent_id=state.parent_id or None,
            custodian_worker_id=state.custodian_worker_id,
            custodian_addr=state.custodian_addr,
            member_worker_ids=[record.worker_id for record in state.members],
            left_child_id=state.left_child_id,
            right_child_id=state.right_child_id,
            left_custodian_worker_id=state.left_custodian_worker_id,
            left_custodian_addr=state.left_custodian_addr,
            right_custodian_worker_id=state.right_custodian_worker_id,
            right_custodian_addr=state.right_custodian_addr,
            centroid_left=list(state.centroid_left),
            centroid_right=list(state.centroid_right),
        )

    async def install_routing_tree_node_on_custodian(self, node: RoutingTreeNode):
        if node.custodian_worker_id == self.worker_id:
            return True
        if not node.custodian_addr:
            return False
        for attempt in range(1, ROUTING_TREE_INSTALL_RETRY_ATTEMPTS + 1):
            try:
                async with grpc.aio.insecure_channel(node.custodian_addr) as channel:
                    stub = rag_pb2_grpc.RagServiceStub(channel)
                    reply = await stub.InstallRoutingTreeNode(
                        rag_pb2.InstallRoutingTreeNodeRequest(
                            sender_worker_id=self.worker_id,
                            node=self.routing_tree_node_state(node),
                        ),
                        timeout=5.0,
                    )
                print(
                    f"[rag-worker {self.worker_id}] installed routing-tree child "
                    f"node_id={node.node_id} custodian="
                    f"{node.custodian_worker_id}@{node.custodian_addr} "
                    f"accepted={reply.accepted} attempt={attempt}",
                    flush=True,
                )
                return reply.accepted
            except Exception as exc:
                if attempt == ROUTING_TREE_INSTALL_RETRY_ATTEMPTS:
                    print(
                        f"[rag-worker {self.worker_id}] failed to install routing-tree child "
                        f"node_id={node.node_id} custodian="
                        f"{node.custodian_worker_id}@{node.custodian_addr} "
                        f"attempts={attempt}: {exc}",
                        flush=True,
                    )
                    return False
                print(
                    f"[rag-worker {self.worker_id}] retrying routing-tree child install "
                    f"node_id={node.node_id} custodian="
                    f"{node.custodian_worker_id}@{node.custodian_addr} "
                    f"attempt={attempt}/{ROUTING_TREE_INSTALL_RETRY_ATTEMPTS}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(ROUTING_TREE_INSTALL_RETRY_SECONDS)

    async def forward_join_to_closest_child(self, request, node: RoutingTreeNode):
        emb = _embedding_array(request.embedding)
        left_distance = np.linalg.norm(emb - np.asarray(node.centroid_left, dtype=np.float64))
        right_distance = np.linalg.norm(emb - np.asarray(node.centroid_right, dtype=np.float64))
        if left_distance <= right_distance:
            child_id = node.left_child_id
            custodian_worker_id = node.left_custodian_worker_id
            custodian_addr = node.left_custodian_addr
        else:
            child_id = node.right_child_id
            custodian_worker_id = node.right_custodian_worker_id
            custodian_addr = node.right_custodian_addr

        print(
            f"[rag-worker {self.worker_id}] forwarding JoinTree "
            f"from={request.worker_id} at={node.node_id} next={child_id} "
            f"custodian={custodian_worker_id}@{custodian_addr} "
            f"left_distance={left_distance:.4f} right_distance={right_distance:.4f}",
            flush=True,
        )
        if custodian_worker_id == self.worker_id:
            forwarded = rag_pb2.JoinTreeRequest(
                worker_id=request.worker_id,
                advertise_addr=request.advertise_addr,
                embedding=request.embedding,
                embedding_model=request.embedding_model,
                embedding_dimension=request.embedding_dimension,
                source_root_count=request.source_root_count,
                source_node_ids=request.source_node_ids,
                tree_node_id=child_id,
            )
            reply = await self.handle_routing_tree_join(forwarded, child_id)
            return self.with_local_join_counts(reply)

        try:
            async with grpc.aio.insecure_channel(custodian_addr) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                reply = await stub.JoinTree(
                    rag_pb2.JoinTreeRequest(
                        worker_id=request.worker_id,
                        advertise_addr=request.advertise_addr,
                        embedding=request.embedding,
                        embedding_model=request.embedding_model,
                        embedding_dimension=request.embedding_dimension,
                        source_root_count=request.source_root_count,
                        source_node_ids=request.source_node_ids,
                        tree_node_id=child_id,
                    ),
                    timeout=5.0,
                )
            return self.with_local_join_counts(reply)
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] failed to forward JoinTree "
                f"from={request.worker_id} to={custodian_worker_id}@{custodian_addr} "
                f"child={child_id}: {exc}",
                flush=True,
            )
            return rag_pb2.JoinTreeReply(
                root_worker_id=self.current_root_worker_id(),
                accepted=False,
                joined_count=len(self.joined_tree_users),
                expected_count=ROUTING_TREE_EXPECTED_USERS,
                routing_epoch=self.routing_epoch,
                tree_node_id=child_id,
                custodian_worker_id=custodian_worker_id,
                custodian_addr=custodian_addr,
            )

    def with_local_join_counts(self, reply):
        if self.is_root_custodian():
            reply.root_worker_id = self.worker_id
            reply.joined_count = len(self.joined_tree_users)
            reply.expected_count = ROUTING_TREE_EXPECTED_USERS
        return reply

    def next_routing_tree_child_id(self, parent_id: str, side: str):
        self.next_routing_tree_node_id += 1
        return f"{parent_id}.{side}{self.next_routing_tree_node_id}"

    def refresh_routing_tree_metadata(self):
        if not self.routing_tree_nodes:
            return

        self.routing_epoch += 1
        self.routing_tree_built_for_workers = set(self.routing_tree_inserted_workers)
        self.closest_user_entries_by_worker = self.closest_users_from_routing_tree()

        leaf_members = self.routing_tree_leaf_members()
        print(
            f"[rag-worker {self.worker_id}] refreshed routing-tree metadata "
            f"epoch={self.routing_epoch} joined={len(self.joined_tree_users)} "
            f"leaves={len(leaf_members)} "
            f"leaf_members={json.dumps(leaf_members, sort_keys=True)}",
            flush=True,
        )

    def routing_tree_leaf_members(self):
        return {
            node_id: node.member_worker_ids
            for node_id, node in sorted(self.routing_tree_nodes.items())
            if node.kind == "leaf"
        }

    def closest_users_from_routing_tree(self):
        closest_by_worker = {}
        for node in self.routing_tree_nodes.values():
            if node.kind != "leaf":
                continue
            for worker_id in node.member_worker_ids:
                owner_entry = self.joined_tree_users.get(worker_id)
                if owner_entry is None:
                    continue
                scored = []
                owner_embedding = owner_entry["embedding"]
                for candidate_id in node.member_worker_ids:
                    if candidate_id == worker_id:
                        continue
                    candidate_entry = self.joined_tree_users.get(candidate_id)
                    if candidate_entry is None:
                        continue
                    score = cosine_similarity(owner_embedding, candidate_entry["embedding"])
                    scored.append((score, candidate_entry))
                scored.sort(key=lambda item: item[0], reverse=True)
                closest_by_worker[worker_id] = [
                    entry
                    for _, entry in scored[:ROUTING_TREE_CLOSEST_USERS]
                ]
        return closest_by_worker

    def user_embedding_record_from_entry(self, entry):
        return rag_pb2.UserEmbeddingRecord(
            worker_id=entry["worker_id"],
            advertise_addr=entry["advertise_addr"],
            embedding=entry["embedding"],
            embedding_model=entry.get("embedding_model", ""),
            embedding_dimension=entry.get("embedding_dimension", 0),
            source_root_count=entry.get("source_root_count", 0),
            source_node_ids=entry.get("source_node_ids", []),
        )

    async def SendEvidence(self, request, context):
        evidence = self.evidence_by_query.setdefault(request.query_id, [])
        evidence.extend(request.evidence)
        benchmark_events.emit(
            "evidence_received",
            query_id=request.query_id,
            evidence_count=len(request.evidence),
            total_evidence_count=len(evidence),
            source_workers=sorted({item.worker_id for item in request.evidence}),
        )
        print(
            f"[rag-worker {self.worker_id}] received evidence query_id={request.query_id} "
            f"count={len(request.evidence)} total={len(evidence)}",
            flush=True,
        )
        for item in request.evidence:
            print(
                f"[rag-worker {self.worker_id}] evidence query_id={request.query_id} "
                f"from={item.worker_id} node={item.node_id} title={item.title} "
                f"metadata={item.metadata_json}",
                flush=True,
            )
            # print(
            #     f"[rag-worker {self.worker_id}] evidence content "
            #     f"query_id={request.query_id} node={item.node_id}: {item.content}",
            #     flush=True,
            # )
        if request.evidence:
            self.start_summary_task(request.query_id)
        return rag_pb2.SendEvidenceReply(
            worker_id=self.worker_id,
            accepted_count=len(request.evidence),
        )

    async def RouteQuery(self, request, context):
        query_id = request.query_id or str(uuid.uuid4())
        coordinator_addr = request.coordinator_addr or ADVERTISE_ADDR
        curr_hop = request.curr_hop
        max_hops = request.max_hops or CHAIN_HOP_MAX_HOPS
        visited_worker_ids = list(request.visited_worker_ids)
        if self.worker_id not in visited_worker_ids:
            visited_worker_ids.append(self.worker_id)
        if coordinator_addr == ADVERTISE_ADDR:
            self.query_by_id[query_id] = request.query
            benchmark_events.emit(
                "query_started",
                query_id=query_id,
                query=request.query,
                coordinator_addr=coordinator_addr,
                max_hops=max_hops,
                top_k=request.top_k or 5,
            )
            self.start_summary_task(query_id)

        benchmark_events.emit(
            "route_received",
            query_id=query_id,
            query=request.query,
            coordinator_addr=coordinator_addr,
            curr_hop=curr_hop,
            max_hops=max_hops,
            visited_worker_ids=visited_worker_ids,
        )
        print(
            f"[rag-worker {self.worker_id}] received query_id={query_id} "
            f"coordinator={coordinator_addr} hop={curr_hop}/{max_hops} "
            f"visited={visited_worker_ids} query={request.query}",
            flush=True,
        )

        top_k = request.top_k or 5
        candidates = []
        local_candidates = self.store.route(
            request.query,
            top_k=top_k,
            embedder=self.bert_embedder,
            max_length=BERT_MAX_LENGTH,
        )
        for score, node in local_candidates:
            benchmark_events.emit(
                "local_candidate",
                query_id=query_id,
                node_id=node.node_id,
                title=node.title,
                score=float(score),
                doc_id=node.metadata.get("doc_id", ""),
                source_path=node.metadata.get("source_path", ""),
                curr_hop=curr_hop,
            )
            print(
                f"[rag-worker {self.worker_id}] local candidate query_id={query_id} "
                f"score={score:.4f} node={node.node_id} title={node.title}",
                flush=True,
            )
            candidates.append(
                rag_pb2.NodeCandidate(
                    worker_id=self.worker_id,
                    node_id=node.node_id,
                    title=node.title,
                    score=float(score),
                    preview=node.preview(),
                    description=node.description,
                    metadata_json=node.metadata_json(),
                )
            )

        if LOCAL_RETRIEVAL_POLICY in {"top1_probe", "topk_probe"}:
            probe_candidates = local_candidates[:1]
            if LOCAL_RETRIEVAL_POLICY == "topk_probe":
                probe_candidates = local_candidates
            best_score = local_candidates[0][0] if local_candidates else None
            best_score_text = f"{best_score:.4f}" if best_score is not None else "none"
            print(
                f"[rag-worker {self.worker_id}] local retrieval probe query_id={query_id} "
                f"policy={LOCAL_RETRIEVAL_POLICY} candidates={len(probe_candidates)} "
                f"best_score={best_score_text}",
                flush=True,
            )
            benchmark_events.emit(
                "local_probe_selected",
                query_id=query_id,
                policy=LOCAL_RETRIEVAL_POLICY,
                candidate_count=len(probe_candidates),
                best_score=float(best_score) if best_score is not None else None,
                curr_hop=curr_hop,
            )
            self.start_pageindex_probe_then_chain_hop_task(
                request,
                query_id,
                coordinator_addr,
                curr_hop,
                max_hops,
                visited_worker_ids,
                probe_candidates,
            )
        else:
            matched_candidates = [
                (score, node)
                for score, node in local_candidates
                if score >= QUERY_DOC_MATCH_THRESHOLD
            ]
            if matched_candidates:
                print(
                    f"[rag-worker {self.worker_id}] local document match query_id={query_id} "
                    f"doc_threshold={QUERY_DOC_MATCH_THRESHOLD:.4f} "
                    f"matches={len(matched_candidates)}/{len(local_candidates)}",
                    flush=True,
                )
            else:
                best_score = local_candidates[0][0] if local_candidates else None
                best_score_text = f"{best_score:.4f}" if best_score is not None else "none"
                print(
                    f"[rag-worker {self.worker_id}] no local document match query_id={query_id} "
                    f"doc_threshold={QUERY_DOC_MATCH_THRESHOLD:.4f} "
                    f"best_score={best_score_text}",
                    flush=True,
                )

            self.start_pageindex_retrieval_task(
                query_id,
                request.query,
                coordinator_addr,
                matched_candidates,
            )
            if not matched_candidates:
                self.start_chain_hop_forward_task(
                    request,
                    query_id,
                    coordinator_addr,
                    curr_hop,
                    max_hops,
                    visited_worker_ids,
                )
        return rag_pb2.RouteQueryReply(
            worker_id=self.worker_id,
            candidates=candidates,
        )

    def start_pageindex_probe_then_chain_hop_task(
        self,
        request,
        query_id: str,
        coordinator_addr: str,
        curr_hop: int,
        max_hops: int,
        visited_worker_ids: list[str],
        local_candidates,
    ):
        task = asyncio.create_task(
            self.pageindex_probe_then_maybe_chain_hop(
                request,
                query_id,
                coordinator_addr,
                curr_hop,
                max_hops,
                visited_worker_ids,
                local_candidates,
            )
        )
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def pageindex_probe_then_maybe_chain_hop(
        self,
        request,
        query_id: str,
        coordinator_addr: str,
        curr_hop: int,
        max_hops: int,
        visited_worker_ids: list[str],
        local_candidates,
    ):
        evidence_count = await self.retrieve_and_send_pageindex_documents(
            query_id,
            request.query,
            coordinator_addr,
            local_candidates,
            reason="local_probe",
        )
        if evidence_count > 0:
            print(
                f"[rag-worker {self.worker_id}] local probe found evidence "
                f"query_id={query_id} evidence_count={evidence_count}; "
                "not forwarding chain hop",
                flush=True,
            )
            return

        print(
            f"[rag-worker {self.worker_id}] local probe found no evidence "
            f"query_id={query_id}; forwarding chain hop if available",
            flush=True,
        )
        await self.forward_chain_hop_if_available(
            request,
            query_id,
            coordinator_addr,
            curr_hop,
            max_hops,
            visited_worker_ids,
        )

    def start_chain_hop_forward_task(
        self,
        request,
        query_id: str,
        coordinator_addr: str,
        curr_hop: int,
        max_hops: int,
        visited_worker_ids: list[str],
    ):
        if curr_hop + 1 >= max_hops:
            benchmark_events.emit(
                "chain_hop_limit_reached",
                query_id=query_id,
                curr_hop=curr_hop,
                max_hops=max_hops,
                visited_worker_ids=visited_worker_ids,
            )
            print(
                f"[rag-worker {self.worker_id}] chain hop limit reached "
                f"query_id={query_id} hop={curr_hop}/{max_hops}",
                flush=True,
            )
            return

        task = asyncio.create_task(
            self.forward_chain_hop(
                request,
                query_id,
                coordinator_addr,
                curr_hop,
                max_hops,
                visited_worker_ids,
            )
        )
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def forward_chain_hop_if_available(
        self,
        request,
        query_id: str,
        coordinator_addr: str,
        curr_hop: int,
        max_hops: int,
        visited_worker_ids: list[str],
    ):
        if curr_hop + 1 >= max_hops:
            benchmark_events.emit(
                "chain_hop_limit_reached",
                query_id=query_id,
                curr_hop=curr_hop,
                max_hops=max_hops,
                visited_worker_ids=visited_worker_ids,
            )
            print(
                f"[rag-worker {self.worker_id}] chain hop limit reached "
                f"query_id={query_id} hop={curr_hop}/{max_hops}",
                flush=True,
            )
            return
        await self.forward_chain_hop(
            request,
            query_id,
            coordinator_addr,
            curr_hop,
            max_hops,
            visited_worker_ids,
        )

    async def forward_chain_hop(
        self,
        request,
        query_id: str,
        coordinator_addr: str,
        curr_hop: int,
        max_hops: int,
        visited_worker_ids: list[str],
    ):
        target = await self.choose_chain_hop_target(
            request.query,
            set(visited_worker_ids),
            query_id=query_id,
        )
        if target is None:
            benchmark_events.emit(
                "chain_hop_no_target",
                query_id=query_id,
                curr_hop=curr_hop,
                max_hops=max_hops,
                visited_worker_ids=visited_worker_ids,
            )
            print(
                f"[rag-worker {self.worker_id}] no chain-hop target "
                f"query_id={query_id} hop={curr_hop}/{max_hops}",
                flush=True,
            )
            return

        next_request = rag_pb2.RouteQueryRequest(
            query_id=query_id,
            query=request.query,
            top_k=request.top_k,
            coordinator_addr=coordinator_addr,
            curr_hop=curr_hop + 1,
            max_hops=max_hops,
            visited_worker_ids=visited_worker_ids,
        )
        await self.send_chain_query_to_target(target, next_request)

    async def choose_chain_hop_target(
        self,
        query: str,
        visited_worker_ids: set[str],
        query_id: str = "",
    ):
        if (
            not self.is_root_custodian()
            and self.worker_id not in self.closest_user_entries_by_worker
            and len(self.user_embedding_registry) <= 1
        ):
            await self.sync_user_embedding_registry_once(
                target_addr=self.assigned_routing_tree_custodian_addr
                or self.current_root_addr()
            )

        query_embedding = self.bert_embedder.embed_text(
            query,
            max_length=BERT_MAX_LENGTH,
        ).embedding
        registry_entries = self.closest_user_entries_by_worker.get(
            self.worker_id,
            list(self.user_embedding_registry.values()),
        )
        scored = []
        for entry in registry_entries:
            worker_id = entry.get("worker_id")
            embedding = entry.get("embedding") or []
            advertise_addr = entry.get("advertise_addr")
            if not worker_id or not advertise_addr or not embedding:
                continue
            if worker_id in visited_worker_ids:
                continue
            score = cosine_similarity(query_embedding, embedding)
            scored.append((score, worker_id, advertise_addr))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        score, worker_id, advertise_addr = scored[0]
        if score < QUERY_USER_MATCH_THRESHOLD:
            benchmark_events.emit(
                "chain_hop_no_user_match",
                query_id=query_id,
                best_worker_id=worker_id,
                best_addr=advertise_addr,
                best_score=float(score),
                user_threshold=QUERY_USER_MATCH_THRESHOLD,
                candidate_count=len(registry_entries),
            )
            print(
                f"[rag-worker {self.worker_id}] no chain-hop user match "
                f"best_worker_id={worker_id} best_addr={advertise_addr} "
                f"best_score={score:.4f} "
                f"user_threshold={QUERY_USER_MATCH_THRESHOLD:.4f} "
                f"candidate_count={len(registry_entries)}",
                flush=True,
            )
            return None

        print(
            f"[rag-worker {self.worker_id}] chain-hop selected target "
            f"worker_id={worker_id} addr={advertise_addr} score={score:.4f} "
            f"user_threshold={QUERY_USER_MATCH_THRESHOLD:.4f} "
            f"candidate_count={len(registry_entries)}",
            flush=True,
        )
        benchmark_events.emit(
            "chain_hop_target_selected",
            query_id=query_id,
            target_worker_id=worker_id,
            target_addr=advertise_addr,
            score=float(score),
            user_threshold=QUERY_USER_MATCH_THRESHOLD,
            candidate_count=len(registry_entries),
        )
        return advertise_addr

    async def send_chain_query_to_target(self, target, request):
        for attempt in range(1, MULTICAST_RETRY_ATTEMPTS + 1):
            try:
                async with grpc.aio.insecure_channel(target) as channel:
                    stub = rag_pb2_grpc.RagServiceStub(channel)
                    await stub.RouteQuery(request, timeout=5.0)
                benchmark_events.emit(
                    "chain_hop_sent",
                    query_id=request.query_id,
                    target_addr=target,
                    curr_hop=request.curr_hop,
                    max_hops=request.max_hops,
                    attempt=attempt,
                )
                print(
                    f"[rag-worker {self.worker_id}] chain-hop query_id={request.query_id} "
                    f"to={target} hop={request.curr_hop}/{request.max_hops} "
                    f"attempt={attempt}",
                    flush=True,
                )
                return True
            except Exception as exc:
                if attempt == MULTICAST_RETRY_ATTEMPTS:
                    print(
                        f"[rag-worker {self.worker_id}] failed chain-hop "
                        f"query_id={request.query_id} to={target} "
                        f"attempts={attempt}: {exc}",
                        flush=True,
                    )
                    return False
                print(
                    f"[rag-worker {self.worker_id}] chain-hop retry "
                    f"query_id={request.query_id} to={target} "
                    f"attempt={attempt}/{MULTICAST_RETRY_ATTEMPTS}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(MULTICAST_RETRY_SECONDS)

    async def sync_user_embedding_registry_once(self, target_addr: str = INIT_ADDR):
        try:
            async with grpc.aio.insecure_channel(target_addr) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                reply = await stub.GetUserEmbeddingRegistry(
                    rag_pb2.GetUserEmbeddingRegistryRequest(
                        requester_worker_id=self.worker_id,
                    ),
                    timeout=5.0,
                )
            synced_records = []
            for record in reply.users:
                if record.worker_id == self.worker_id:
                    continue
                if record.embedding:
                    synced_records.append(record)
            self.user_embedding_registry = {}
            own_registration = self.build_user_embedding_registration()
            if own_registration.embedding:
                self.user_embedding_registry[self.worker_id] = (
                    self.user_embedding_entry_from_registration(own_registration)
                )
            for record in synced_records:
                self.record_user_embedding_record(record)
            print(
                f"[rag-worker {self.worker_id}] synced user embedding registry "
                f"from={reply.worker_id}@{target_addr} "
                f"registry_size={len(self.user_embedding_registry)} "
                f"received={len(reply.users)}",
                flush=True,
            )
            return True
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] user embedding registry sync failed "
                f"target_addr={target_addr}: {exc}",
                flush=True,
            )
            return False

    def start_pageindex_retrieval_task(
        self,
        query_id: str,
        query: str,
        coordinator_addr: str,
        local_candidates,
    ):
        doc_ids = self._candidate_doc_ids(local_candidates)
        if not doc_ids:
            benchmark_events.emit(
                "pageindex_skipped",
                query_id=query_id,
                reason="no_doc_ids",
            )
            print(
                f"[rag-worker {self.worker_id}] no PageIndex doc_ids to query "
                f"for query_id={query_id}",
                flush=True,
            )
            return
        if not self.store.pageindex_api_key:
            benchmark_events.emit(
                "pageindex_skipped",
                query_id=query_id,
                reason="missing_api_key",
                doc_ids=doc_ids,
            )
            print(
                f"[rag-worker {self.worker_id}] PAGE_INDEX_API_KEY is missing; "
                f"skipping PageIndex retrieval for query_id={query_id}",
                flush=True,
            )
            return
        task = asyncio.create_task(
            self.retrieve_pageindex_documents(query_id, query, coordinator_addr, doc_ids)
        )
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def retrieve_and_send_pageindex_documents(
        self,
        query_id: str,
        query: str,
        coordinator_addr: str,
        local_candidates,
        reason: str,
    ) -> int:
        doc_ids = self._candidate_doc_ids(local_candidates)
        if not doc_ids:
            benchmark_events.emit(
                "pageindex_skipped",
                query_id=query_id,
                reason="no_doc_ids",
                retrieval_reason=reason,
            )
            print(
                f"[rag-worker {self.worker_id}] no PageIndex doc_ids to query "
                f"for query_id={query_id} reason={reason}",
                flush=True,
            )
            return 0
        if not self.store.pageindex_api_key:
            benchmark_events.emit(
                "pageindex_skipped",
                query_id=query_id,
                reason="missing_api_key",
                retrieval_reason=reason,
                doc_ids=doc_ids,
            )
            print(
                f"[rag-worker {self.worker_id}] PAGE_INDEX_API_KEY is missing; "
                f"skipping PageIndex retrieval for query_id={query_id} reason={reason}",
                flush=True,
            )
            return 0

        print(
            f"[rag-worker {self.worker_id}] starting PageIndex retrieval "
            f"query_id={query_id} doc_ids={doc_ids} reason={reason}",
            flush=True,
        )
        benchmark_events.emit(
            "pageindex_retrieval_started",
            query_id=query_id,
            doc_ids=doc_ids,
            retrieval_reason=reason,
        )
        try:
            evidence = await self.retrieve_pageindex_documents_once(
                query_id,
                query,
                doc_ids,
            )
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] PageIndex retrieval failed "
                f"query_id={query_id} doc_ids={doc_ids} reason={reason}: {exc}",
                flush=True,
            )
            benchmark_events.emit(
                "pageindex_retrieval_failed",
                query_id=query_id,
                doc_ids=doc_ids,
                retrieval_reason=reason,
                error=str(exc),
            )
            return 0

        print(
            f"[rag-worker {self.worker_id}] PageIndex retrieval evidence "
            f"query_id={query_id} doc_ids={doc_ids} reason={reason} "
            f"count={len(evidence)}",
            flush=True,
        )
        for item in evidence:
            print(
                f"[rag-worker {self.worker_id}] local evidence query_id={query_id} "
                f"node={item.node_id} title={item.title} metadata={item.metadata_json}",
                flush=True,
            )
            print(
                f"[rag-worker {self.worker_id}] local evidence content "
                f"query_id={query_id} node={item.node_id}: {item.content}",
                flush=True,
            )
        if evidence:
            await self.send_evidence(
                coordinator_addr,
                query_id,
                evidence,
            )
        benchmark_events.emit(
            "pageindex_retrieval_completed",
            query_id=query_id,
            doc_ids=doc_ids,
            retrieval_reason=reason,
            evidence_count=len(evidence),
        )
        return len(evidence)

    def _candidate_doc_ids(self, local_candidates):
        doc_ids = []
        seen = set()
        for _, node in local_candidates:
            doc_id = node.metadata.get("doc_id")
            if not doc_id or doc_id in seen:
                continue
            doc_ids.append(doc_id)
            seen.add(doc_id)
            if len(doc_ids) >= PAGEINDEX_QUERY_TOP_K:
                break
        return doc_ids

    async def retrieve_pageindex_documents(
        self,
        query_id: str,
        query: str,
        coordinator_addr: str,
        doc_ids: list[str],
    ):
        print(
            f"[rag-worker {self.worker_id}] starting PageIndex retrieval "
            f"query_id={query_id} doc_ids={doc_ids} reason=threshold_match",
            flush=True,
        )
        benchmark_events.emit(
            "pageindex_retrieval_started",
            query_id=query_id,
            doc_ids=doc_ids,
            retrieval_reason="threshold_match",
        )
        try:
            evidence = await self.retrieve_pageindex_documents_once(
                query_id,
                query,
                doc_ids,
            )
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] PageIndex retrieval failed "
                f"query_id={query_id} doc_ids={doc_ids}: {exc}",
                flush=True,
            )
            benchmark_events.emit(
                "pageindex_retrieval_failed",
                query_id=query_id,
                doc_ids=doc_ids,
                retrieval_reason="threshold_match",
                error=str(exc),
            )
            return
        print(
            f"[rag-worker {self.worker_id}] PageIndex retrieval evidence "
            f"query_id={query_id} doc_ids={doc_ids} count={len(evidence)}",
            flush=True,
        )
        for item in evidence:
            print(
                f"[rag-worker {self.worker_id}] local evidence query_id={query_id} "
                f"node={item.node_id} title={item.title} metadata={item.metadata_json}",
                flush=True,
            )
            print(
                f"[rag-worker {self.worker_id}] local evidence content "
                f"query_id={query_id} node={item.node_id}: {item.content}",
                flush=True,
            )
        await self.send_evidence(
            coordinator_addr,
            query_id,
            evidence,
        )
        benchmark_events.emit(
            "pageindex_retrieval_completed",
            query_id=query_id,
            doc_ids=doc_ids,
            retrieval_reason="threshold_match",
            evidence_count=len(evidence),
        )

    async def send_evidence(self, coordinator_addr: str, query_id: str, evidence):
        try:
            async with grpc.aio.insecure_channel(coordinator_addr) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                reply = await stub.SendEvidence(
                    rag_pb2.SendEvidenceRequest(
                        query_id=query_id,
                        evidence=evidence,
                    ),
                    timeout=5.0,
                )
            print(
                f"[rag-worker {self.worker_id}] sent evidence query_id={query_id} "
                f"coordinator={coordinator_addr} accepted={reply.accepted_count}",
                flush=True,
            )
            benchmark_events.emit(
                "evidence_sent",
                query_id=query_id,
                coordinator_addr=coordinator_addr,
                evidence_count=len(evidence),
                accepted_count=reply.accepted_count,
            )
            return True
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] failed to send evidence "
                f"query_id={query_id} coordinator={coordinator_addr}: {exc}",
                flush=True,
            )
            benchmark_events.emit(
                "evidence_send_failed",
                query_id=query_id,
                coordinator_addr=coordinator_addr,
                evidence_count=len(evidence),
                error=str(exc),
            )
            return False

    async def retrieve_pageindex_documents_once(
        self,
        query_id: str,
        query: str,
        doc_ids: list[str],
    ):
        evidence = []
        for doc_id in doc_ids:
            retrieval_id, result = await asyncio.to_thread(
                self.retrieve_pageindex_document_blocking,
                doc_id,
                query,
            )
            evidence.extend(
                self.evidence_from_retrieval_result(
                    query_id,
                    doc_id,
                    retrieval_id,
                    result,
                )
            )
        return evidence

    def retrieve_pageindex_document_blocking(self, doc_id: str, query: str):
        from pageindex import PageIndexClient

        client = PageIndexClient(api_key=self.store.pageindex_api_key)
        print(
            f"[rag-worker {self.worker_id}] checking PageIndex retrieval readiness "
            f"doc_id={doc_id}",
            flush=True,
        )
        if not client.is_retrieval_ready(doc_id):
            raise RuntimeError(f"PageIndex doc_id={doc_id} is not ready for retrieval")

        print(
            f"[rag-worker {self.worker_id}] submitting PageIndex retrieval "
            f"doc_id={doc_id}",
            flush=True,
        )
        payload = client.submit_query(doc_id=doc_id, query=query, thinking=False)
        retrieval_id = payload.get("retrieval_id")
        if not retrieval_id:
            raise ValueError(f"PageIndex retrieval response missing retrieval_id: {payload}")
        print(
            f"[rag-worker {self.worker_id}] submitted PageIndex retrieval "
            f"doc_id={doc_id} retrieval_id={retrieval_id}",
            flush=True,
        )

        result = self.wait_for_pageindex_retrieval_blocking(client, retrieval_id)
        return retrieval_id, result

    def wait_for_pageindex_retrieval_blocking(self, client, retrieval_id: str):
        import time

        deadline = time.monotonic() + PAGEINDEX_RETRIEVAL_TIMEOUT_SECONDS
        last_payload = None
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            print(
                f"[rag-worker {self.worker_id}] polling PageIndex retrieval "
                f"retrieval_id={retrieval_id} attempt={attempt}",
                flush=True,
            )
            payload = client.get_retrieval(retrieval_id)
            last_payload = payload
            if isinstance(payload, list):
                print(
                    f"[rag-worker {self.worker_id}] PageIndex retrieval returned list "
                    f"retrieval_id={retrieval_id} nodes={len(payload)}",
                    flush=True,
                )
                return {
                    "retrieval_id": retrieval_id,
                    "status": "completed",
                    "retrieved_nodes": payload,
                }
            status = payload.get("status")
            print(
                f"[rag-worker {self.worker_id}] PageIndex retrieval status "
                f"retrieval_id={retrieval_id} status={status}",
                flush=True,
            )
            if status == "completed":
                print(
                    f"[rag-worker {self.worker_id}] PageIndex retrieval payload "
                    f"retrieval_id={retrieval_id}: "
                    f"{self._compact_text(json.dumps(payload, sort_keys=True), PAGEINDEX_RETRIEVAL_DEBUG_CHARS)}",
                    flush=True,
                )
                return payload
            if status in {"failed", "error"}:
                raise RuntimeError(f"PageIndex retrieval failed: {payload}")
            time.sleep(PAGEINDEX_RETRIEVAL_POLL_SECONDS)
        raise TimeoutError(
            f"Timed out waiting for PageIndex retrieval_id={retrieval_id}: {last_payload}"
        )

    def evidence_from_retrieval_result(
        self,
        query_id: str,
        doc_id: str,
        retrieval_id: str,
        result,
        ):
        evidence = []
        retrieved_nodes = result.get("retrieved_nodes") or []
        retrieved_nodes = self._unwrap_one_level(retrieved_nodes)
        for index, node in enumerate(retrieved_nodes):
            if not isinstance(node, dict):
                print(
                    f"[rag-worker {self.worker_id}] skipping unexpected PageIndex "
                    f"retrieved node type={type(node).__name__} value={node}",
                    flush=True,
                )
                continue
            relevant_contents = node.get("relevant_contents") or []
            relevant_contents = self._unwrap_one_level(relevant_contents)
            content_parts = []
            page_indexes = []
            for item in relevant_contents:
                if not isinstance(item, dict):
                    print(
                        f"[rag-worker {self.worker_id}] skipping unexpected PageIndex "
                        f"content item type={type(item).__name__} value={item}",
                        flush=True,
                    )
                    continue
                page_index = item.get("page_index")
                if page_index is not None:
                    page_indexes.append(page_index)
                relevant_content = item.get("relevant_content")
                if relevant_content:
                    content_parts.append(str(relevant_content))
            content = "\n\n".join(content_parts)
            if not content:
                content = json.dumps(node, sort_keys=True)
            node_id = node.get("node_id") or f"{retrieval_id}:{index}"
            evidence.append(
                rag_pb2.Evidence(
                    worker_id=self.worker_id,
                    node_id=str(node_id),
                    title=node.get("title") or f"PageIndex retrieval node {index + 1}",
                    content=self._compact_text(content),
                    metadata_json=json.dumps(
                        {
                            "doc_id": doc_id,
                            "retrieval_id": retrieval_id,
                            "query_id": query_id,
                            "source": "pageindex_retrieval",
                            "page_indexes": page_indexes,
                        },
                        sort_keys=True,
                    ),
                )
            )
        return evidence

    def _unwrap_one_level(self, items):
        unwrapped = []
        for item in items:
            if isinstance(item, list):
                unwrapped.extend(item)
            else:
                unwrapped.append(item)
        return unwrapped

    def start_summary_task(self, query_id: str):
        if query_id not in self.query_by_id or query_id in self.summary_tasks:
            return
        task = asyncio.create_task(self.summarize_evidence_after_delay(query_id))
        self.summary_tasks[query_id] = task
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def summarize_evidence_after_delay(self, query_id: str):
        await asyncio.sleep(COORDINATOR_SUMMARY_DELAY_SECONDS)
        query = self.query_by_id.get(query_id)
        evidence = self.evidence_by_query.get(query_id, [])
        if not query:
            self.summary_tasks.pop(query_id, None)
            print(
                f"[rag-worker {self.worker_id}] no query to summarize "
                f"query_id={query_id}",
                flush=True,
            )
            return
        if not evidence:
            self.summary_tasks.pop(query_id, None)
            print(
                f"[rag-worker {self.worker_id}] final answer query_id={query_id}: "
                f"{NOT_FOUND_ANSWER}",
                flush=True,
            )
            benchmark_events.emit(
                "final_answer",
                query_id=query_id,
                answer=NOT_FOUND_ANSWER,
                evidence_count=0,
                not_found=True,
                llm_called=False,
            )
            return

        context = self.build_summary_context(evidence)
        messages = [
            {
                "role": "system",
                "content": (
                    "Answer the query using only the provided distributed evidence. "
                    "Be concise. If the evidence contains conflicting values, mention them."
                ),
            },
            {
                "role": "user",
                "content": f"Query: {query}\n\nDistributed evidence:\n{context}",
            },
        ]
        try:
            if self.llm_client is None:
                self.llm_client = build_inference_client_from_env()
            answer = await self.llm_client.get_text(messages)
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] final answer failed "
                f"query_id={query_id}: {exc}",
                flush=True,
            )
            benchmark_events.emit(
                "final_answer_failed",
                query_id=query_id,
                evidence_count=len(evidence),
                error=str(exc),
            )
            return

        print(
            f"[rag-worker {self.worker_id}] final answer query_id={query_id}: "
            f"{answer}",
            flush=True,
        )
        benchmark_events.emit(
            "final_answer",
            query_id=query_id,
            answer=answer,
            evidence_count=len(evidence),
            not_found=False,
            llm_called=True,
        )

    def build_summary_context(self, evidence):
        parts = []
        for index, item in enumerate(evidence, start=1):
            parts.append(
                f"[Evidence {index}]\n"
                f"worker_id: {item.worker_id}\n"
                f"node_id: {item.node_id}\n"
                f"title: {item.title}\n"
                f"metadata: {item.metadata_json}\n"
                f"content: {item.content}"
            )
        return self._compact_text(
            "\n\n".join(parts),
            max_chars=COORDINATOR_CONTEXT_MAX_CHARS,
        )

    def _compact_text(self, text: str, max_chars: int = 1200):
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + "...<truncated>"

    async def broadcast_query(self, request):
        if not request.coordinator_addr:
            request = rag_pb2.RouteQueryRequest(
                query_id=request.query_id,
                query=request.query,
                top_k=request.top_k,
                coordinator_addr=ADVERTISE_ADDR,
            )
        if request.coordinator_addr == ADVERTISE_ADDR:
            self.query_by_id[request.query_id] = request.query
        results = await asyncio.gather(
            *(self.send_query_to_neighbor(target, request) for target in self.neighbors),
            return_exceptions=True,
        )
        return sum(1 for result in results if result is True)

    async def send_query_to_neighbor(self, target, request):
        for attempt in range(1, MULTICAST_RETRY_ATTEMPTS + 1):
            try:
                async with grpc.aio.insecure_channel(target) as channel:
                    stub = rag_pb2_grpc.RagServiceStub(channel)
                    await stub.RouteQuery(request, timeout=5.0)
                print(
                    f"[rag-worker {self.worker_id}] multicast query_id={request.query_id} "
                    f"to={target} attempt={attempt}",
                    flush=True,
                )
                return True
            except Exception as exc:
                if attempt == MULTICAST_RETRY_ATTEMPTS:
                    print(
                        f"[rag-worker {self.worker_id}] failed multicast query_id={request.query_id} "
                        f"to={target} attempts={attempt}: {exc}",
                        flush=True,
                    )
                    return False
                print(
                    f"[rag-worker {self.worker_id}] multicast retry query_id={request.query_id} "
                    f"to={target} attempt={attempt}/{MULTICAST_RETRY_ATTEMPTS}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(MULTICAST_RETRY_SECONDS)



async def serve():
    print(
        f"[rag-worker {WORKER_ID}] loading BERT embedder model={BERT_MODEL} "
        f"device={BERT_DEVICE or 'auto'}",
        flush=True,
    )
    bert_embedder = BertEmbedder(model_name=BERT_MODEL, device=BERT_DEVICE)
    print(
        f"[rag-worker {WORKER_ID}] loaded BERT embedder model={bert_embedder.model_name} "
        f"device={bert_embedder.device}",
        flush=True,
    )

    store = LocalDocumentStore(WORKER_ID, DOC_DIR)
    store.load()
    embedded_roots = store.embed_root_pageindex_nodes(
        bert_embedder,
        max_length=BERT_MAX_LENGTH,
    )
    print(
        f"[rag-worker {WORKER_ID}] loaded {len(store.nodes)} PageIndex-style nodes from {DOC_DIR}",
        flush=True,
    )
    print(
        f"[rag-worker {WORKER_ID}] embedded {len(embedded_roots)} PageIndex root nodes",
        flush=True,
    )
    print(
        f"[rag-worker {WORKER_ID}] user embedding "
        f"dimension={store.user_embedding_dimension} "
        f"source_roots={len(store.user_embedding_source_node_ids)}",
        flush=True,
    )

    servicer = RagWorkerServicer(WORKER_ID, store, NEIGHBORS, bert_embedder)
    servicer.record_user_embedding(servicer.build_user_embedding_registration())

    server = grpc.aio.server()
    rag_pb2_grpc.add_RagServiceServicer_to_server(
        servicer,
        server,
    )

    service_names = (
        rag_pb2.DESCRIPTOR.services_by_name["RagService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)

    listen_addr = f"{HOST}:{PORT}"
    server.add_insecure_port(listen_addr)
    print(
        f"[rag-worker {WORKER_ID}] listening on {listen_addr} "
        f"routing_mode={ROUTING_MODE.value} neighbors={NEIGHBORS}",
        flush=True,
    )
    await server.start()

    if FORCE_LEADER_ID:
        elected_leader_id: str | None = FORCE_LEADER_ID
        is_leader = WORKER_ID == FORCE_LEADER_ID
        print(
            f"[rag-worker {WORKER_ID}] forced leader: leader={elected_leader_id} "
            f"is_leader={is_leader} (RAG_FORCE_LEADER_ID set, skipping DynamoDB election)",
            flush=True,
        )
    else:
        global leader_election
        leader_election = LeaderElection(WORKER_ID, LEASE_DURATION_SECONDS)
        await leader_election.start()
        elected_leader_id = await leader_election.wait_for_leader(timeout=30.0)
        is_leader = leader_election.is_leader()
        print(
            f"[rag-worker {WORKER_ID}] leader election result: "
            f"leader={elected_leader_id} is_leader={is_leader}",
            flush=True,
        )

    if is_leader:
        await servicer.handle_routing_tree_join(
            servicer.build_routing_tree_join_request(),
            servicer.routing_tree_root_node_id,
        )

    effective_init_addr = addr_for_worker_id(elected_leader_id)
    start_routing_startup_tasks(servicer, effective_init_addr)
    if BOOTSTRAP_QUERY and (BOOTSTRAP_DELAY_SECONDS > 0.0):
        asyncio.create_task(inject_bootstrap_query(servicer))
    await server.wait_for_termination()
    if leader_election is not None:
        await leader_election.stop()


def start_routing_startup_tasks(servicer: RagWorkerServicer, init_addr: str = INIT_ADDR):
    print(
        f"[rag-worker {WORKER_ID}] routing-tree startup selected",
        flush=True,
    )
    asyncio.create_task(join_routing_tree(servicer, init_addr))


async def join_routing_tree(servicer: RagWorkerServicer, init_addr: str = INIT_ADDR):
    if servicer.is_root_custodian():
        print(
            f"[rag-worker {WORKER_ID}] root custodian ready for routing tree "
            f"joined={len(servicer.joined_tree_users)}/{ROUTING_TREE_EXPECTED_USERS}",
            flush=True,
        )
        return

    request = servicer.build_routing_tree_join_request()
    for attempt in range(1, USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS + 1):
        try:
            async with grpc.aio.insecure_channel(init_addr) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                reply = await stub.JoinTree(request, timeout=5.0)
            if not reply.accepted:
                raise RuntimeError(
                    f"root {reply.root_worker_id} rejected JoinTree request"
                )
            for record in reply.closest_users:
                servicer.record_user_embedding_record(record)
            servicer.assigned_routing_tree_node_id = reply.tree_node_id
            servicer.assigned_routing_tree_custodian_worker_id = reply.custodian_worker_id
            servicer.assigned_routing_tree_custodian_addr = reply.custodian_addr or init_addr
            print(
                f"[rag-worker {WORKER_ID}] joined routing tree "
                f"root={reply.root_worker_id} addr={init_addr} "
                f"leaf={reply.tree_node_id} custodian="
                f"{reply.custodian_worker_id}@{reply.custodian_addr or init_addr} "
                f"joined={reply.joined_count}/{reply.expected_count} "
                f"routing_epoch={reply.routing_epoch} "
                f"closest_users={len(reply.closest_users)} attempt={attempt}",
                flush=True,
            )
            asyncio.create_task(
                sync_routing_tree_closest_users(
                    servicer,
                    servicer.assigned_routing_tree_custodian_addr,
                )
            )
            return
        except Exception as exc:
            if attempt == USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS:
                print(
                    f"[rag-worker {WORKER_ID}] failed to join routing tree "
                    f"root_addr={init_addr} attempts={attempt}: {exc}",
                    flush=True,
                )
                return
            print(
                f"[rag-worker {WORKER_ID}] retrying routing-tree join "
                f"root_addr={init_addr} attempt={attempt}/"
                f"{USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS}: {exc}",
                flush=True,
            )
            await asyncio.sleep(USER_EMBEDDING_REGISTER_RETRY_SECONDS)


async def sync_routing_tree_closest_users(
    servicer: RagWorkerServicer,
    target_addr: str = INIT_ADDR,
):
    if servicer.is_root_custodian():
        return

    for attempt in range(1, USER_EMBEDDING_SYNC_RETRY_ATTEMPTS + 1):
        ok = await servicer.sync_user_embedding_registry_once(target_addr=target_addr)
        if ok and len(servicer.user_embedding_registry) > 1:
            print(
                f"[rag-worker {WORKER_ID}] synced routing-tree closest-users "
                f"from={target_addr} count={len(servicer.user_embedding_registry) - 1} "
                f"attempt={attempt}",
                flush=True,
            )
            return
        if attempt == USER_EMBEDDING_SYNC_RETRY_ATTEMPTS:
            print(
                f"[rag-worker {WORKER_ID}] routing-tree closest-users sync timed out "
                f"target_addr={target_addr} attempts={attempt}",
                flush=True,
            )
            return
        await asyncio.sleep(USER_EMBEDDING_SYNC_RETRY_SECONDS)
async def inject_bootstrap_query(servicer: RagWorkerServicer):
    await asyncio.sleep(BOOTSTRAP_DELAY_SECONDS)
    if BOOTSTRAP_WAIT_FOR_ROUTING_TREE:
        await wait_for_bootstrap_routing_tree(servicer)
    query_id = BOOTSTRAP_QUERY_ID or str(uuid.uuid4())
    request = rag_pb2.RouteQueryRequest(
        query_id=query_id,
        query=BOOTSTRAP_QUERY,
        top_k=5,
        coordinator_addr=ADVERTISE_ADDR,
        curr_hop=0,
        max_hops=CHAIN_HOP_MAX_HOPS,
    )
    print(
        f"[rag-worker {WORKER_ID}] injecting bootstrap query_id={query_id} "
        f"query={BOOTSTRAP_QUERY}",
        flush=True,
    )
    await servicer.RouteQuery(request, context=None)

async def wait_for_bootstrap_routing_tree(servicer: RagWorkerServicer):
    if not servicer.is_root_custodian():
        return

    deadline = asyncio.get_running_loop().time() + BOOTSTRAP_ROUTING_TREE_TIMEOUT_SECONDS
    while (
        len(servicer.joined_tree_users) < ROUTING_TREE_EXPECTED_USERS
        or servicer.routing_epoch == 0
    ):
        if servicer.routing_epoch == 0:
            servicer.refresh_routing_tree_metadata()
        if (
            len(servicer.joined_tree_users) >= ROUTING_TREE_EXPECTED_USERS
            and servicer.routing_epoch > 0
        ):
            break
        if asyncio.get_running_loop().time() >= deadline:
            print(
                f"[rag-worker {WORKER_ID}] bootstrap routing-tree wait timed out "
                f"joined={len(servicer.joined_tree_users)}/"
                f"{ROUTING_TREE_EXPECTED_USERS} routing_epoch={servicer.routing_epoch}; "
                "injecting query anyway",
                flush=True,
            )
            return
        print(
            f"[rag-worker {WORKER_ID}] waiting for routing tree before bootstrap "
            f"joined={len(servicer.joined_tree_users)}/"
            f"{ROUTING_TREE_EXPECTED_USERS} routing_epoch={servicer.routing_epoch}",
            flush=True,
        )
        await asyncio.sleep(2.0)

    print(
        f"[rag-worker {WORKER_ID}] routing tree ready for bootstrap "
        f"joined={len(servicer.joined_tree_users)}/"
        f"{ROUTING_TREE_EXPECTED_USERS} routing_epoch={servicer.routing_epoch}",
        flush=True,
    )


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()
