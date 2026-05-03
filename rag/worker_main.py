import asyncio
import json
import os
import uuid

import grpc
from grpc_reflection.v1alpha import reflection

from rag import rag_pb2, rag_pb2_grpc
from rag.bert_embedder import BertEmbedder, DEFAULT_BERT_MODEL, cosine_similarity
from rag.document_store import LocalDocumentStore
from rag.llm_client import build_inference_client_from_env


WORKER_ID = os.getenv("RAG_WORKER_ID", os.getenv("HOSTNAME", "rag-worker"))
HOST = os.getenv("RAG_HOST", "0.0.0.0")
PORT = int(os.getenv("RAG_PORT", "9100"))
ADVERTISE_ADDR = os.getenv("RAG_ADVERTISE_ADDR", f"{WORKER_ID}:{PORT}")
DOC_DIR = os.getenv("RAG_DOC_DIR", "/data/rag")
BERT_MODEL = os.getenv("RAG_BERT_MODEL", DEFAULT_BERT_MODEL)
BERT_DEVICE = os.getenv("RAG_BERT_DEVICE") or None
BERT_MAX_LENGTH = int(os.getenv("RAG_BERT_MAX_LENGTH", "512"))
PAGEINDEX_QUERY_TOP_K = int(os.getenv("RAG_PAGEINDEX_QUERY_TOP_K", "3"))
LOCAL_MATCH_THRESHOLD = float(os.getenv("RAG_LOCAL_MATCH_THRESHOLD", "0.67"))
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
ROUTING_TREE_RECORD_LIMIT = int(os.getenv("RAG_ROUTING_TREE_RECORD_LIMIT", "50"))
ROUTING_TREE_DELTA = float(os.getenv("RAG_ROUTING_TREE_DELTA", "0.0005"))
ROUTING_TREE_CLOSEST_USERS = int(os.getenv("RAG_ROUTING_TREE_CLOSEST_USERS", "2"))
ROUTING_TREE_CANDIDATE_USERS = int(os.getenv("RAG_ROUTING_TREE_CANDIDATE_USERS", "0"))
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

    async def Ping(self, request, context):
        return rag_pb2.RagPingReply(worker_id=self.worker_id, status="alive")

    async def RegisterUserEmbedding(self, request, context):
        self.record_user_embedding(request)
        self.maybe_build_routing_tree()
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

    def maybe_build_routing_tree(self):
        if self.worker_id != INIT_WORKER_ID:
            return
        registered_count = len(self.user_embedding_registry)
        if registered_count < ROUTING_TREE_EXPECTED_USERS:
            print(
                f"[rag-worker {self.worker_id}] waiting to build routing tree "
                f"registered={registered_count}/{ROUTING_TREE_EXPECTED_USERS}",
                flush=True,
            )
            return

        usable_entries = [
            entry
            for entry in self.user_embedding_registry.values()
            if entry.get("embedding")
        ]
        worker_ids = {entry["worker_id"] for entry in usable_entries}
        if not usable_entries:
            print(
                f"[rag-worker {self.worker_id}] cannot build routing tree; "
                "no registered users have embeddings",
                flush=True,
            )
            return
        if worker_ids == self.routing_tree_built_for_workers:
            return

        try:
            from Semantica.tree.script import construct_rag_routing_tree

            records = [
                {
                    "worker_id": entry["worker_id"],
                    "addr": entry["advertise_addr"],
                    "embedding": entry["embedding"],
                }
                for entry in usable_entries
            ]
            result = construct_rag_routing_tree(
                records,
                record_limit_per_leafnode=ROUTING_TREE_RECORD_LIMIT,
                delta=ROUTING_TREE_DELTA,
                closest_user_count=ROUTING_TREE_CLOSEST_USERS,
                candidate_user_count=(
                    ROUTING_TREE_CANDIDATE_USERS
                    if ROUTING_TREE_CANDIDATE_USERS > 0
                    else None
                ),
            )
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] failed to build routing tree: {exc}",
                flush=True,
            )
            return

        self.routing_epoch += 1
        self.routing_tree_result = result
        self.routing_tree_built_for_workers = worker_ids
        self.closest_user_entries_by_worker = {}
        closest_users = result["closest_users"]
        for owner_worker_id, closest_records in closest_users.items():
            entries = []
            for closest in closest_records:
                closest_worker_id = closest["worker_id"]
                entry = self.user_embedding_registry.get(closest_worker_id)
                if entry is not None:
                    entries.append(entry)
            self.closest_user_entries_by_worker[owner_worker_id] = entries

        print(
            f"[rag-worker {self.worker_id}] built Semantica routing tree "
            f"epoch={self.routing_epoch} users={len(worker_ids)} "
            f"leaves={len(result['leaf_members'])} "
            f"record_limit={ROUTING_TREE_RECORD_LIMIT} "
            f"closest_k={ROUTING_TREE_CLOSEST_USERS}",
            flush=True,
        )
        print(
            f"[rag-worker {self.worker_id}] routing tree leaf_members="
            f"{json.dumps(result['leaf_members'], sort_keys=True)}",
            flush=True,
        )
        for owner_worker_id, entries in sorted(self.closest_user_entries_by_worker.items()):
            print(
                f"[rag-worker {self.worker_id}] routing closest-users "
                f"epoch={self.routing_epoch} worker_id={owner_worker_id} "
                f"closest={[entry['worker_id'] for entry in entries]}",
                flush=True,
            )

    async def SendEvidence(self, request, context):
        evidence = self.evidence_by_query.setdefault(request.query_id, [])
        evidence.extend(request.evidence)
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

        matched_candidates = [
            (score, node)
            for score, node in local_candidates
            if score >= LOCAL_MATCH_THRESHOLD
        ]
        if matched_candidates:
            print(
                f"[rag-worker {self.worker_id}] local match query_id={query_id} "
                f"threshold={LOCAL_MATCH_THRESHOLD:.4f} "
                f"matches={len(matched_candidates)}/{len(local_candidates)}",
                flush=True,
            )
        else:
            best_score = local_candidates[0][0] if local_candidates else None
            best_score_text = f"{best_score:.4f}" if best_score is not None else "none"
            print(
                f"[rag-worker {self.worker_id}] no local match query_id={query_id} "
                f"threshold={LOCAL_MATCH_THRESHOLD:.4f} best_score={best_score_text}",
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
        )
        if target is None:
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

    async def choose_chain_hop_target(self, query: str, visited_worker_ids: set[str]):
        if self.worker_id != INIT_WORKER_ID:
            await self.sync_user_embedding_registry_once()

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
        print(
            f"[rag-worker {self.worker_id}] chain-hop selected target "
            f"worker_id={worker_id} addr={advertise_addr} score={score:.4f} "
            f"candidate_count={len(registry_entries)}",
            flush=True,
        )
        return advertise_addr

    async def send_chain_query_to_target(self, target, request):
        for attempt in range(1, MULTICAST_RETRY_ATTEMPTS + 1):
            try:
                async with grpc.aio.insecure_channel(target) as channel:
                    stub = rag_pb2_grpc.RagServiceStub(channel)
                    await stub.RouteQuery(request, timeout=5.0)
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

    async def sync_user_embedding_registry_once(self):
        try:
            async with grpc.aio.insecure_channel(INIT_ADDR) as channel:
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
                f"from init={reply.worker_id} registry_size={len(self.user_embedding_registry)} "
                f"received={len(reply.users)}",
                flush=True,
            )
            return True
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] user embedding registry sync failed "
                f"init_addr={INIT_ADDR}: {exc}",
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
            print(
                f"[rag-worker {self.worker_id}] no PageIndex doc_ids to query "
                f"for query_id={query_id}",
                flush=True,
            )
            return
        if not self.store.pageindex_api_key:
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
            f"query_id={query_id} doc_ids={doc_ids}",
            flush=True,
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
            return True
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] failed to send evidence "
                f"query_id={query_id} coordinator={coordinator_addr}: {exc}",
                flush=True,
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
        if not query or not evidence:
            self.summary_tasks.pop(query_id, None)
            print(
                f"[rag-worker {self.worker_id}] no evidence to summarize "
                f"query_id={query_id}",
                flush=True,
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
            return

        print(
            f"[rag-worker {self.worker_id}] final answer query_id={query_id}: "
            f"{answer}",
            flush=True,
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
    servicer.maybe_build_routing_tree()

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
        f"[rag-worker {WORKER_ID}] listening on {listen_addr} neighbors={NEIGHBORS}",
        flush=True,
    )
    await server.start()
    asyncio.create_task(register_user_embedding_with_init(servicer))
    asyncio.create_task(sync_user_embedding_registry_from_init(servicer))
    if BOOTSTRAP_QUERY and (BOOTSTRAP_DELAY_SECONDS > 0.0):
        asyncio.create_task(inject_bootstrap_query(servicer))
    await server.wait_for_termination()


async def register_user_embedding_with_init(servicer: RagWorkerServicer):
    if WORKER_ID == INIT_WORKER_ID:
        return

    request = servicer.build_user_embedding_registration()
    for attempt in range(1, USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS + 1):
        try:
            async with grpc.aio.insecure_channel(INIT_ADDR) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                reply = await stub.RegisterUserEmbedding(request, timeout=5.0)
            print(
                f"[rag-worker {WORKER_ID}] registered user embedding with init "
                f"addr={INIT_ADDR} init_worker={reply.worker_id} "
                f"registered_count={reply.registered_count} attempt={attempt}",
                flush=True,
            )
            return
        except Exception as exc:
            if attempt == USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS:
                print(
                    f"[rag-worker {WORKER_ID}] failed to register user embedding "
                    f"with init addr={INIT_ADDR} attempts={attempt}: {exc}",
                    flush=True,
                )
                return
            print(
                f"[rag-worker {WORKER_ID}] retrying user embedding registration "
                f"addr={INIT_ADDR} attempt={attempt}/"
                f"{USER_EMBEDDING_REGISTER_RETRY_ATTEMPTS}: {exc}",
                flush=True,
            )
            await asyncio.sleep(USER_EMBEDDING_REGISTER_RETRY_SECONDS)


async def sync_user_embedding_registry_from_init(servicer: RagWorkerServicer):
    if WORKER_ID == INIT_WORKER_ID:
        return

    for attempt in range(1, USER_EMBEDDING_SYNC_RETRY_ATTEMPTS + 1):
        ok = await servicer.sync_user_embedding_registry_once()
        if ok:
            return
        if attempt == USER_EMBEDDING_SYNC_RETRY_ATTEMPTS:
            print(
                f"[rag-worker {WORKER_ID}] failed to sync user embedding registry "
                f"from init addr={INIT_ADDR} attempts={attempt}",
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
    if WORKER_ID != INIT_WORKER_ID:
        return

    deadline = asyncio.get_running_loop().time() + BOOTSTRAP_ROUTING_TREE_TIMEOUT_SECONDS
    while servicer.routing_epoch == 0:
        servicer.maybe_build_routing_tree()
        if servicer.routing_epoch > 0:
            break
        if asyncio.get_running_loop().time() >= deadline:
            print(
                f"[rag-worker {WORKER_ID}] bootstrap routing-tree wait timed out "
                f"registered={len(servicer.user_embedding_registry)}/"
                f"{ROUTING_TREE_EXPECTED_USERS}; injecting query anyway",
                flush=True,
            )
            return
        print(
            f"[rag-worker {WORKER_ID}] waiting for routing tree before bootstrap "
            f"registered={len(servicer.user_embedding_registry)}/"
            f"{ROUTING_TREE_EXPECTED_USERS}",
            flush=True,
        )
        await asyncio.sleep(2.0)

    print(
        f"[rag-worker {WORKER_ID}] routing tree ready for bootstrap "
        f"epoch={servicer.routing_epoch}",
        flush=True,
    )


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()
