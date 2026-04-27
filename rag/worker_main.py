import asyncio
import json
import os
import uuid

import grpc
from grpc_reflection.v1alpha import reflection

from rag import rag_pb2, rag_pb2_grpc
from rag.bert_embedder import BertEmbedder, DEFAULT_BERT_MODEL
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
BOOTSTRAP_QUERY = os.getenv("RAG_BOOTSTRAP_QUERY", "")
BOOTSTRAP_DELAY_SECONDS = float(os.getenv("RAG_BOOTSTRAP_DELAY_SECONDS", "2.0"))
BOOTSTRAP_QUERY_ID = os.getenv("RAG_BOOTSTRAP_QUERY_ID", "")
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

    async def Ping(self, request, context):
        return rag_pb2.RagPingReply(worker_id=self.worker_id, status="alive")

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
        self.start_summary_task(request.query_id)
        return rag_pb2.SendEvidenceReply(
            worker_id=self.worker_id,
            accepted_count=len(request.evidence),
        )

    async def RouteQuery(self, request, context):
        query_id = request.query_id or str(uuid.uuid4())
        coordinator_addr = request.coordinator_addr or ADVERTISE_ADDR
        if coordinator_addr == ADVERTISE_ADDR:
            self.query_by_id[query_id] = request.query

        print(
            f"[rag-worker {self.worker_id}] received query_id={query_id} "
            f"coordinator={coordinator_addr} query={request.query}",
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

        self.start_pageindex_retrieval_task(
            query_id,
            request.query,
            coordinator_addr,
            local_candidates,
        )
        return rag_pb2.RouteQueryReply(
            worker_id=self.worker_id,
            candidates=candidates,
        )

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

    servicer = RagWorkerServicer(WORKER_ID, store, NEIGHBORS, bert_embedder)
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
    if BOOTSTRAP_QUERY:
        asyncio.create_task(inject_bootstrap_query(servicer))
    await server.wait_for_termination()


async def inject_bootstrap_query(servicer: RagWorkerServicer):
    await asyncio.sleep(BOOTSTRAP_DELAY_SECONDS)
    query_id = BOOTSTRAP_QUERY_ID or str(uuid.uuid4())
    request = rag_pb2.RouteQueryRequest(
        query_id=query_id,
        query=BOOTSTRAP_QUERY,
        top_k=5,
        coordinator_addr=ADVERTISE_ADDR,
    )
    print(
        f"[rag-worker {WORKER_ID}] injecting bootstrap query_id={query_id} "
        f"query={BOOTSTRAP_QUERY}",
        flush=True,
    )
    await servicer.RouteQuery(request, context=None)
    sent_count = await servicer.broadcast_query(request)
    print(
        f"[rag-worker {WORKER_ID}] broadcast query_id={query_id} sent_count={sent_count}",
        flush=True,
    )


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()
