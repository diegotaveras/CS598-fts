import asyncio
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
DOC_DIR = os.getenv("RAG_DOC_DIR", "/data/rag")
USE_LLM_ANSWER = os.getenv("RAG_USE_LLM_ANSWER", "0") == "1"
BERT_MODEL = os.getenv("RAG_BERT_MODEL", DEFAULT_BERT_MODEL)
BERT_DEVICE = os.getenv("RAG_BERT_DEVICE") or None
BERT_MAX_LENGTH = int(os.getenv("RAG_BERT_MAX_LENGTH", "512"))
NEIGHBORS = [
    neighbor.strip()
    for neighbor in os.getenv("RAG_NEIGHBORS", "").split(",")
    if neighbor.strip()
]
BOOTSTRAP_QUERY = os.getenv("RAG_BOOTSTRAP_QUERY", "")
BOOTSTRAP_DELAY_SECONDS = float(os.getenv("RAG_BOOTSTRAP_DELAY_SECONDS", "2.0"))
BOOTSTRAP_QUERY_ID = os.getenv("RAG_BOOTSTRAP_QUERY_ID", "")


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
        self.llm_client = build_inference_client_from_env() if USE_LLM_ANSWER else None

    async def Ping(self, request, context):
        return rag_pb2.RagPingReply(worker_id=self.worker_id, status="alive")

    async def RouteQuery(self, request, context):
        query_id = request.query_id or str(uuid.uuid4())

        print(
            f"[rag-worker {self.worker_id}] received query_id={query_id} "
            f"query={request.query}",
            flush=True,
        )

        top_k = request.top_k or 5
        candidates = []
        local_candidates = self.store.route(request.query, top_k=top_k)
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

        return rag_pb2.RouteQueryReply(
            worker_id=self.worker_id,
            candidates=candidates,
        )

    async def broadcast_query(self, request):
        results = await asyncio.gather(
            *(self.send_query_to_neighbor(target, request) for target in self.neighbors),
            return_exceptions=True,
        )
        return sum(1 for result in results if result is True)

    async def send_query_to_neighbor(self, target, request):
        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = rag_pb2_grpc.RagServiceStub(channel)
                await stub.RouteQuery(request, timeout=5.0)
            print(
                f"[rag-worker {self.worker_id}] multicast query_id={request.query_id} to={target}",
                flush=True,
            )
            return True
        except Exception as exc:
            print(
                f"[rag-worker {self.worker_id}] failed multicast query_id={request.query_id} "
                f"to={target}: {exc}",
                flush=True,
            )
            return False



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
