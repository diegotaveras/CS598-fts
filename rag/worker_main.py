import asyncio
import os
import uuid

import grpc
from grpc_reflection.v1alpha import reflection

from rag import rag_pb2, rag_pb2_grpc
from rag.document_store import LocalDocumentStore
from rag.llm_client import build_inference_client_from_env


WORKER_ID = os.getenv("RAG_WORKER_ID", os.getenv("HOSTNAME", "rag-worker"))
HOST = os.getenv("RAG_HOST", "0.0.0.0")
PORT = int(os.getenv("RAG_PORT", "9100"))
DOC_DIR = os.getenv("RAG_DOC_DIR", "/data/rag")
USE_LLM_ANSWER = os.getenv("RAG_USE_LLM_ANSWER", "0") == "1"
NEIGHBORS = [
    neighbor.strip()
    for neighbor in os.getenv("RAG_NEIGHBORS", "").split(",")
    if neighbor.strip()
]
BOOTSTRAP_QUERY = os.getenv("RAG_BOOTSTRAP_QUERY", "")
BOOTSTRAP_DELAY_SECONDS = float(os.getenv("RAG_BOOTSTRAP_DELAY_SECONDS", "2.0"))
BOOTSTRAP_QUERY_ID = os.getenv("RAG_BOOTSTRAP_QUERY_ID", "")


class RagWorkerServicer(rag_pb2_grpc.RagServiceServicer):
    def __init__(self, worker_id: str, store: LocalDocumentStore, neighbors: list[str]):
        self.worker_id = worker_id
        self.store = store
        self.neighbors = neighbors
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
        for score, section in local_candidates:
            print(
                f"[rag-worker {self.worker_id}] local candidate query_id={query_id} "
                f"score={score:.4f} section={section.section_id} title={section.title}",
                flush=True,
            )
            candidates.append(
                rag_pb2.SubtreeCandidate(
                    worker_id=self.worker_id,
                    section_id=section.section_id,
                    title=section.title,
                    score=float(score),
                    preview=section.preview(),
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
    store = LocalDocumentStore(WORKER_ID, DOC_DIR)
    store.load()
    print(
        f"[rag-worker {WORKER_ID}] loaded {len(store.sections)} sections from {DOC_DIR}",
        flush=True,
    )

    servicer = RagWorkerServicer(WORKER_ID, store, NEIGHBORS)
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
