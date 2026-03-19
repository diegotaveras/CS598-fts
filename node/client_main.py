import os
import asyncio
import grpc
import network_pb2
import network_pb2_grpc
from grpc_reflection.v1alpha import reflection
import time
import Client


CLIENT_ID = os.getenv("CLIENT_ID", "client1")
HOST = os.getenv("HOST", "0.0.0.0")
CLIENT_PORT = int(os.getenv("CLIENT_PORT", "9000"))

REPLICAS = os.getenv("REPLICAS", "").split(",") if os.getenv("REPLICAS") else []
PRIMARY = os.getenv("PRIMARY", "node2:8000")

TEST_PROMPT = os.getenv("TEST_PROMPT", "explain RAFT consensus protocol")
LOG_PATH = os.getenv("LOG_PATH")


class NetworkServicer(network_pb2_grpc.NetworkServiceServicer):
    def __init__(self, client):
        self.client = client

    async def Ping(self, request, context):
        print(f"[client {CLIENT_ID}] got ping from {request.sender}", flush=True)
        self.client.log_event("ping_received", sender=request.sender)
        return network_pb2.PingReply(node_id=str(CLIENT_ID), status="alive")

    async def HandleProtocolMessage(self, request, context):
        if request.HasField("speculative_reply"):
            sr = request.speculative_reply
            print(
                f"[client {CLIENT_ID}] received speculative_reply from {request.sender}: "
                f"request_id={sr.request_id} seqno={sr.seqno} replica={sr.replica_id}",
                flush=True,
            )
            await self.client.handle_speculative_reply(sr, sender=request.sender)
            return network_pb2.MessageReply(status="received")

        return network_pb2.MessageReply(status="ignored")


client = Client.Client(CLIENT_ID, HOST, CLIENT_PORT, REPLICAS, LOG_PATH)


async def client_loop():
    await client.handshake_loop()

    print(
        f"[client {CLIENT_ID}] sending request to primary {PRIMARY}: {TEST_PROMPT}",
        flush=True,
    )
    client.log_event("client_request_preparing", primary=PRIMARY, prompt=TEST_PROMPT)

    msg = network_pb2.ProtocolMessage(
        sender=client.self_addr,
        client_request=network_pb2.ClientRequest(
            request_id="0",
            client_id=str(CLIENT_ID),
            prompt=TEST_PROMPT,
            timestamp=int(time.time())
        ),
    )
    
    ok = await client.send_request(PRIMARY, msg)

    if not ok:
        print(f"[client {CLIENT_ID}] failed to send request to primary", flush=True)
        client.log_event("client_request_failed", primary=PRIMARY)
    else:
        print(f"[client {CLIENT_ID}] request sent to primary", flush=True)
        client.log_event("client_request_dispatched", primary=PRIMARY)


async def serve():
    server = grpc.aio.server()
    network_pb2_grpc.add_NetworkServiceServicer_to_server(
        NetworkServicer(client), server
    )

    SERVICE_NAMES = (
        network_pb2.DESCRIPTOR.services_by_name["NetworkService"].full_name,
        reflection.SERVICE_NAME,
    )

    reflection.enable_server_reflection(SERVICE_NAMES, server)

    listen_addr = f"{HOST}:{CLIENT_PORT}"
    server.add_insecure_port(listen_addr)

    print(f"[client {CLIENT_ID}] starting gRPC server on {listen_addr}", flush=True)
    client.log_event("server_starting", listen_addr=listen_addr)

    client.peer_manager.connect_all()

    await server.start()
    print(f"[client {CLIENT_ID}] gRPC server started", flush=True)
    client.log_event("server_started", listen_addr=listen_addr)

    asyncio.create_task(client_loop())

    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()