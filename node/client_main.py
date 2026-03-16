import os
import asyncio
import grpc
import network_pb2
import network_pb2_grpc
from grpc_reflection.v1alpha import reflection

import Client


CLIENT_ID = os.getenv("CLIENT_ID", "1")
HOST = os.getenv("HOST", "0.0.0.0")
CLIENT_PORT = int(os.getenv("CLIENT_PORT", "9000"))

REPLICAS = os.getenv("REPLICAS", "").split(",") if os.getenv("REPLICAS") else []
PRIMARY = os.getenv("PRIMARY", "node2:8000")

TEST_PROMPT = os.getenv("TEST_PROMPT", "explain RAFT consensus protocol")


class NetworkServicer(network_pb2_grpc.NetworkServiceServicer):
    def __init__(self, client):
        self.client = client

    async def Ping(self, request, context):
        print(f"[client {CLIENT_ID}] got ping from {request.sender}", flush=True)
        return network_pb2.PingReply(node_id=f"client{CLIENT_ID}", status="alive")

    async def SendMessage(self, request, context):
        print(
            f"[client {CLIENT_ID}] received reply from {request.sender}: {request.msg}",
            flush=True,
        )
        return network_pb2.MessageReply(status="received")


client = Client.Client(CLIENT_ID, HOST, CLIENT_PORT, REPLICAS)


async def client_loop():
    await client.handshake_loop()

    print(
        f"[client {CLIENT_ID}] sending request to primary {PRIMARY}: {TEST_PROMPT}",
        flush=True,
    )

    msg = network_pb2.ProtocolMessage(
        sender=client.self_addr,
        client_request=network_pb2.ClientRequest(
            request_id="0",
            client_id=str(CLIENT_ID),
            prompt=TEST_PROMPT,
        ),
    )
    
    ok = await client.send_request(PRIMARY, msg)

    if not ok:
        print(f"[client {CLIENT_ID}] failed to send request to primary", flush=True)
    else:
        print(f"[client {CLIENT_ID}] request sent to primary", flush=True)


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

    client.peer_manager.connect_all()

    await server.start()
    print(f"[client {CLIENT_ID}] gRPC server started", flush=True)

    asyncio.create_task(client_loop())

    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()