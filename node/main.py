import os
import asyncio
import grpc
import network_pb2
import network_pb2_grpc


NODE_ID = os.getenv("NODE_ID", "node")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []
SELF_ADDR = os.getenv("SELF_ADDR", f"node{NODE_ID}:{PORT}")


class NetworkServicer(network_pb2_grpc.NetworkServiceServicer):
    async def Ping(self, request, context):
        print(f"[{NODE_ID}] got ping from {request.sender}", flush=True)
        return network_pb2.PingReply(node_id=str(NODE_ID), status="alive")

    async def SendMessage(self, request, context):
        print(f"[{NODE_ID}] received from {request.sender}: {request.msg}", flush=True)
        return network_pb2.MessageReply(status="received")


class PeerManager:
    def __init__(self, peers):
        self.peers = [p for p in peers if p and p != SELF_ADDR]
        self.channels = {}
        self.stubs = {}

    def connect_all(self):
        for peer in self.peers:
            channel = grpc.aio.insecure_channel(peer)
            stub = network_pb2_grpc.NetworkServiceStub(channel)
            self.channels[peer] = channel
            self.stubs[peer] = stub

    async def close_all(self):
        for channel in self.channels.values():
            await channel.close()

    async def ping_peer(self, peer, timeout=2.0):
        try:
            reply = await self.stubs[peer].Ping(
                network_pb2.PingRequest(sender=str(NODE_ID)),
                timeout=timeout,
            )
            print(f"[{NODE_ID}] ping ok -> {peer}: {reply.status}", flush=True)
            return True
        except Exception as e:
            print(f"[{NODE_ID}] ping failed -> {peer}: {e}", flush=True)
            return False

    async def send_message(self, peer, msg, timeout=3.0):
        try:
            reply = await self.stubs[peer].SendMessage(
                network_pb2.MessageRequest(sender=str(NODE_ID), msg=msg),
                timeout=timeout,
            )
            print(f"[{NODE_ID}] sent to {peer}: {reply.status}", flush=True)
            return True
        except Exception as e:
            print(f"[{NODE_ID}] send failed -> {peer}: {e}", flush=True)
            return False


peer_manager = PeerManager(PEERS)


async def handshake_loop():
    await asyncio.sleep(2)

    for peer in peer_manager.peers:
        ok = False
        for attempt in range(5):
            ok = await peer_manager.ping_peer(peer, timeout=2.0)
            if ok:
                await peer_manager.send_message(peer, "hello from networking setup")
                break

            await asyncio.sleep(1.0)

        if not ok:
            print(f"[{NODE_ID}] could not establish contact with {peer}", flush=True)


async def serve():
    server = grpc.aio.server()
    network_pb2_grpc.add_NetworkServiceServicer_to_server(NetworkServicer(), server)

    listen_addr = f"{HOST}:{PORT}"
    server.add_insecure_port(listen_addr)

    print(f"[{NODE_ID}] starting gRPC server on {listen_addr}", flush=True)

    peer_manager.connect_all()

    await server.start()
    print(f"[{NODE_ID}] gRPC server started", flush=True)

    # background runtime tasks, here we can start the agent process too
    asyncio.create_task(handshake_loop())


    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()