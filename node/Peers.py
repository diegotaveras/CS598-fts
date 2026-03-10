import grpc
import network_pb2
import network_pb2_grpc


class PeerManager:
    def __init__(self, peers, SELF_ADDR):
        self.peers = [p for p in peers if p and p != SELF_ADDR]
        self.NODE_ID = SELF_ADDR.split(":")[0] if SELF_ADDR.split(":") else "-1"
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
                network_pb2.PingRequest(sender=str(self.NODE_ID)),
                timeout=timeout,
            )
            print(f"[{self.NODE_ID}] ping ok -> {peer}: {reply.status}", flush=True)
            return True
        except Exception as e:
            print(f"[{self.NODE_ID}] ping failed -> {peer}: {e}", flush=True)
            return False

    async def send_message(self, peer, msg, timeout=3.0):
        try:
            reply = await self.stubs[peer].SendMessage(
                network_pb2.MessageRequest(sender=str(self.NODE_ID), msg=msg),
                timeout=timeout,
            )
            print(f"[{self.NODE_ID}] sent to {peer}: {reply.status}", flush=True)
            return True
        except Exception as e:
            print(f"[{self.NODE_ID}] send failed -> {peer}: {e}", flush=True)
            return False
