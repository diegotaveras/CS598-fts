import Peers
import asyncio

class Node:
    def __init__(self, node_id, host, port, peers):
        self.node_id = str(node_id)
        self.host = host
        self.port = int(port)

        self.hostname = f"node{self.node_id}"
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"
        self.peers = peers

        self.peer_manager = Peers.PeerManager(peers, self.self_addr)

    async def handshake_loop(self):
        await asyncio.sleep(2)

        for peer in self.peer_manager.peers:
            ok = False
            for attempt in range(5):
                ok = await self.peer_manager.ping_peer(peer, timeout=2.0)
                if ok:
                    await self.peer_manager.send_message(peer, "hello from networking setup")
                    break

                await asyncio.sleep(1.0)

            if not ok:
                print(f"[{self.NODE_ID}] could not establish contact with {peer}", flush=True)