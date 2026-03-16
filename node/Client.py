import Peers
import asyncio
from protocol.proto_state import ProtocolState


class Client:
    def __init__(self, client_id, host, port, replicas):
        self.client_id = str(client_id)
        self.host = host
        self.port = int(port)

        self.hostname = f"client{self.client_id}"
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"

        self.peer_manager = Peers.PeerManager(replicas, self.self_addr)

        self.proto = ProtocolState(
            node_id=client_id,
            peers=replicas,
            role="client",
            f=1,
            current_view=0,
            primary_id="2",
        )

    async def handshake_loop(self):
        await asyncio.sleep(2)

        for replica in self.peer_manager.peers:
            ok = False
            for attempt in range(5):
                ok = await self.peer_manager.ping_peer(replica, timeout=2.0)
                if ok:
                    break

                await asyncio.sleep(1.0)

            if not ok:
                print(
                    f"[client {self.client_id}] could not establish contact with {replica}",
                    flush=True,
                )

    async def send_request(self, replica, msg):
        try:
            ok = await self.peer_manager.send_message(replica, msg)
            return ok
        except Exception as e:
            print(
                f"[client {self.client_id}] failed to send request to {replica}: {e}",
                flush=True,
            )
            return False

    async def broadcast_request(self, msg):
        results = {}

        send_tasks = [
            self.peer_manager.send_message(replica, msg)
            for replica in self.peer_manager.peers
        ]

        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)

        for replica, result in zip(self.peer_manager.peers, send_results):
            if isinstance(result, Exception):
                print(
                    f"[client {self.client_id}] failed to send to {replica}: {result}",
                    flush=True,
                )
                results[replica] = False
            else:
                results[replica] = result

        return results

    async def close(self):
        await self.peer_manager.close_all()