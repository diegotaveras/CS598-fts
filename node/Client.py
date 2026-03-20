import Peers
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from protocol.proto_state import ProtocolState


class Client:
    def __init__(self, client_id, host, port, replicas, log_path=None):
        self.client_id = str(client_id)
        self.host = host
        self.port = int(port)

        self.hostname = self.client_id
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"

        self.peer_manager = Peers.PeerManager(replicas, self.self_addr)
        default_log_path = Path(__file__).resolve().parent / "logs" / f"{self.client_id}.log"
        self.log_path = Path(log_path) if log_path else default_log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.proto = ProtocolState(
            node_id=client_id,
            peers=replicas,
            role="client",
            f=1,
            current_view=0,
            primary_id="node2",
        )

    def log_event(self, event_type, **fields):
        timestamp = datetime.now(timezone.utc).isoformat()
        entry = {
            "ts": timestamp,
            "client_id": self.client_id,
            "event": event_type,
            **fields,
        }
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")

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
                self.log_event("handshake_failed", replica=replica)

    async def send_request(self, replica, msg):
        try:
            ok = await self.peer_manager.send_message(replica, msg)
            self.log_event("client_request_sent", replica=replica, ok=bool(ok))
            return ok
        except Exception as e:
            print(
                f"[client {self.client_id}] failed to send request to {replica}: {e}",
                flush=True,
            )
            self.log_event("client_request_send_failed", replica=replica, error=str(e))
            return False

    async def handle_speculative_reply(self, reply, sender):
        self.log_event(
            "speculative_reply_received",
            sender=sender,
            request_id=reply.request_id,
            client_id=reply.client_id,
            seqno=reply.seqno,
            view=reply.view,
            history_digest=reply.history_digest,
            result_digest=reply.result_digest,
            replica_id=reply.replica_id,
            ordered_request_seqno=reply.ordered_request.seqno if reply.HasField("ordered_request") else None,
            result=reply.result,
        )

    async def broadcast_request(self, msg):
        results = {}

        send_tasks = [
            self.send_request(replica, msg)
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