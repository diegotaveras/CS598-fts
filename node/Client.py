import Peers
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
from protocol.proto_state import ProtocolState
import network_pb2


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
        self.speculative_vote_tracker = {}
        self.slot_vote_tracker = {}
        self.delivered_fast_path = set()
        self.started_slow_path = set()

        self.proto = ProtocolState(
            node_id=client_id,
            peers=replicas,
            role="client",
            f=1,
            current_view=0,
            primary_id="node2",
        )

    def fast_quorum_size(self):
        return 3 * self.proto.f + 1

    def slow_quorum_size(self):
        return 2 * self.proto.f + 1

    def _vote_key(self, reply):
        return (
            reply.request_id,
            reply.view,
            reply.seqno,
            reply.leader_result_digest,
        )

    def _slot_key(self, reply):
        return (
            reply.request_id,
            reply.view,
            reply.seqno,
        )

    def _should_start_slow_path(self, slot_key, total_vote_count, agree_vote_count):
        if total_vote_count < self.slow_quorum_size():
            return False

        slot_voters = self.slot_vote_tracker.get(slot_key, {})
        has_non_agree_vote = any(
            vote["vote_decision"] != network_pb2.VOTE_DECISION_AGREE
            for vote in slot_voters.values()
        )
        if has_non_agree_vote:
            return True

        return total_vote_count >= len(self.peer_manager.peers) and agree_vote_count < self.fast_quorum_size()

    async def deliver_reply(self, request_id, view, seqno, result, leader_result_digest):
        print(
            f"[client {self.client_id}] fast-path deliver request_id={request_id} "
            f"view={view} seqno={seqno}: {result}",
            flush=True,
        )
        self.log_event(
            "reply_delivered",
            request_id=request_id,
            view=view,
            seqno=seqno,
            leader_result_digest=leader_result_digest,
            result=result,
            delivery_path="fast",
        )

    async def start_slow_path(self, reply):
        print(
            f"[client {self.client_id}] slow-path trigger request_id={reply.request_id} "
            f"view={reply.view} seqno={reply.seqno}",
            flush=True,
        )
        self.log_event(
            "slow_path_triggered",
            request_id=reply.request_id,
            view=reply.view,
            seqno=reply.seqno,
            leader_result_digest=reply.leader_result_digest,
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
            vote_decision=reply.vote_decision,
            leader_result_digest=reply.leader_result_digest,
            judge_reason=reply.judge_reason,
            ordered_request_seqno=reply.ordered_request.seqno if reply.HasField("ordered_request") else None,
            result=reply.result,
        )

        slot_key = self._slot_key(reply)
        slot_voters = self.slot_vote_tracker.setdefault(slot_key, {})
        slot_voters[reply.replica_id] = {
            "sender": sender,
            "vote_decision": reply.vote_decision,
            "leader_result_digest": reply.leader_result_digest,
            "result_digest": reply.result_digest,
        }
        total_vote_count = len(slot_voters)
        self.log_event(
            "speculative_reply_counted",
            request_id=reply.request_id,
            view=reply.view,
            seqno=reply.seqno,
            total_vote_count=total_vote_count,
            slow_quorum_size=self.slow_quorum_size(),
        )

        if reply.vote_decision != network_pb2.VOTE_DECISION_AGREE:
            if (
                self._should_start_slow_path(slot_key, total_vote_count, 0)
                and slot_key not in self.started_slow_path
            ):
                self.started_slow_path.add(slot_key)
                await self.start_slow_path(reply)
            return

        vote_key = self._vote_key(reply)
        voters = self.speculative_vote_tracker.setdefault(vote_key, {})
        voters[reply.replica_id] = {
            "sender": sender,
            "result_digest": reply.result_digest,
            "leader_result": reply.ordered_request.leader_result if reply.HasField("ordered_request") else reply.result,
        }
        vote_count = len(voters)
        self.log_event(
            "speculative_reply_agree_counted",
            request_id=reply.request_id,
            view=reply.view,
            seqno=reply.seqno,
            leader_result_digest=reply.leader_result_digest,
            vote_count=vote_count,
            quorum_size=self.fast_quorum_size(),
        )

        if vote_count < self.fast_quorum_size():
            if (
                self._should_start_slow_path(slot_key, total_vote_count, vote_count)
                and slot_key not in self.started_slow_path
            ):
                self.started_slow_path.add(slot_key)
                await self.start_slow_path(reply)
            return

        if vote_key in self.delivered_fast_path:
            return

        self.delivered_fast_path.add(vote_key)
        leader_result = (
            reply.ordered_request.leader_result
            if reply.HasField("ordered_request")
            else reply.result
        )
        await self.deliver_reply(
            request_id=reply.request_id,
            view=reply.view,
            seqno=reply.seqno,
            result=leader_result,
            leader_result_digest=reply.leader_result_digest,
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