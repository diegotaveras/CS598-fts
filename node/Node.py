import Peers
import asyncio
import Agents
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from protocol.proto_state import ProtocolState
import network_pb2

class Node:
    def __init__(self, node_id, host, port, peers, client_addr, log_path=None):
        self.node_id = str(node_id)
        self.host = host
        self.port = int(port)
        self.client_addr = client_addr

        self.hostname = f"node{self.node_id}"
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"
        self.agent_manager = Agents.AgentManager()
        self.peer_manager = Peers.PeerManager(peers, self.self_addr)
        default_log_path = Path(__file__).resolve().parent / "logs" / f"node{self.node_id}.log"
        self.log_path = Path(log_path) if log_path else default_log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.proto = ProtocolState(
        node_id=node_id,
        peers=peers,
        role="replica",
        f=1,
        current_view=0,
        primary_id="2",
    )

    def log_event(self, event_type, **fields):
        timestamp = datetime.now(timezone.utc).isoformat()
        entry = {
            "ts": timestamp,
            "node_id": self.node_id,
            "event": event_type,
            **fields,
        }
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")

    def connect_agent(self, socket, agent_id="local_agent"):
        self.agent_manager.connect_agent(socket, agent_id)

    def _digest_client_request(self, request):
        payload = {
            "request_id": request.request_id,
            "client_id": request.client_id,
            "prompt": request.prompt,
            "timestamp": request.timestamp,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _build_nondeterministic_data(self, request):
        return json.dumps(
            {
                "client_timestamp": request.timestamp,
            },
            sort_keys=True,
        )

    def _digest_text(self, text):
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _is_well_formed_client_request(self, request):
        return bool(request.request_id and request.client_id and request.prompt)

    def _sender_matches_primary(self, sender, leader_id):
        sender_host = sender.split(":", 1)[0]
        normalized_sender = sender_host.replace("node", "")
        return normalized_sender == str(leader_id)

    def _validate_ordered_request(self, request, sender):
        if not self._sender_matches_primary(sender, request.leader_id):
            return False, "sender is not the current primary"

        client_request = request.client_request
        if not self._is_well_formed_client_request(client_request):
            return False, "client request is not well formed"

        computed_digest = self._digest_client_request(client_request)
        if computed_digest != request.request_digest:
            return False, "request digest does not match client request"

        expected_seqno = self.proto.seqnum + 1
        if request.seqno != expected_seqno:
            return False, f"expected seqno {expected_seqno}, got {request.seqno}"

        expected_history_digest = self.proto.expected_history_digest(request.request_digest)
        if request.history_digest != expected_history_digest:
            return False, "history digest does not match local history"

        return True, "accepted"
        
    async def process_prompt(self, prompt, sender):
        try:
            agent_reply = await self.agent_manager.run_task(
                agent_id="local_agent",
                task_id=f"{sender}-to-{self.node_id}",
                payload=prompt,
            )

            agent_result = agent_reply.result if agent_reply is not None else "agent failed"
            print(
                f"[{self.node_id}] local agent processed message from {sender}: {agent_result}",
                flush=True,
            )
            self.log_event(
                "agent_result",
                sender=sender,
                result=agent_result,
            )
            return agent_result
        except Exception as e:
            print(
                f"[{self.node_id}] failed to forward message to local agent: {e}",
                flush=True,
            )
            self.log_event(
                "agent_error",
                sender=sender,
                error=str(e),
            )
            return None

    async def handshake_loop(self):
        await asyncio.sleep(2)

        for peer in self.peer_manager.peers:
            ok = False
            for attempt in range(5):
                ok = await self.peer_manager.ping_peer(peer, timeout=2.0)
                if ok:
                    break

                await asyncio.sleep(1.0)

            if not ok:
                print(f"[{self.node_id}] could not establish contact with {peer}", flush=True)
                self.log_event("handshake_failed", peer=peer)

    async def multicast_prompt(self, request):
        results = {}
        next_seqno = self.proto.seqnum + 1
        request_digest = self._digest_client_request(request)
        history_digest = self.proto.expected_history_digest(request_digest)
        nondeterministic_data = self._build_nondeterministic_data(request)
        self.log_event(
            "ordered_request_created",
            request_id=request.request_id,
            client_id=request.client_id,
            seqno=next_seqno,
            view=self.proto.current_view,
            request_digest=request_digest,
            history_digest=history_digest,
            nondeterministic_data=nondeterministic_data,
        )

        ordered_request = network_pb2.OrderedRequest(
            client_request=network_pb2.ClientRequest(
                request_id=request.request_id,
                client_id=request.client_id,
                prompt=request.prompt,
                timestamp=request.timestamp,
            ),
            view=self.proto.current_view,
            seqno=next_seqno,
            request_digest=request_digest,
            history_digest=history_digest,
            nondeterministic_data=nondeterministic_data,
            leader_id=self.proto.primary_id,
        )

        local_task = asyncio.create_task(
            self.handle_ordered_request(ordered_request, self.self_addr)
        )

        send_tasks = [
            self.peer_manager.send_message(
                peer,
                network_pb2.ProtocolMessage(
                    sender=self.self_addr,
                    ordered_request=ordered_request,
                ),
            )
            for peer in self.peer_manager.peers if peer != self.client_addr
        ]

        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)

        for peer, result in zip(self.peer_manager.peers, send_results):
            if isinstance(result, Exception):
                print(f"[{self.node_id}] failed to multicast to {peer}: {result}", flush=True)
                results[peer] = False
                self.log_event(
                    "ordered_request_send_failed",
                    peer=peer,
                    request_id=request.request_id,
                    seqno=next_seqno,
                    error=str(result),
                )
            else:
                results[peer] = result
                self.log_event(
                    "ordered_request_sent",
                    peer=peer,
                    request_id=request.request_id,
                    seqno=next_seqno,
                    ok=bool(result),
                )

        await local_task
        return results


    async def agent_health_check(self, agent_id="local_agent", retries=10, delay=1.0):
        for attempt in range(retries):
            ok = await self.agent_manager.health_check(agent_id)
            if ok:
                print(f"[{self.node_id}] agent {agent_id} is ready", flush=True)
                self.log_event("agent_ready", agent_id=agent_id)
                return True

            print(
                f"[{self.node_id}] waiting for agent {agent_id} "
                f"(attempt {attempt + 1}/{retries})",
                flush=True,
            )
            await asyncio.sleep(delay)

        print(f"[{self.node_id}] agent {agent_id} failed health check", flush=True)
        self.log_event("agent_health_failed", agent_id=agent_id)
        return False
    
    async def handle_client_request(self, request):
        self.log_event(
            "client_request_received",
            request_id=request.request_id,
            client_id=request.client_id,
            prompt=request.prompt,
            timestamp=request.timestamp,
        )
        asyncio.create_task(self.multicast_prompt(request))

    async def handle_ordered_request(self, request, sender):
        self.log_event(
            "ordered_request_received",
            sender=sender,
            request_id=request.client_request.request_id,
            client_id=request.client_request.client_id,
            seqno=request.seqno,
            view=request.view,
            request_digest=request.request_digest,
            history_digest=request.history_digest,
            nondeterministic_data=request.nondeterministic_data,
        )
        accepted, reason = self._validate_ordered_request(request, sender)
        if not accepted:
            self.log_event(
                "ordered_request_rejected",
                sender=sender,
                request_id=request.client_request.request_id,
                seqno=request.seqno,
                reason=reason,
            )
            return

        self.proto.append_ordered_request(request)
        self.log_event(
            "ordered_request_accepted",
            sender=sender,
            request_id=request.client_request.request_id,
            seqno=request.seqno,
            history_digest=request.history_digest,
        )

        result = await self.process_prompt(request.client_request.prompt, sender)
        if result is None:
            return

        result_digest = self._digest_text(result)
        speculative_reply = network_pb2.SpeculativeReply(
            request_id=request.client_request.request_id,
            client_id=request.client_request.client_id,
            view=request.view,
            seqno=request.seqno,
            history_digest=request.history_digest,
            result_digest=result_digest,
            replica_id=self.node_id,
            result=result,
            ordered_request=request,
        )
        self.log_event(
            "speculative_reply_created",
            request_id=speculative_reply.request_id,
            client_id=speculative_reply.client_id,
            seqno=speculative_reply.seqno,
            view=speculative_reply.view,
            history_digest=speculative_reply.history_digest,
            result_digest=speculative_reply.result_digest,
            replica_id=speculative_reply.replica_id,
        )
        await self.send_speculative_reply(speculative_reply)

    async def send_speculative_reply(self, reply):
        msg = network_pb2.ProtocolMessage(
            sender=self.self_addr,
            speculative_reply=reply,
        )
        ok = await self.peer_manager.send_message(self.client_addr, msg)
        self.log_event(
            "speculative_reply_sent",
            client_addr=self.client_addr,
            request_id=reply.request_id,
            seqno=reply.seqno,
            replica_id=reply.replica_id,
            ok=bool(ok),
        )
        return ok


        


        
