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

        self.hostname = self.node_id
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"
        self.agent_manager = Agents.AgentManager()
        self.peer_manager = Peers.PeerManager(peers, self.self_addr)
        self.fill_hole_timeout_seconds = 1.0
        self.pending_fill_hole = None
        self.local_client_tasks = {}
        self.local_client_results = {}
        default_log_path = Path(__file__).resolve().parent / "logs" / f"{self.node_id}.log"
        self.log_path = Path(log_path) if log_path else default_log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        self.proto = ProtocolState(
        node_id=node_id,
        peers=peers,
        role="replica",
        f=1,
        current_view=0,
        primary_id="node2",
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
        return sender == str(leader_id)

    def _address_for_replica_id(self, replica_id):
        if self.node_id == replica_id:
            return self.self_addr
        for peer in self.peer_manager.peers:
            if peer.split(":", 1)[0] == replica_id:
                return peer
        return None

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

        if self._digest_text(request.leader_result) != request.leader_result_digest:
            return False, "leader result digest does not match leader result"

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

    async def _run_client_request_execution(self, request):
        local_result = await self.process_prompt(request.prompt, request.client_id)
        if local_result is None:
            self.log_event(
                "client_request_local_execution_failed",
                request_id=request.request_id,
                client_id=request.client_id,
            )
            return None

        local_result_digest = self._digest_text(local_result)
        self.local_client_results[request.request_id] = {
            "request": request,
            "result": local_result,
            "result_digest": local_result_digest,
        }
        self.log_event(
            "client_request_locally_processed",
            request_id=request.request_id,
            client_id=request.client_id,
            result_digest=local_result_digest,
        )
        return self.local_client_results[request.request_id]

    def _ensure_client_request_execution(self, request):
        task = self.local_client_tasks.get(request.request_id)
        if task is None or task.done():
            task = asyncio.create_task(self._run_client_request_execution(request))
            self.local_client_tasks[request.request_id] = task
            self.log_event(
                "client_request_local_execution_started",
                request_id=request.request_id,
                client_id=request.client_id,
            )
        return task

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

    async def broadcast_ordered_request(self, request, leader_result):
        results = {}
        next_seqno = self.proto.seqnum + 1
        request_digest = self._digest_client_request(request)
        history_digest = self.proto.expected_history_digest(request_digest)
        nondeterministic_data = self._build_nondeterministic_data(request)
        leader_result_digest = self._digest_text(leader_result)
        self.log_event(
            "ordered_request_created",
            request_id=request.request_id,
            client_id=request.client_id,
            seqno=next_seqno,
            view=self.proto.current_view,
            request_digest=request_digest,
            history_digest=history_digest,
            nondeterministic_data=nondeterministic_data,
            leader_result_digest=leader_result_digest,
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
            leader_result=leader_result,
            leader_result_digest=leader_result_digest,
        )

        local_task = asyncio.create_task(
            self.handle_ordered_request(ordered_request, self.node_id)
        )

        send_tasks = [
            self.peer_manager.send_message(
                peer,
                network_pb2.ProtocolMessage(
                    sender=self.node_id,
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
        local_result_record = await self._ensure_client_request_execution(request)
        if local_result_record is None:
            return

        if self.node_id != self.proto.primary_id:
            self.log_event(
                "client_request_waiting_for_ordered_request",
                request_id=request.request_id,
                client_id=request.client_id,
                primary_id=self.proto.primary_id,
            )
            return

        self.log_event(
            "client_request_accepted_by_primary",
            request_id=request.request_id,
            client_id=request.client_id,
            leader_result_digest=local_result_record["result_digest"],
        )
        asyncio.create_task(self.broadcast_ordered_request(request, local_result_record["result"]))

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
            leader_result_digest=request.leader_result_digest,
        )
        accepted, reason = self._validate_ordered_request(request, sender)
        if not accepted:
            expected_seqno = self.proto.seqnum + 1
            if request.seqno > expected_seqno:
                await self.request_fill_hole(expected_seqno, request.seqno)
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

        cached_result = self.local_client_results.get(request.client_request.request_id)
        if cached_result is not None:
            result = cached_result["result"]
            result_digest = cached_result["result_digest"]
            self.log_event(
                "ordered_request_using_cached_local_result",
                request_id=request.client_request.request_id,
                seqno=request.seqno,
                result_digest=result_digest,
            )
        else:
            task = self.local_client_tasks.get(request.client_request.request_id)
            if task is not None:
                self.log_event(
                    "ordered_request_waiting_for_local_result",
                    request_id=request.client_request.request_id,
                    seqno=request.seqno,
                )
                cached_result = await task
                if cached_result is not None:
                    result = cached_result["result"]
                    result_digest = cached_result["result_digest"]
                    self.log_event(
                        "ordered_request_using_awaited_local_result",
                        request_id=request.client_request.request_id,
                        seqno=request.seqno,
                        result_digest=result_digest,
                    )
                else:
                    return
            else:
                result = await self.process_prompt(request.client_request.prompt, sender)
                if result is None:
                    return
                result_digest = self._digest_text(result)
                self.log_event(
                    "ordered_request_computed_local_result",
                    request_id=request.client_request.request_id,
                    seqno=request.seqno,
                    result_digest=result_digest,
                )

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

    async def send_fill_hole_request(self, request, target):
        msg = network_pb2.ProtocolMessage(
            sender=self.node_id,
            fill_hole_request=request,
        )
        ok = await self.peer_manager.send_message(target, msg)
        self.log_event(
            "fill_hole_request_sent",
            target=target,
            replica_id=request.replica_id,
            view=request.view,
            start_seqno=request.start_seqno,
            end_seqno=request.end_seqno,
            ok=bool(ok),
        )
        return ok

    async def send_fill_hole_response(self, response, target):
        msg = network_pb2.ProtocolMessage(
            sender=self.node_id,
            fill_hole_response=response,
        )
        ok = await self.peer_manager.send_message(target, msg)
        self.log_event(
            "fill_hole_response_sent",
            target=target,
            responder_id=response.responder_id,
            view=response.view,
            ordered_request_count=len(response.ordered_requests),
            ok=bool(ok),
        )
        return ok

    async def broadcast_fill_hole_request(self, request):
        targets = [peer for peer in self.peer_manager.peers if peer != self.self_addr]
        send_results = await asyncio.gather(
            *(self.send_fill_hole_request(request, target) for target in targets),
            return_exceptions=True,
        )
        self.log_event(
            "fill_hole_request_broadcast_complete",
            replica_id=request.replica_id,
            view=request.view,
            start_seqno=request.start_seqno,
            end_seqno=request.end_seqno,
            sent_count=len(send_results),
        )

    async def _fill_hole_timer_worker(self, request):
        try:
            await asyncio.sleep(self.fill_hole_timeout_seconds)
            pending = self.pending_fill_hole
            if pending is None:
                return
            pending_request = pending["request"]
            if (
                pending_request.view != request.view
                or pending_request.start_seqno != request.start_seqno
                or pending_request.end_seqno != request.end_seqno
            ):
                return

            self.log_event(
                "fill_hole_timer_fired",
                replica_id=pending_request.replica_id,
                view=pending_request.view,
                start_seqno=pending_request.start_seqno,
                end_seqno=pending_request.end_seqno,
            )
            await self.broadcast_fill_hole_request(pending_request)
        except asyncio.CancelledError:
            self.log_event(
                "fill_hole_timer_cancelled",
                view=request.view,
                start_seqno=request.start_seqno,
                end_seqno=request.end_seqno,
            )
            raise
        finally:
            pending = self.pending_fill_hole
            if pending is not None:
                pending_request = pending["request"]
                if (
                    pending_request.view == request.view
                    and pending_request.start_seqno == request.start_seqno
                    and pending_request.end_seqno == request.end_seqno
                ):
                    self.pending_fill_hole = None

    def _ensure_fill_hole_timer(self, request):
        existing = self.pending_fill_hole
        if existing is not None:
            existing_request = existing["request"]
            if existing_request.view == request.view:
                if (
                    existing_request.start_seqno <= request.start_seqno
                    and existing_request.end_seqno >= request.end_seqno
                ):
                    self.log_event(
                        "fill_hole_request_already_pending",
                        replica_id=request.replica_id,
                        view=request.view,
                        start_seqno=request.start_seqno,
                        end_seqno=request.end_seqno,
                        pending_start_seqno=existing_request.start_seqno,
                        pending_end_seqno=existing_request.end_seqno,
                    )
                    return False
                self.cancel_fill_hole_timer(
                    existing_request.view,
                    existing_request.start_seqno,
                    existing_request.end_seqno,
                )

        task = asyncio.create_task(self._fill_hole_timer_worker(request))
        self.pending_fill_hole = {
            "request": request,
            "task": task,
        }
        self.log_event(
            "fill_hole_timer_started",
            replica_id=request.replica_id,
            view=request.view,
            start_seqno=request.start_seqno,
            end_seqno=request.end_seqno,
            timeout_seconds=self.fill_hole_timeout_seconds,
        )
        return True

    def cancel_fill_hole_timer(self, view, start_seqno, end_seqno):
        pending = self.pending_fill_hole
        if pending is None:
            return False
        request = pending["request"]
        if (
            request.view != view
            or request.start_seqno != start_seqno
            or request.end_seqno != end_seqno
        ):
            return False
        self.pending_fill_hole = None
        task = pending["task"] if pending is not None else None
        if task is not None and not task.done():
            task.cancel()
        return True

    async def request_fill_hole(self, start_seqno, end_seqno):
        request = network_pb2.FillHoleRequest(
            view=self.proto.current_view,
            start_seqno=start_seqno,
            end_seqno=end_seqno,
            replica_id=self.node_id,
        )
        started = self._ensure_fill_hole_timer(request)
        if not started:
            return True

        primary_addr = self._address_for_replica_id(self.proto.primary_id)
        if primary_addr is None:
            self.log_event(
                "fill_hole_request_failed",
                replica_id=request.replica_id,
                view=request.view,
                start_seqno=start_seqno,
                end_seqno=end_seqno,
                reason="primary_address_not_found",
            )
            return False

        return await self.send_fill_hole_request(request, primary_addr)

    async def send_speculative_reply(self, reply):
        msg = network_pb2.ProtocolMessage(
            sender=self.node_id,
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

    async def handle_fill_hole_request(self, request, sender):
        self.log_event(
            "fill_hole_request_received",
            sender=sender,
            replica_id=request.replica_id,
            view=request.view,
            start_seqno=request.start_seqno,
            end_seqno=request.end_seqno,
        )

        if request.view != self.proto.current_view:
            self.log_event(
                "fill_hole_request_ignored",
                sender=sender,
                replica_id=request.replica_id,
                view=request.view,
                reason="view_mismatch",
                current_view=self.proto.current_view,
            )
            return

        available_requests = self.proto.get_ordered_requests_in_range(
            request.start_seqno,
            request.end_seqno,
        )
        requester_addr = self._address_for_replica_id(request.replica_id)

        handler_role = "primary" if self.node_id == self.proto.primary_id else "replica"
        self.log_event(
            "fill_hole_request_accepted",
            sender=sender,
            handler_role=handler_role,
            replica_id=request.replica_id,
            view=request.view,
            start_seqno=request.start_seqno,
            end_seqno=request.end_seqno,
        )

        if requester_addr is None:
            self.log_event(
                "fill_hole_response_not_sent",
                sender=sender,
                replica_id=request.replica_id,
                view=request.view,
                reason="requester_address_not_found",
            )
            return

        if not available_requests:
            self.log_event(
                "fill_hole_response_not_sent",
                sender=sender,
                replica_id=request.replica_id,
                view=request.view,
                reason="no_matching_ordered_requests",
            )
            return

        response = network_pb2.FillHoleResponse(
            view=request.view,
            responder_id=self.node_id,
            ordered_requests=available_requests,
        )
        await self.send_fill_hole_response(response, requester_addr)

    async def handle_fill_hole_response(self, response, sender):
        start_seqno = response.ordered_requests[0].seqno if response.ordered_requests else None
        end_seqno = response.ordered_requests[-1].seqno if response.ordered_requests else None
        self.log_event(
            "fill_hole_response_received",
            sender=sender,
            responder_id=response.responder_id,
            view=response.view,
            start_seqno=start_seqno,
            end_seqno=end_seqno,
            ordered_request_count=len(response.ordered_requests),
        )

        if response.view != self.proto.current_view or not response.ordered_requests:
            self.log_event(
                "fill_hole_response_ignored",
                sender=sender,
                responder_id=response.responder_id,
                view=response.view,
                reason="view_mismatch_or_empty",
            )
            return

        sorted_requests = sorted(response.ordered_requests, key=lambda req: req.seqno)
        for ordered_request in sorted_requests:
            known = self.proto.get_ordered_request(ordered_request.seqno)
            if known is not None:
                if known.request_digest != ordered_request.request_digest:
                    self.log_event(
                        "fill_hole_conflict_detected",
                        sender=sender,
                        responder_id=response.responder_id,
                        seqno=ordered_request.seqno,
                        known_digest=known.request_digest,
                        received_digest=ordered_request.request_digest,
                    )
                continue

            if ordered_request.seqno != self.proto.seqnum + 1:
                continue

            await self.handle_ordered_request(ordered_request, ordered_request.leader_id)

        cancelled = False
        pending = self.pending_fill_hole
        if pending is not None:
            request = pending["request"]
            if request.view == response.view and self.proto.seqnum >= request.end_seqno:
                cancelled = self.cancel_fill_hole_timer(
                    request.view,
                    request.start_seqno,
                    request.end_seqno,
                )

        self.log_event(
            "fill_hole_response_processed",
            sender=sender,
            responder_id=response.responder_id,
            view=response.view,
            start_seqno=start_seqno,
            end_seqno=end_seqno,
            timer_cancelled=bool(cancelled),
            current_seqno=self.proto.seqnum,
        )


        


        
