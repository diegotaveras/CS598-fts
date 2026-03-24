import asyncio
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import Node
import network_pb2


def make_node(node_id: str, log_name: str) -> Node.Node:
    return Node.Node(
        node_id=node_id,
        host="127.0.0.1",
        port=8000,
        peers=["node2:8000", "node3:8000", "node4:8000", "node5:8000"],
        client_addr="node1:9000",
        log_path=LOG_DIR / log_name,
    )


def read_events(log_path: Path):
    if not log_path.exists():
        return []
    return [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def make_client_request(request_id: str, prompt: str = "prompt", timestamp: int = 1):
    return network_pb2.ClientRequest(
        request_id=request_id,
        client_id="client1",
        prompt=prompt,
        timestamp=timestamp,
    )


class FakePeerManager:
    def __init__(self, peers):
        self.peers = peers
        self.sent_messages = []

    async def send_message(self, peer, msg, timeout=3.0):
        self.sent_messages.append((peer, msg))
        return True


async def fake_process_prompt(self, prompt, sender):
    return f"processed:{prompt}:{sender}"


async def fake_send_speculative_reply(self, reply):
    return True


class FillHoleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        LOG_DIR.mkdir(parents=True, exist_ok=True)

    async def test_non_primary_accepts_for_replica_handling(self):
        node = make_node("node4", "fill_hole_non_primary.log")
        if node.log_path.exists():
            node.log_path.unlink()

        request = network_pb2.FillHoleRequest(
            view=0,
            start_seqno=2,
            end_seqno=4,
            replica_id="node4",
        )

        await node.handle_fill_hole_request(request, sender="node4")

        events = read_events(node.log_path)
        self.assertEqual(events[0]["event"], "fill_hole_request_received")
        self.assertEqual(
            events[1]["event"],
            "fill_hole_request_accepted",
        )
        self.assertEqual(events[1]["handler_role"], "replica")

    async def test_primary_ignores_wrong_view(self):
        node = make_node("node2", "fill_hole_view_mismatch.log")
        if node.log_path.exists():
            node.log_path.unlink()

        request = network_pb2.FillHoleRequest(
            view=1,
            start_seqno=2,
            end_seqno=4,
            replica_id="node4",
        )

        await node.handle_fill_hole_request(request, sender="node4")

        events = read_events(node.log_path)
        self.assertEqual(events[0]["event"], "fill_hole_request_received")
        self.assertEqual(events[1]["event"], "fill_hole_request_ignored")
        self.assertEqual(events[1]["reason"], "view_mismatch")
        self.assertEqual(events[1]["current_view"], 0)

    async def test_primary_accepts_matching_view(self):
        node = make_node("node2", "fill_hole_primary_accept.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager

        client_request = make_client_request("req-2")
        node.proto.append_ordered_request(
            network_pb2.OrderedRequest(
                client_request=client_request,
                view=0,
                seqno=2,
                request_digest=node._digest_client_request(client_request),
                history_digest="history-2",
                nondeterministic_data="{}",
                leader_id="node2",
            )
        )

        request = network_pb2.FillHoleRequest(
            view=0,
            start_seqno=2,
            end_seqno=4,
            replica_id="node4",
        )

        await node.handle_fill_hole_request(request, sender="node4")

        events = read_events(node.log_path)
        self.assertEqual(events[0]["event"], "fill_hole_request_received")
        self.assertEqual(
            events[1]["event"],
            "fill_hole_request_accepted",
        )
        self.assertEqual(events[1]["handler_role"], "primary")
        self.assertTrue(any(event["event"] == "fill_hole_response_sent" for event in events))

    async def test_request_fill_hole_starts_timer_and_sends_to_primary(self):
        node = make_node("node4", "fill_hole_timer_start.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager
        node.fill_hole_timeout_seconds = 0.5

        ok = await node.request_fill_hole(2, 4)

        self.assertTrue(ok)
        self.assertEqual(len(fake_peer_manager.sent_messages), 1)
        peer, msg = fake_peer_manager.sent_messages[0]
        self.assertEqual(peer, "node2:8000")
        self.assertTrue(msg.HasField("fill_hole_request"))
        self.assertIsNotNone(node.pending_fill_hole)
        self.assertEqual(node.pending_fill_hole["request"].start_seqno, 2)
        self.assertEqual(node.pending_fill_hole["request"].end_seqno, 4)

        node.cancel_fill_hole_timer(0, 2, 4)

    async def test_fill_hole_timer_broadcasts_on_timeout(self):
        node = make_node("node4", "fill_hole_timer_broadcast.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager
        node.fill_hole_timeout_seconds = 0.01

        await node.request_fill_hole(2, 4)
        await asyncio.sleep(0.05)

        fill_hole_sends = [msg for _, msg in fake_peer_manager.sent_messages if msg.HasField("fill_hole_request")]
        self.assertGreaterEqual(len(fill_hole_sends), 1 + len(fake_peer_manager.peers))

        events = read_events(node.log_path)
        self.assertTrue(any(event["event"] == "fill_hole_timer_fired" for event in events))

    async def test_fill_hole_response_cancels_timer(self):
        node = make_node("node4", "fill_hole_response_cancel.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager
        node.fill_hole_timeout_seconds = 1.0
        node.process_prompt = fake_process_prompt.__get__(node, Node.Node)
        node.send_speculative_reply = fake_send_speculative_reply.__get__(node, Node.Node)

        await node.request_fill_hole(2, 4)
        self.assertIsNotNone(node.pending_fill_hole)

        response = network_pb2.FillHoleResponse(
            view=0,
            responder_id="node2",
            ordered_requests=[
                network_pb2.OrderedRequest(
                    client_request=network_pb2.ClientRequest(
                        request_id="req-2",
                        client_id="client1",
                        prompt="prompt",
                        timestamp=1,
                    ),
                    view=0,
                    seqno=2,
                    request_digest="digest-2",
                    history_digest=node.proto.expected_history_digest("digest-2"),
                    nondeterministic_data="{}",
                    leader_id="node2",
                ),
            ],
        )

        await node.handle_fill_hole_response(response, sender="node2")

        self.assertIsNotNone(node.pending_fill_hole)
        self.assertEqual(node.pending_fill_hole["request"].start_seqno, 2)
        self.assertEqual(node.pending_fill_hole["request"].end_seqno, 4)
        events = read_events(node.log_path)
        self.assertTrue(any(event["event"] == "fill_hole_response_processed" for event in events))

    async def test_primary_sends_fill_hole_response_with_matching_requests(self):
        node = make_node("node2", "fill_hole_primary_response.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager

        client_request = make_client_request("req-2")
        ordered_request = network_pb2.OrderedRequest(
            client_request=client_request,
            view=0,
            seqno=2,
            request_digest=node._digest_client_request(client_request),
            history_digest="history-2",
            nondeterministic_data="{}",
            leader_id="node2",
        )
        node.proto.append_ordered_request(ordered_request)

        request = network_pb2.FillHoleRequest(
            view=0,
            start_seqno=2,
            end_seqno=4,
            replica_id="node4",
        )

        await node.handle_fill_hole_request(request, sender="node4")

        self.assertEqual(len(fake_peer_manager.sent_messages), 1)
        peer, msg = fake_peer_manager.sent_messages[0]
        self.assertEqual(peer, "node4:8000")
        self.assertTrue(msg.HasField("fill_hole_response"))
        self.assertEqual(len(msg.fill_hole_response.ordered_requests), 1)

    async def test_fill_hole_response_replays_missing_ordered_request(self):
        node = make_node("node4", "fill_hole_response_replay.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager
        node.fill_hole_timeout_seconds = 1.0
        node.process_prompt = fake_process_prompt.__get__(node, Node.Node)
        node.send_speculative_reply = fake_send_speculative_reply.__get__(node, Node.Node)

        await node.request_fill_hole(1, 1)

        client_request = make_client_request("req-1")
        request_digest = node._digest_client_request(client_request)
        response = network_pb2.FillHoleResponse(
            view=0,
            responder_id="node2",
            ordered_requests=[
                network_pb2.OrderedRequest(
                    client_request=client_request,
                    view=0,
                    seqno=1,
                    request_digest=request_digest,
                    history_digest=node.proto.expected_history_digest(request_digest),
                    nondeterministic_data="{}",
                    leader_id="node2",
                ),
            ],
        )

        await node.handle_fill_hole_response(response, sender="node2")

        self.assertEqual(node.proto.seqnum, 1)
        self.assertIsNone(node.pending_fill_hole)

    async def test_overlapping_fill_hole_request_is_not_duplicated(self):
        node = make_node("node4", "fill_hole_overlap.log")
        if node.log_path.exists():
            node.log_path.unlink()

        fake_peer_manager = FakePeerManager(node.peer_manager.peers)
        node.peer_manager = fake_peer_manager
        node.fill_hole_timeout_seconds = 1.0

        await node.request_fill_hole(2, 4)
        ok = await node.request_fill_hole(3, 4)

        self.assertTrue(ok)
        self.assertEqual(len(fake_peer_manager.sent_messages), 1)
        self.assertIsNotNone(node.pending_fill_hole)
        self.assertEqual(node.pending_fill_hole["request"].start_seqno, 2)
        self.assertEqual(node.pending_fill_hole["request"].end_seqno, 4)

        events = read_events(node.log_path)
        self.assertTrue(any(event["event"] == "fill_hole_request_already_pending" for event in events))

        node.cancel_fill_hole_timer(0, 2, 4)


if __name__ == "__main__":
    unittest.main()
