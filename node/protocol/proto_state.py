import hashlib


class ProtocolState:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        role: str = "replica",
        f: int = 1,
        current_view: int = 0,
        primary_id = "node1",
    ):

        self.node_id = str(node_id)
        self.peers = list(peers)
        self.role = role  # "replica" or "client"
        self.f = f
        self.n = len(peers)
        self.current_view = current_view
        self.seqnum = 0
        self.history_digest = "GENESIS"
        self.ordered_history = []
        self.primary_id = primary_id if primary_id is not None else "node1"

    def allocate_seqno(self) -> int:
        self.seqnum += 1
        return self.seqnum

    def advance_history(self, request_digest: str) -> str:
        material = f"{self.history_digest}:{request_digest}".encode("utf-8")
        self.history_digest = hashlib.sha256(material).hexdigest()
        return self.history_digest

    def expected_history_digest(self, request_digest: str) -> str:
        material = f"{self.history_digest}:{request_digest}".encode("utf-8")
        return hashlib.sha256(material).hexdigest()

    def append_ordered_request(self, ordered_request):
        self.seqnum = ordered_request.seqno
        self.history_digest = ordered_request.history_digest
        self.ordered_history.append(ordered_request)
    
