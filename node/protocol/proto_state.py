import hashlib


class ProtocolState:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        role: str = "replica",
        f: int = 1,
        current_view: int = 0,
        primary_id = "1",
    ):

        self.node_id = str(node_id)
        self.peers = list(peers)
        self.role = role  # "replica" or "client"
        self.f = f
        self.n = len(peers)
        self.current_view = current_view
        self.seqnum = 0
        self.history_digest = "GENESIS"
        self.primary_id = primary_id if primary_id is not None else "1"

    def allocate_seqno(self) -> int:
        self.seqnum += 1
        return self.seqnum

    def advance_history(self, request_digest: str) -> str:
        material = f"{self.history_digest}:{request_digest}".encode("utf-8")
        self.history_digest = hashlib.sha256(material).hexdigest()
        return self.history_digest
    
