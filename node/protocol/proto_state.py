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
        self.primary_id = primary_id if primary_id is not None else "1"
    