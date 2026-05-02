from collections import defaultdict


co_occurrences = defaultdict(set)


class Node:
    node_counter = 0

    def __init__(self, depth, parent=None, path_from_root=None, tree_ref=None):
        self.depth = depth
        self.parent = parent
        self.path_from_root = path_from_root if path_from_root is not None else []
        self.nodes_dict = {}
        self.node_id = Node.node_counter
        Node.node_counter += 1

        if tree_ref is not None:
            tree_ref.nodes_dict[self.node_id] = self


class SplitNode(Node):
    def __init__(self, depth, centroid_left, centroid_right, parent=None, path_from_root=None, tree_ref=None):
        super().__init__(depth, parent, path_from_root, tree_ref=tree_ref)
        self.centroid_left = centroid_left
        self.centroid_right = centroid_right
        self.left_child: Node | None = None
        self.right_child: Node | None = None

    def get_children(self):
        return [c for c in (self.left_child, self.right_child) if c is not None]


class LeafNode(Node):
    def __init__(self, depth, parent=None, path_from_root=None, tree_ref=None):
        super().__init__(depth, parent, path_from_root, tree_ref=tree_ref)
        self.records = []

    def add_record(self, record):
        for existing_rec in self.records:
            co_occurrences[existing_rec['AnonID']].add(record['AnonID'])
            co_occurrences[record['AnonID']].add(existing_rec['AnonID'])

        self.records.append(record)
