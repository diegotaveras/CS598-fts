import copy
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

from .nodes import Node, SplitNode, LeafNode


class Tree:
    def __init__(self, record_limit_per_leafnode, delta):
        self.queries_done = []
        self.nodes_dict = {}
        self.root: Node = LeafNode(depth=0, parent=None, path_from_root=[], tree_ref=self)
        self.root.path_from_root = [self.root.node_id]
        self.maximum_attained_depth = 0
        self.record_limit_per_leafnode = record_limit_per_leafnode
        self.delta = delta
        self.anonid_to_embedding: dict | None = None

    def get_all_nodes(self):
        all_nodes = []
        queue: list[Node] = [self.root]
        while queue:
            node = queue.pop(0)
            all_nodes.append(node)
            if isinstance(node, SplitNode):
                queue.extend(node.get_children())
        return all_nodes

    def get_split_nodes(self, depth):
        """
        Retrieves all nodes at a given depth. If depth = maximum_attained_depth,
        we might return leaf nodes. Otherwise, return split nodes at that depth.
        """
        nodes_at_depth = []
        queue: list[Node] = [self.root]
        while queue:
            node = queue.pop(0)
            if node.depth == depth:
                nodes_at_depth.append(node)
            elif node.depth < depth:
                if isinstance(node, SplitNode):
                    queue.extend(node.get_children())
        return nodes_at_depth

    def turn_leafnode_into_splitnode(self, leaf_node, delta=None):
        """Perform K-means clustering on the leaf node's records and turn it into a split node."""
        if delta is None:
            delta = self.delta
        embeddings = np.array([rec['embedding'] for rec in leaf_node.records])

        kmeans = KMeans(n_clusters=2, random_state=0)
        labels = kmeans.fit_predict(embeddings)
        centroid_left = kmeans.cluster_centers_[0]
        centroid_right = kmeans.cluster_centers_[1]

        split_node = SplitNode(depth=leaf_node.depth,
                               centroid_left=centroid_left,
                               centroid_right=centroid_right,
                               parent=leaf_node.parent,
                               path_from_root=leaf_node.path_from_root,
                               tree_ref=self)

        left_leaf = LeafNode(depth=leaf_node.depth + 1,
                             parent=split_node,
                             path_from_root=split_node.path_from_root + [split_node.node_id],
                             tree_ref=self)
        right_leaf = LeafNode(depth=leaf_node.depth + 1,
                              parent=split_node,
                              path_from_root=split_node.path_from_root + [split_node.node_id],
                              tree_ref=self)

        for rec, lbl in zip(leaf_node.records, labels):
            emb = rec['embedding']
            dist_left = np.linalg.norm(emb - centroid_left)
            dist_right = np.linalg.norm(emb - centroid_right)

            rec['split_distances'].append((split_node.node_id, round(dist_left, 3), round(dist_right, 3), round(abs(dist_left - dist_right), 3)))
            rec['path'].append(split_node.node_id)

            if abs(dist_left - dist_right) < delta:
                rec['split_distances'].append((f'Delta too small, duplicating to nodes: '
                                               f'{left_leaf.node_id}, {right_leaf.node_id}'))
                left_leaf.add_record(rec)
                right_leaf.add_record(rec)
            else:
                if lbl == 0:
                    left_leaf.add_record(rec)
                else:
                    right_leaf.add_record(rec)

        if left_leaf.depth > self.maximum_attained_depth:
            self.maximum_attained_depth = left_leaf.depth
        if right_leaf.depth > self.maximum_attained_depth:
            self.maximum_attained_depth = right_leaf.depth

        split_node.left_child = left_leaf
        split_node.right_child = right_leaf

        return split_node

    def position_record(self, record):
        self._insert_record_downstream(self.root, record)

    def _insert_record_downstream(self, current_node, record):
        """
        Traverse the tree starting from the root:
          - If current node is a split node, determine which centroid is closer
            and record distances.
            If distance to centroids too small, copy record and send it both ways
          - If it's a leaf node, add the record there.
            If the leaf node exceeds the limit, split it.
        """
        while True:
            if isinstance(current_node, SplitNode):
                left, right = current_node.left_child, current_node.right_child
                if left is None and right is None:
                    raise ValueError(f"SplitNode {current_node.node_id} has no children")

                embedding = record['embedding']
                dist_left = np.linalg.norm(embedding - current_node.centroid_left)
                dist_right = np.linalg.norm(embedding - current_node.centroid_right)

                diff = abs(dist_left - dist_right)
                if diff < self.delta and left is not None and right is not None:
                    record_copy = copy.deepcopy(record)
                    self._insert_record_downstream(left, record)
                    self._insert_record_downstream(right, record_copy)
                    record['split_distances'].append((current_node.node_id, round(dist_left, 3), round(dist_right, 3), round(diff, 3), 'DUPLICATING'))
                    return
                else:
                    record['split_distances'].append((current_node.node_id, round(dist_left, 3), round(dist_right, 3), round(diff, 3)))
                    if left is None:
                        current_node = right
                    elif right is None:
                        current_node = left
                    elif dist_left < dist_right:
                        current_node = left
                    else:
                        current_node = right

            elif isinstance(current_node, LeafNode):
                record['path'] = current_node.path_from_root[:]
                current_node.add_record(record)
                if len(current_node.records) > self.record_limit_per_leafnode:
                    self.replace_leaf_with_splitnode(current_node)
                return

    def replace_leaf_with_splitnode(self, leaf_node, delta=None):
        """Replace a leaf node in the tree with a split node after KMeans."""
        if delta is None:
            delta = self.delta
        if self.root is leaf_node:
            self.root = self.turn_leafnode_into_splitnode(leaf_node, delta=delta)
            return

        parent, is_left_child = self._find_parent(self.root, leaf_node)
        if parent is not None:
            new_split = self.turn_leafnode_into_splitnode(leaf_node, delta=delta)
            if is_left_child:
                parent.left_child = new_split
            else:
                parent.right_child = new_split

    def _find_parent(self, current_node, target_node):
        """
        Helper function to find the parent of target_node.
        Returns a tuple (parent_node, is_left_child) or (None, None) if not found.
        """
        if isinstance(current_node, SplitNode):
            if current_node.left_child is target_node:
                return current_node, True
            if current_node.right_child is target_node:
                return current_node, False

            if current_node.left_child is not None:
                left_result = self._find_parent(current_node.left_child, target_node)
                if left_result[0] is not None:
                    return left_result

            if current_node.right_child is not None:
                right_result = self._find_parent(current_node.right_child, target_node)
                if right_result[0] is not None:
                    return right_result

        return None, None

    def get_all_leaf_nodes(self):
        """
        Returns a list of all leaf nodes in the tree.
        """
        leaf_nodes = []
        queue: list[Node] = [self.root]
        while queue:
            node = queue.pop(0)
            if isinstance(node, LeafNode):
                leaf_nodes.append(node)
            elif isinstance(node, SplitNode):
                queue.extend(node.get_children())
        return leaf_nodes

    def get_anon_ids_from_leaf(self, leaf_node):
        """
        Given a leaf node, return a list of AnonIDs present in that leaf node.
        """
        return [rec['AnonID'] for rec in leaf_node.records]

    def get_path_to_node(self, node):
        """
        Returns the path_from_root for a given node, i.e., the sequence of "left"/"right" moves.
        """
        return node.path_from_root

    def nodes_at_distance(self, start_node, distance):
        """
        Returns all nodes exactly 'distance' edges away from start_node.
        distance=0 would return [start_node] itself.
        """
        if distance == 0:
            return [start_node]

        visited = set()
        queue: list[tuple[Node, int]] = [(start_node, 0)]
        result = []
        while queue:
            current, dist = queue.pop(0)
            if current not in visited:
                visited.add(current)
                neighbors = []
                if current.parent is not None:
                    neighbors.append(current.parent)
                if isinstance(current, SplitNode):
                    neighbors.extend(current.get_children())

                for n in neighbors:
                    if n not in visited:
                        if dist + 1 == distance:
                            result.append(n)
                        else:
                            queue.append((n, dist + 1))
        return result

    def get_leaf_nodes_for_query(self, query_embedding, delta=None):
        """
        Routes a query embedding from the root to the appropriate leaf node(s).
        If the distance to both child centroids is less than delta, the query
        is duplicated. Returns *all* leaf nodes where this query would land.
        """
        if delta is None:
            delta = self.delta
        leaf_nodes = []
        stack: list[tuple[Node, np.ndarray]] = [(self.root, query_embedding)]

        while stack:
            current_node, q_emb = stack.pop()
            if isinstance(current_node, SplitNode):
                left, right = current_node.left_child, current_node.right_child
                dist_left = np.linalg.norm(q_emb - current_node.centroid_left)
                dist_right = np.linalg.norm(q_emb - current_node.centroid_right)
                diff = abs(dist_left - dist_right)

                if diff < delta and left is not None and right is not None:
                    stack.append((left, q_emb))
                    stack.append((right, q_emb))
                else:
                    if left is None and right is not None:
                        stack.append((right, q_emb))
                    elif right is None and left is not None:
                        stack.append((left, q_emb))
                    elif left is not None and right is not None:
                        if dist_left < dist_right:
                            stack.append((left, q_emb))
                        else:
                            stack.append((right, q_emb))

            elif isinstance(current_node, LeafNode):
                leaf_nodes.append(current_node)

        return leaf_nodes

    def get_closest_users_for_query(self, query_embedding, M=25, delta=None, N=10):
        """
        1. Route the query to one or more leaf nodes.
        2. For each leaf node, do a BFS outward (using nodes_at_distance) until
           we've collected >= M AnonIDs.
        3. Compute the distances from the query to these candidate AnonIDs.
        4. Return the top M closest AnonIDs by L2 distance (or you could
           switch to cosine similarity if desired).

        The query is *not* stored in the tree; this is ephemeral.
        """
        if delta is None:
            delta = self.delta
        target_leaf_nodes = self.get_leaf_nodes_for_query(query_embedding, delta=delta)

        candidate_anon_ids = set()
        max_distance = self.maximum_attained_depth * 4

        for leaf_node in target_leaf_nodes:
            for dist in range(max_distance + 1):
                nodes_at_dist = self.nodes_at_distance(leaf_node, dist)
                for n in nodes_at_dist:
                    if isinstance(n, LeafNode) and len(candidate_anon_ids) < M:
                        candidate_anon_ids.update(r['AnonID'] for r in n.records)
                if len(candidate_anon_ids) >= M:
                    break

        if self.anonid_to_embedding is None:
            raise ValueError(
                "You must set 'self.anonid_to_embedding' as a dict: {AnonID: embedding_array} "
                "before calling get_closest_users_for_query()."
            )
        anonid_to_embedding = self.anonid_to_embedding

        candidate_anon_ids = [
            cid for cid in candidate_anon_ids if cid in anonid_to_embedding
        ]

        query_embedding = query_embedding.reshape(1, -1)
        distances = []
        for cid in candidate_anon_ids:
            cand_emb = anonid_to_embedding[cid].reshape(1, -1)
            dist = 1 - cosine_similarity(query_embedding, cand_emb)[0][0]
            distances.append((cid, dist))

        distances.sort(key=lambda x: x[1])
        closest_anon_ids = [cid for cid, _ in distances[:N]]
        return closest_anon_ids
