import numpy as np
import pandas as pd
import networkx as nx
from scipy.sparse import eye
from sklearn.metrics.pairwise import cosine_similarity

from embedding import construct_anon_embedding


def personalized_pagerank(orig_emb, A, alpha, node_list, max_iter=100):
    """Iterative PPR diffusion: X^(t+1) = (1-alpha)*P*X^(t) + alpha*X^(0)."""
    initial_embeddings_matrix = np.array([orig_emb[node] for node in node_list])
    embeddings_matrix = initial_embeddings_matrix.copy()
    row_sums = np.array(A.sum(axis=1)).flatten()
    row_sums[row_sums == 0] = 1.0
    P = A.multiply(1 / row_sums[:, np.newaxis])
    for _ in range(max_iter):
        embeddings_matrix = (1 - alpha) * P.dot(embeddings_matrix) + alpha * initial_embeddings_matrix
    return {node_list[i]: embeddings_matrix[i] for i in range(len(node_list))}


def fast_pagerank(orig_emb, A, alpha, node_list):
    """Closed-form PPR: X = alpha * (I - (1-alpha)*P)^(-1) * X^(0)."""
    num_nodes = len(node_list)
    E0 = np.array([orig_emb[node] for node in node_list])
    A_norm = A.copy()
    row_sums = np.array(A_norm.sum(axis=1)).flatten()
    row_sums[row_sums == 0] = 1.0
    A_norm = A_norm.multiply(1 / row_sums[:, np.newaxis])
    I = eye(num_nodes, format='csr')
    M = I - (1 - alpha) * A_norm
    M_dense = M.toarray()
    M_inv = np.linalg.inv(M_dense)
    E = alpha * M_inv.dot(E0)
    return {node_list[i]: E[i] for i in range(num_nodes)}


def execute_query(query_embedding, start_node, golden_nodes, G, ppr_embeddings, max_hops=50):
    """Greedy cosine-similarity walk over G; returns (hit, hops)."""
    visited = set()
    current_node = start_node

    for hop in range(max_hops):
        if current_node in golden_nodes:
            return True, hop
        visited.add(current_node)

        neighbors = list(G.neighbors(current_node))
        if not neighbors:
            return False, hop

        unvisited = [n for n in neighbors if n not in visited]
        if not unvisited:
            unvisited = neighbors

        best_score = -1e9
        best_neighbor = None
        for nbr in unvisited:
            emb = ppr_embeddings[nbr]
            if np.isnan(emb).any():
                sim = -1e6
            else:
                sim = cosine_similarity(
                    query_embedding.reshape(1, -1),
                    emb.reshape(1, -1)
                )[0, 0]
            if sim > best_score:
                best_score = sim
                best_neighbor = nbr

        current_node = best_neighbor

    return False, max_hops


def load_diffusion_data(nbr_train_recs, nbr_test_records, sample_rdn_test, filter_out_users_with_few_docs):
    """
    Returns (df_avgemb, train_data, test_data, query_embs, doc_to_users).
    df_avgemb has 'AnonEmbedding' column; query_embs has 'QueryEmbedding' column.
    Both embedding columns are np.array. doc_to_users maps DocIndex -> set(AnonID).
    """
    df_avgemb, train_data, test_data, _ = construct_anon_embedding(
        nbr_train_recs=nbr_train_recs,
        nbr_test_records=nbr_test_records,
        sample_rdn_test=sample_rdn_test,
        filter_out_users_with_few_docs=filter_out_users_with_few_docs,
    )
    df_avgemb = df_avgemb.rename(columns={'embedding': 'AnonEmbedding'})
    query_embs = pd.read_pickle('embedded_queries_df_BERT.pkl').rename(
        columns={'embedding': 'QueryEmbedding'}
    )

    df_avgemb['AnonEmbedding'] = df_avgemb['AnonEmbedding'].apply(lambda x: np.array(x))
    query_embs['QueryEmbedding'] = query_embs['QueryEmbedding'].apply(lambda x: np.array(x))

    doc_to_users = {}
    for _, row in train_data.iterrows():
        doc_to_users.setdefault(row['DocIndex'], set()).add(row['AnonID'])

    return df_avgemb, train_data, test_data, query_embs, doc_to_users


def build_graph(unique_users, graph_type, *, m=None, k=None, p=None):
    """Construct nx graph and relabel nodes to AnonID. graph_type in {'barabasi', 'small_world'}."""
    n = len(unique_users)
    if graph_type == 'barabasi':
        G_temp = nx.barabasi_albert_graph(n=n, m=m)
    elif graph_type == 'small_world':
        G_temp = nx.watts_strogatz_graph(n=n, k=k, p=p)
    else:
        raise ValueError(f"Unknown graph_type: {graph_type}")

    idx_to_user = dict(enumerate(unique_users))
    return nx.relabel_nodes(G_temp, idx_to_user)


def build_node_embeddings(df_avgemb, G):
    """Returns dict[AnonID] -> embedding. Zero-fills G nodes missing from df_avgemb."""
    node_embeddings = {}
    for _, row in df_avgemb.iterrows():
        node_embeddings[row['AnonID']] = row['AnonEmbedding']

    if node_embeddings:
        zero_vec = np.zeros_like(next(iter(node_embeddings.values())))
    else:
        zero_vec = np.zeros(768)

    for user_id in G.nodes():
        if user_id not in node_embeddings:
            node_embeddings[user_id] = zero_vec

    return node_embeddings
