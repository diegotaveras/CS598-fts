import logging
import os
import pickle as pkl

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np

from baselines.diffusion import (
    personalized_pagerank,
    execute_query,
    load_diffusion_data,
    build_graph,
    build_node_embeddings,
)


def run_experiment(
    *,
    alpha,
    number_docs_to_test,
    G,
    node_embeddings,
    test_data,
    query_embs,
    doc_to_users,
    max_hops=50,
    max_plot_dist=2000,
):
    logging.info(f"[Process α={alpha}] Building adjacency matrix...")
    node_list = list(G.nodes())
    A = nx.to_scipy_sparse_array(G, nodelist=node_list, dtype=np.float32).tocsc()

    orig_emb = {user: node_embeddings[user].copy() for user in node_list}

    logging.info(f"[Process α={alpha}] Running PPR diffusion...")
    ppr_embs = personalized_pagerank(orig_emb, A, alpha, node_list, max_iter=500)

    distance_buckets = {}
    test_subset = test_data.sample(n=min(number_docs_to_test, len(test_data)))

    for i, (_, row) in enumerate(test_subset.iterrows()):
        if i % 1000 == 0 and i > 0:
            logging.info(f"[Process α={alpha}] Processed {i} rows out of {len(test_subset)}")

        golden_doc = row['DocIndex']
        if golden_doc not in doc_to_users:
            continue
        golden_nodes = doc_to_users[golden_doc]
        if not golden_nodes:
            continue

        qindex = row['QueryIndex']
        q_row = query_embs[query_embs['QueryIndex'] == qindex]
        if q_row.empty:
            continue
        query_embedding = q_row.iloc[0]['QueryEmbedding']

        start_node = row['AnonID']
        if start_node not in G:
            continue

        bfs_map = nx.single_source_shortest_path_length(G, start_node)
        distances_to_golden = [bfs_map[gn] for gn in golden_nodes if gn in bfs_map]
        dist_val = min(distances_to_golden) if distances_to_golden else 999999

        hit, _ = execute_query(
            query_embedding, start_node, golden_nodes, G, ppr_embs, max_hops=max_hops,
        )
        distance_buckets.setdefault(dist_val, []).append(1 if hit else 0)

    if not distance_buckets:
        return 0.0, {}

    total_hits = sum(sum(lst) for lst in distance_buckets.values())
    total_count = sum(len(lst) for lst in distance_buckets.values())
    overall_accuracy = total_hits / total_count

    final_acc = {
        dist_val: float(np.mean(hits_list))
        for dist_val, hits_list in distance_buckets.items()
        if dist_val <= max_plot_dist
    }
    return overall_accuracy, final_acc


def main():
    graph_type = 'barabasi'  # or 'small_world'
    barabasi_graph_m_value = 86
    small_world_k = 10
    small_world_p = 0.5

    nbr_train_recs = 20
    nbr_test_recs = 10
    sample_rdn_test = True
    filter_out_users_with_few_docs = False
    num_docs_to_test = 500000
    alpha_values = [0.9]

    if graph_type == 'small_world':
        experiment_name = (
            f'diffusionBaseline_{graph_type}_results_'
            f'smallUserFilter{filter_out_users_with_few_docs}_p{small_world_p}_k{small_world_k}'
        )
    else:
        experiment_name = (
            f'diffusionBaseline_{graph_type}_results_'
            f'smallUserFilter{filter_out_users_with_few_docs}_m{barabasi_graph_m_value}'
        )

    logging.basicConfig(
        filename=f'{experiment_name}.log',
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        force=True,
    )
    logging.info("Loading large data...")

    df_avgemb, _data, test_data, query_embs, doc_to_users = load_diffusion_data(
        nbr_train_recs=nbr_train_recs,
        nbr_test_records=nbr_test_recs,
        sample_rdn_test=sample_rdn_test,
        filter_out_users_with_few_docs=filter_out_users_with_few_docs,
    )

    unique_users = df_avgemb['AnonID'].unique()
    G = build_graph(
        unique_users, graph_type,
        m=barabasi_graph_m_value, k=small_world_k, p=small_world_p,
    )
    node_embeddings = build_node_embeddings(df_avgemb, G)
    logging.info("Data loaded successfully!")

    final_results = {}
    for alpha in alpha_values:
        overall_accuracy, acc_map = run_experiment(
            alpha=alpha,
            number_docs_to_test=num_docs_to_test,
            G=G,
            node_embeddings=node_embeddings,
            test_data=test_data,
            query_embs=query_embs,
            doc_to_users=doc_to_users,
        )
        final_results[alpha] = (overall_accuracy, acc_map)
        logging.info(f"[Process α={alpha}] Completed successfully with accuracy {overall_accuracy}.")

    output_path = os.path.join(
        'results',
        f'diffusionBaseline_results_smallUserFilter{filter_out_users_with_few_docs}'
        f'_m{barabasi_graph_m_value}.pkl',
    )
    with open(output_path, 'wb') as f:
        pkl.dump(final_results, f)

    plt.figure(figsize=(10, 6))
    max_dist_to_plot = 1000
    for alpha in alpha_values:
        if alpha not in final_results:
            continue
        _, acc_map = final_results[alpha]
        distances_to_plot = sorted(d for d in acc_map.keys() if d <= max_dist_to_plot)
        accuracies = [acc_map[d] for d in distances_to_plot]
        plt.plot(distances_to_plot, accuracies, marker='o', label=f'α={alpha}')

    plt.xlabel('Distance from start_node to golden node(s)')
    plt.ylabel('Accuracy')
    plt.title(f'Accuracy vs. distance (sample_rdn = {sample_rdn_test})')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    main()
