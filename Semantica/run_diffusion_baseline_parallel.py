import logging
import os
import pickle as pkl
from multiprocessing import Manager, Pool, cpu_count
from typing import Any

import networkx as nx
import numpy as np

from baselines.diffusion import (
    personalized_pagerank,
    execute_query,
    load_diffusion_data,
    build_graph,
    build_node_embeddings,
)


# Pool-worker globals: populated by init_pool in each worker process, then read by
# process_single_query. Annotated Any because the static initial value (None) does
# not reflect their runtime type, and Pylance otherwise treats them as None-only.
doc_to_users_glob: Any = None
query_embs_glob: Any = None
G_glob: Any = None
ppr_embs_glob: Any = None
max_hops_glob: Any = None
counter_glob: Any = None
lock_glob: Any = None


def init_pool(doc_to_users, query_embs, G, ppr_embs, max_hops, counter, lock):
    global doc_to_users_glob, query_embs_glob, G_glob, ppr_embs_glob
    global max_hops_glob, counter_glob, lock_glob

    doc_to_users_glob = doc_to_users
    query_embs_glob = query_embs
    G_glob = G
    ppr_embs_glob = ppr_embs
    max_hops_glob = max_hops
    counter_glob = counter
    lock_glob = lock


def process_single_query(row):
    golden_doc = row['DocIndex']
    if golden_doc not in doc_to_users_glob:
        return None

    golden_nodes = doc_to_users_glob[golden_doc]
    if not golden_nodes:
        return None

    qindex = row['QueryIndex']
    q_row = query_embs_glob[query_embs_glob['QueryIndex'] == qindex]
    if q_row.empty:
        return None

    query_embedding = q_row.iloc[0]['QueryEmbedding']
    start_node = row['AnonID']
    if start_node not in G_glob:
        return None

    bfs_map = nx.single_source_shortest_path_length(G_glob, start_node)
    distances_to_golden = [bfs_map[gn] for gn in golden_nodes if gn in bfs_map]
    dist_val = min(distances_to_golden) if distances_to_golden else 999999

    hit, _ = execute_query(
        query_embedding=query_embedding,
        start_node=start_node,
        golden_nodes=golden_nodes,
        G=G_glob,
        ppr_embeddings=ppr_embs_glob,
        max_hops=max_hops_glob,
    )

    with lock_glob:
        counter_glob.value += 1
        if counter_glob.value % 100 == 0:
            logging.info(f"[PID={os.getpid()}] Processed {counter_glob.value} queries so far...")

    return (dist_val, 1 if hit else 0)


def run_experiment(
    *,
    max_hops,
    number_docs_to_test,
    alpha,
    doc_to_users,
    query_embs,
    test_data,
    node_embeddings,
    G,
    counter,
    lock,
    max_plot_dist=2000,
):
    logging.info(f"[max_hops={max_hops}] Building adjacency matrix...")
    node_list = list(G.nodes())
    A = nx.to_scipy_sparse_array(G, nodelist=node_list, dtype=np.float32).tocsc()

    orig_emb = {user: node_embeddings[user].copy() for user in node_list}

    logging.info(f"[max_hops={max_hops}] Running PPR diffusion (alpha={alpha})...")
    ppr_embs = personalized_pagerank(orig_emb, A, alpha, node_list)

    test_subset = test_data.sample(n=min(number_docs_to_test, len(test_data))).copy()
    rows_for_pool = [row for _, row in test_subset.iterrows()]

    n_cores = cpu_count()
    logging.info(f"[max_hops={max_hops}] Starting parallel query processing with {n_cores} cores.")
    with Pool(
        processes=n_cores,
        initializer=init_pool,
        initargs=(doc_to_users, query_embs, G, ppr_embs, max_hops, counter, lock),
    ) as pool:
        results = pool.map(process_single_query, rows_for_pool, chunksize=500)

    distance_buckets = {}
    for res in results:
        if res is None:
            continue
        dist_val, hit_flag = res
        distance_buckets.setdefault(dist_val, []).append(hit_flag)

    if not distance_buckets:
        return 0.0, {}

    total_hits = sum(sum(hits_list) for hits_list in distance_buckets.values())
    total_count = sum(len(hits_list) for hits_list in distance_buckets.values())
    overall_accuracy = total_hits / total_count

    final_acc = {
        dist_val: float(np.mean(hits_list))
        for dist_val, hits_list in distance_buckets.items()
        if dist_val <= max_plot_dist
    }
    return overall_accuracy, final_acc


def main():
    m = 104
    alpha = 0.9
    max_hops_list = [600]
    num_docs_to_test = 500000

    logging.basicConfig(
        filename=f'graphDiffusion_baseline_m{m}_alpha01_n600.log',
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        force=True,
    )
    logging.info("Loading data...")

    df_avgemb, data, test_data, query_embs, doc_to_users = load_diffusion_data(
        nbr_train_recs=20,
        nbr_test_records=10,
        sample_rdn_test=True,
        filter_out_users_with_few_docs=True,
    )
    logging.info(f'{data.shape}, {test_data.shape}, {df_avgemb.shape}')
    logging.info(f"Performing analysis of {test_data.shape[0]} queries")

    unique_users = df_avgemb['AnonID'].unique()
    G = build_graph(unique_users, 'barabasi', m=m)
    node_embeddings = build_node_embeddings(df_avgemb, G)
    logging.info("Data loaded!")

    manager = Manager()
    counter = manager.Value('i', 0)
    lock = manager.Lock()

    final_results = {}
    for mh in max_hops_list:
        counter.value = 0
        logging.info(f"=== Starting experiment for max_hops={mh} ===")

        overall_accuracy, acc_map = run_experiment(
            max_hops=mh,
            number_docs_to_test=num_docs_to_test,
            alpha=alpha,
            doc_to_users=doc_to_users,
            query_embs=query_embs,
            test_data=test_data,
            node_embeddings=node_embeddings,
            G=G,
            counter=counter,
            lock=lock,
        )
        final_results[mh] = (overall_accuracy, acc_map)
        logging.info(f"=== Finished max_hops={mh}, Accuracy={overall_accuracy:.4f} ===")

    output_path = f"graphDiffusion_baseline_m{m}_alpha{str(alpha).replace('.', '')}_n600_test.pkl"
    with open(output_path, 'wb') as f:
        pkl.dump(final_results, f)

    logging.info("All done.")


if __name__ == '__main__':
    main()
