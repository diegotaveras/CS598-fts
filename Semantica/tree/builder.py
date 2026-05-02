import os

import numpy as np
import pandas as pd
import dill
from sklearn.preprocessing import normalize
from sklearn.metrics.pairwise import cosine_similarity

from .nodes import LeafNode
from .tree import Tree
from .expansion import expand_neighbors_via_referral_all_users
from .evaluation import (
    calculate_userlevel_overlap_percentage,
    evaluate_query_as_node,
    evaluate_closest_users,
    evaluate_closest_user_chain,
)
from .plots import plot_construct_tree_histograms


def store_tree_locally(tree, path_to_tree_pickle="temp/tree.pkl"):
    os.makedirs(os.path.dirname(path_to_tree_pickle), exist_ok=True)
    with open(path_to_tree_pickle, "wb") as f:
        dill.dump(tree, f)
    print(f"Tree saved to {path_to_tree_pickle}")


def _compute_leaf_cosine_stats(tree, df_avgemb):
    """Compute per-leaf intra-cluster cosine similarity, leaf sizes, and gather all final records."""
    leaf_cosine_sims = []
    nbr_users_per_leaf = []
    final_records = []
    embedding_by_anon = df_avgemb.set_index('AnonID')['embedding']
    for lf in tree.get_all_leaf_nodes():
        final_records.extend(lf.records)

        anon_ids_in_leaf = list(set(tree.get_anon_ids_from_leaf(lf)))
        if not anon_ids_in_leaf:
            continue
        embs = np.array(embedding_by_anon.loc[anon_ids_in_leaf].tolist())
        if embs.shape[0] == 0:
            continue
        nbr_users_per_leaf.append(embs.shape[0])
        cosine_sim_matrix = cosine_similarity(embs)
        np.fill_diagonal(cosine_sim_matrix, np.nan)
        leaf_cosine_sims.append(np.mean(np.nanmean(cosine_sim_matrix, axis=1)))
    return leaf_cosine_sims, nbr_users_per_leaf, final_records


def _build_anonid_to_nodeids(tree):
    """Map each AnonID to the leaf node(s) it landed in (a user can be duplicated across leaves via delta-splitting)."""
    anonid_to_nodeids = {}
    for leaf in tree.get_all_leaf_nodes():
        for rec in leaf.records:
            anon_id = rec['AnonID']
            if anon_id not in anonid_to_nodeids:
                anonid_to_nodeids[anon_id] = []
            anonid_to_nodeids[anon_id].append(leaf.node_id)
    return anonid_to_nodeids


def _build_record_distance_maps(tree, anonid_to_nodeids, M):
    """For each (user, leaf) pair, BFS outward to gather up to M neighbors, bucketed by tree distance."""
    record_distance_map_list = []
    max_distance = tree.maximum_attained_depth * 4
    for anon_id, node_ids in anonid_to_nodeids.items():
        for node_id in node_ids:
            node = tree.nodes_dict[node_id]
            distance_map_for_copy = {}
            total_number_of_neighbors = []

            for dist in range(max_distance + 1):
                nodes_at_dist = tree.nodes_at_distance(node, dist)
                records_at_dist = []
                for n in nodes_at_dist:
                    if isinstance(n, LeafNode):
                        records_at_dist.extend([r['AnonID'] for r in n.records])

                if records_at_dist:
                    total_number_of_neighbors.extend(records_at_dist)
                    total_number_of_neighbors = list(set(total_number_of_neighbors))
                    distance_map_for_copy[dist] = records_at_dist

                if len(total_number_of_neighbors) > M:
                    break

            record_distance_map_list.append({
                'AnonID': anon_id,
                'NodeID': node_id,
                'DistanceMap': distance_map_for_copy
            })
    return record_distance_map_list


def _score_records_by_tree_neighbors(record_distance_map_list, anonid_to_embedding, M):
    """For each (user, leaf), take the M nearest tree-neighbors, compute mean cosine similarity, and capture the candidate set."""
    tree_closest_users_dict = {}
    mean_similarities = {}

    for entry in record_distance_map_list:
        target_anon_id = entry['AnonID']
        node_id = entry['NodeID']
        dist_map = entry['DistanceMap']

        candidates = [target_anon_id]
        for d in sorted(dist_map.keys()):
            for candidate_anon_id in dist_map[d]:
                if candidate_anon_id not in candidates:
                    candidates.append(candidate_anon_id)
                    if len(candidates) == M:
                        break
            if len(candidates) == M:
                break

        if len(candidates) == 0:
            mean_similarities[(target_anon_id, node_id)] = np.nan
            continue

        target_emb = anonid_to_embedding[target_anon_id].reshape(1, -1)
        candidate_embs = np.array([anonid_to_embedding[c] for c in candidates])
        sim_matrix = cosine_similarity(target_emb, candidate_embs)

        tree_closest_users_dict[(target_anon_id, node_id)] = candidates
        mean_similarities[(target_anon_id, node_id)] = np.mean(sim_matrix)

    return tree_closest_users_dict, mean_similarities


def _top_n_absolute_neighbors(cosine_sims, N):
    """Vectorized top-N neighbors per user by cosine similarity, ignoring self via in-place diagonal masking."""
    np.fill_diagonal(cosine_sims.values, -np.inf)
    closest_columns = [f'Closest_{i+1}' for i in range(N)]
    return cosine_sims.apply(
        lambda row: pd.Series(row.nlargest(N).index.tolist(), index=closest_columns),
        axis=1)


def _run_query_evaluation(mode, tree, df_avgemb, user_docs_train, user_docs_test,
                          tree_anon_closest_users_df, M, N):
    """Dispatch to the appropriate query-level evaluator. Returns -1 when `mode` isn't a recognized eval string."""
    if mode == "Query-as-Node":
        store_tree_locally(tree, path_to_tree_pickle="temp/tree.pkl")
        df_q = pd.read_pickle('embedded_queries_df_BERT.pkl').rename(columns={'embedding': 'query_embedding'})
        user_docs_test = pd.merge(df_q, user_docs_test, on='QueryIndex')
        accuracy = evaluate_query_as_node(
            path_to_tree_pickle="temp/tree.pkl",
            user_train_docs=user_docs_train,
            user_docs_test=user_docs_test,
            M=M,
            N=N,
            log_filename="querynode_progress.log"
        )
        print("QueryAsNode accuracy:", accuracy * 100)
        return accuracy

    if mode == 'Closest-Users':
        df_q = pd.read_pickle('embedded_queries_df_BERT.pkl').rename(columns={'embedding': 'query_embedding'})
        user_docs_test = pd.merge(user_docs_test, df_q, on='QueryIndex')
        accuracy = evaluate_closest_users(df_avgemb,
                                          user_docs_train,
                                          user_docs_test,
                                          tree_anon_closest_users_df,
                                          top_k=50)
        print(f'QueryLevel with Neighbor similarity: {accuracy * 100}')
        return accuracy

    if mode == 'Closest-User-Chain':
        df_q = pd.read_pickle('embedded_queries_df_BERT.pkl').rename(columns={'embedding': 'query_embedding'})
        user_docs_test = pd.merge(user_docs_test, df_q, on='QueryIndex')
        accuracy = evaluate_closest_user_chain(df_avgemb,
                                               user_docs_train,
                                               user_docs_test,
                                               tree_anon_closest_users_df,
                                               top_k=50)
        print(f'QueryLevel with Neighbor similarity: {accuracy * 100}')
        return accuracy

    return -1


def construct_tree(record_limit_per_leafnode, N, DELTA,
                   df_avgemb, user_docs_train, user_docs_test, cosine_sims, print_hists=False, M=25,
                   perform_query_test=True, EXPANSION_ROUNDS=10):
    """Build a hierarchical kmeans tree, score per-user neighbor accuracy, and optionally evaluate at query level."""
    # ---- Build tree by inserting each user's averaged embedding ----
    df_avgemb['embedding'] = list(normalize(np.vstack(df_avgemb['embedding']), axis=1))

    tree = Tree(record_limit_per_leafnode=record_limit_per_leafnode, delta=DELTA)
    for anon_id, emb in zip(df_avgemb['AnonID'].values, df_avgemb['embedding'].values):
        record = {'AnonID': anon_id, 'embedding': emb, 'path': [], 'split_distances': []}
        tree.position_record(record)

    # ---- Per-leaf cosine stats; rebuild df_avgemb from records that landed in leaves ----
    leaf_cosine_sims, nbr_users_per_leaf, final_records = _compute_leaf_cosine_stats(tree, df_avgemb)
    df_avgemb = pd.DataFrame(final_records)

    # ---- Tree-neighborhood discovery + per-record similarity scoring ----
    anonid_to_nodeids = _build_anonid_to_nodeids(tree)
    record_distance_map_list = _build_record_distance_maps(tree, anonid_to_nodeids, M)

    anonid_to_embedding = dict(zip(df_avgemb['AnonID'], df_avgemb['embedding']))
    tree.anonid_to_embedding = anonid_to_embedding
    tree_closest_users_dict, mean_similarities = _score_records_by_tree_neighbors(
        record_distance_map_list, anonid_to_embedding, M)

    # ---- Ground-truth top-N neighbors + tree-clone DataFrame ----
    absolute_closest_users_df = _top_n_absolute_neighbors(cosine_sims, N)

    tree_clone_closest_users_df = pd.DataFrame.from_dict(
        tree_closest_users_dict, orient='index',
        columns=[f'Closest_{i+1}' for i in range(len(next(iter(tree_closest_users_dict.values()))))])
    tree_clone_closest_users_df.index = pd.MultiIndex.from_tuples(
        tree_clone_closest_users_df.index, names=["AnonID", "NodeID"])

    # ---- Tree-based neighbors per user (deduped across leaf copies), then expand via referrals ----
    tree_anon_neighbors: dict = {
        anon_id: set(group.values.flatten())
        for anon_id, group in tree_clone_closest_users_df.groupby(level='AnonID')
    }

    user_list = list(absolute_closest_users_df.index)
    results_all_users = expand_neighbors_via_referral_all_users(
        user_ids=user_list,
        tree_anon_closest_users_df=tree_anon_neighbors,
        cosine_sims=cosine_sims,
        num_rounds=EXPANSION_ROUNDS,
        DELTA=DELTA,
        K=N,
        absolute_closest_users_df=absolute_closest_users_df
    )

    # ---- Per-user reduction: count overlap with absolute top-N, record final candidate set ----
    common_counts = {}
    total_nbr_connected: dict = {}
    tree_anon_results: dict = {}
    for i, u in enumerate(user_list):
        final_topk_u, all_shared_u = results_all_users[u]
        total_nbr_connected[u] = len(all_shared_u)
        common_counts[u] = len(set(absolute_closest_users_df.loc[u].values).intersection(final_topk_u))
        tree_anon_results[u] = {'Closest_Users': list(final_topk_u), 'All_Shared': list(all_shared_u)}
        if i % 1000 == 0:
            print(i)

    # ---- Convert reduction dicts to DataFrames and merge into the final per-user table ----
    common_counts_df = pd.DataFrame.from_dict(common_counts, orient='index', columns=['Common_Count'])
    tree_anon_closest_users_df = pd.DataFrame.from_dict(tree_anon_results, orient='index')
    total_nbr_connected_df = pd.DataFrame.from_dict(total_nbr_connected, orient='index', columns=['Connected_Users'])

    results_df_abs = calculate_userlevel_overlap_percentage(user_docs_train, user_docs_test, absolute_closest_users_df)
    results_df_tree_anon = calculate_userlevel_overlap_percentage(user_docs_train, user_docs_test, tree_anon_closest_users_df[['Closest_Users']])
    tree_anon_closest_users_df['Number_Queried'] = tree_anon_closest_users_df['Closest_Users'].apply(lambda l: len(l))

    tree_anon_closest_users_df = pd.merge(total_nbr_connected_df, tree_anon_closest_users_df, left_index=True,
                                          right_index=True)
    tree_anon_closest_users_df.to_pickle(os.path.join('results', 'tree_anon_closest_users_df.pkl'))

    # ---- Summary metrics and console printout ----
    results_df_tree = results_df_tree_anon.groupby('AnonID')['OverlapPercentage'].max().reset_index()
    results_df_abs.drop(columns=['EndNodeID'], inplace=True)

    train_doc_nbr = user_docs_train.groupby('AnonID')['DocIndex'].count().iloc[0]
    test_doc_nbr = user_docs_test.groupby('AnonID')['DocIndex'].count().iloc[0]

    cmn_counts_median = common_counts_df.Common_Count.median()
    cmn_counts_mean = common_counts_df.Common_Count.mean()
    nbr_users_contacted = np.mean(tree_anon_closest_users_df['All_Shared'].apply(lambda i: len(i)).mean())
    tree_overlap = results_df_tree["OverlapPercentage"].mean()
    absolute_overlap = results_df_abs["OverlapPercentage"].mean()

    if float(DELTA) == 0.:
        assert tree_anon_closest_users_df['Number_Queried'].min() == tree_anon_closest_users_df['Number_Queried'].max()

    print(f'Median Common Closest {N} Users: {cmn_counts_median}')
    print(f'Mean Common Closest {N} Users: {cmn_counts_mean}')
    print()
    print(f'Accuracy with {train_doc_nbr} train and {test_doc_nbr}:')
    print(f'Absolute: {absolute_overlap}')
    print(f'Tree: {tree_overlap}')
    print(f'Number of users contacted mean: ', nbr_users_contacted)

    # ---- Optional query-level evaluation (returns -1 when no eval mode is selected) ----
    querylevel_accuracy = _run_query_evaluation(
        perform_query_test, tree, df_avgemb, user_docs_train, user_docs_test,
        tree_anon_closest_users_df, M, N)

    # ---- Optional diagnostic histograms ----
    if print_hists:
        plot_construct_tree_histograms(
            DELTA=DELTA,
            N=N,
            leaf_cosine_sims=leaf_cosine_sims,
            total_nbr_connected=total_nbr_connected,
            mean_similarities=mean_similarities,
            nbr_users_per_leaf=nbr_users_per_leaf,
            common_counts_df=common_counts_df,
            results_df_abs=results_df_abs,
            results_df_tree=results_df_tree,
            tree_clone_closest_users_df=tree_clone_closest_users_df,
        )

    return (cmn_counts_mean, cmn_counts_median, nbr_users_contacted, tree_overlap,
            absolute_overlap, tree_anon_closest_users_df, querylevel_accuracy * 100)
