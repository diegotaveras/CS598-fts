"""User-level and query-level evaluation of the tree's discovered neighborhoods."""
import os
from multiprocessing import Pool, cpu_count

import dill
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


def calculate_userlevel_overlap_percentage(user_docs_train, user_docs_test, tree_clone_closest_users_df):
    """
    Calculates the percentage of documents that users are looking for, held by their closest users.

    Parameters:
        user_docs_train (pd.DataFrame): DataFrame with columns 'AnonID', 'DocIndex' representing documents users hold.
        user_docs_test (pd.DataFrame): DataFrame with columns 'AnonID', 'DocIndex' representing documents users are looking for.
        tree_clone_closest_users_df (pd.DataFrame): DataFrame with columns 'Closest1', 'Closest2', ..., 'ClosestN' representing the closest users.

    Returns:
        pd.DataFrame: A DataFrame with columns 'AnonID' and 'OverlapPercentage'.
    """
    user_train_docs = user_docs_train.groupby('AnonID')['DocIndex'].apply(set).to_dict()
    user_test_docs = user_docs_test.groupby('AnonID')['DocIndex'].apply(set).to_dict()

    single_column = tree_clone_closest_users_df.shape[1] == 1
    results = []

    for tup in tree_clone_closest_users_df.itertuples(index=True, name=None):
        idx_val = tup[0]
        if isinstance(idx_val, tuple):
            u, node_id = idx_val[0], idx_val[1]
        else:
            u, node_id = idx_val, None

        if u not in user_test_docs:
            continue

        closest_users = tup[1] if single_column else tup[1:]

        test_docs = user_test_docs[u]
        closest_users_docs = set()
        for closest_user in closest_users:
            closest_users_docs.update(user_train_docs.get(closest_user, set()))

        overlap_docs = test_docs.intersection(closest_users_docs)
        overlap_percentage = (len(overlap_docs) / len(test_docs)) * 100 if test_docs else 0

        results.append({'AnonID': u, 'EndNodeID': node_id, 'OverlapPercentage': overlap_percentage})

    return pd.DataFrame(results)


# ============================================================================
# Query-level evaluation: three modes that route a test query through the
# tree's discovered neighborhoods and check whether the target doc is reachable.
# Each public evaluator partitions user_docs_test across worker processes.
# ============================================================================


def _query_as_node_chunk(
    chunk_df,
    path_to_tree_pickle,
    user_train_docs_dict,
    M,
    N,
    log_filename="querynode_progress.log"
):
    """Worker: route each query through the tree (loaded from pickle), check whether target doc is in the union of closest users' docs."""
    pid = os.getpid()
    chunk_size = len(chunk_df)
    log_every = 100
    results = []

    with open(path_to_tree_pickle, "rb") as f:
        tree = dill.load(f)

    for i, row in enumerate(chunk_df.itertuples(index=False)):
        sender_id = row.AnonID
        query_emb = row.query_embedding
        target_doc_index = row.DocIndex

        closest_users = tree.get_closest_users_for_query(query_emb, M=M, N=N)

        documents_from_these_users = set()
        for anon_id in closest_users:
            documents_from_these_users |= user_train_docs_dict.get(anon_id, set())

        found_flag = (target_doc_index in documents_from_these_users)
        results.append({
            "AnonID": sender_id,
            "DocIndex": target_doc_index,
            "QueryFound": found_flag
        })

        if i % log_every == 0:
            with open(log_filename, "a") as lf:
                lf.write(f"[PID={pid}] Processed {i}/{chunk_size} queries in this chunk.\n")

    with open(log_filename, "a") as lf:
        lf.write(f"[PID={pid}] Completed all {chunk_size} queries in this chunk.\n")

    return results


def evaluate_query_as_node(
    path_to_tree_pickle,
    user_train_docs,
    user_docs_test,
    M,
    N,
    n_cores=max(1, cpu_count() - 1),
    log_filename="querynode_progress.log"
):
    """Route each test query through the tree (loaded per-worker from disk) and return found-rate accuracy."""
    user_train_docs_dict = user_train_docs.groupby('AnonID')['DocIndex'].apply(set).to_dict()

    chunks = np.array_split(user_docs_test, n_cores)

    with Pool(n_cores) as pool:
        results_lists = pool.starmap(
            _query_as_node_chunk,
            [
                (chunk, path_to_tree_pickle, user_train_docs_dict, M, N, log_filename)
                for chunk in chunks
            ]
        )

    all_results = []
    for res_list in results_lists:
        all_results.extend(res_list)

    results_df = pd.DataFrame(all_results)
    return results_df["QueryFound"].mean()


def _closest_users_chunk(
    chunk_df,
    anonid_to_embedding,
    user_docs_dict,
    tree_anon_closest_users_df,
    top_k=50
):
    """Worker: rank each query's accessible users by cosine similarity, take top-k, check for target doc."""
    pid = os.getpid()
    chunk_size = len(chunk_df)
    log_every = 100
    results = []

    for i, row in enumerate(chunk_df.itertuples(index=False)):
        sender_id = row.AnonID
        query_emb = np.asarray(row.query_embedding).reshape(1, -1)
        target_doc = row.DocIndex

        if sender_id not in tree_anon_closest_users_df.index:
            results.append({"AnonID": sender_id, "DocIndex": target_doc, "found": False})
            continue

        accessible_users = tree_anon_closest_users_df.loc[sender_id, "All_Shared"]
        valid_users = [u for u in accessible_users if u in anonid_to_embedding]
        if not valid_users:
            results.append({"AnonID": sender_id, "DocIndex": target_doc, "found": False})
            continue

        candidate_embs = np.stack([anonid_to_embedding[u] for u in valid_users])
        sims = cosine_similarity(query_emb, candidate_embs)[0]

        if len(sims) > top_k:
            top_idx = np.argpartition(-sims, top_k - 1)[:top_k]
        else:
            top_idx = np.arange(len(sims))
        top_user_ids = [valid_users[idx] for idx in top_idx]

        found_flag = any(
            u in user_docs_dict and target_doc in user_docs_dict[u]
            for u in top_user_ids
        )
        results.append({"AnonID": sender_id, "DocIndex": target_doc, "found": found_flag})

        if i % log_every == 0:
            with open("non_chain_progress.log", "a") as f:
                f.write(f"[PID={pid}] Processed {i}/{chunk_size} queries in this chunk.\n")

    with open("non_chain_progress.log", "a") as f:
        f.write(f"[PID={pid}] Completed all {chunk_size} queries in this chunk.\n")
    return results


def evaluate_closest_users(
    df_avgemb,
    user_train_docs,
    user_docs_test,
    tree_anon_closest_users_df,
    top_k=50,
    n_cores=max(1, cpu_count() - 1)
):
    """For each test query, rank its sender's tree-discovered contacts by cosine similarity, take top_k, check for target doc."""
    anonid_to_embedding = df_avgemb.set_index('AnonID')['embedding'].to_dict()
    user_docs_dict = user_train_docs.groupby('AnonID')['DocIndex'].apply(set).to_dict()

    chunks = np.array_split(user_docs_test, n_cores)

    with Pool(n_cores) as pool:
        results_lists = pool.starmap(
            _closest_users_chunk,
            [
                (chunk, anonid_to_embedding, user_docs_dict, tree_anon_closest_users_df, top_k)
                for chunk in chunks
            ]
        )

    all_results = []
    for res_list in results_lists:
        all_results.extend(res_list)

    results_df = pd.DataFrame(all_results)
    return results_df["found"].mean()


def _closest_user_chain_chunk(
    chunk_df,
    anonid_to_embedding,
    user_docs_dict,
    tree_anon_closest_users_df,
    max_hops=50
):
    """Worker: chain-route each query, picking the highest-similarity unvisited contact at each hop until the target doc is found or hops exhausted."""
    pid = os.getpid()
    chunk_size = len(chunk_df)
    log_every = 100
    results = []

    for i, row in enumerate(chunk_df.itertuples(index=False)):
        sender_id = row.AnonID
        query_emb = np.asarray(row.query_embedding).reshape(1, -1)
        target_doc = row.DocIndex

        found_flag = False
        hops = 0
        visited = {sender_id}
        current_user = sender_id

        while hops < max_hops:
            hops += 1

            if current_user not in tree_anon_closest_users_df.index:
                break
            contacts = tree_anon_closest_users_df.loc[current_user, "All_Shared"]
            if not contacts:
                break

            valid_contacts = [c for c in contacts if c in anonid_to_embedding and c not in visited]
            if not valid_contacts:
                break

            candidate_embs = np.stack([anonid_to_embedding[c] for c in valid_contacts])
            sims = cosine_similarity(query_emb, candidate_embs)[0]
            best_contact = valid_contacts[int(np.argmax(sims))]

            if best_contact in user_docs_dict and target_doc in user_docs_dict[best_contact]:
                found_flag = True
                break
            visited.add(best_contact)
            current_user = best_contact

        results.append({
            "AnonID": sender_id,
            "DocIndex": target_doc,
            "found": found_flag
        })

        if i % log_every == 0:
            with open("chain_progress.log", "a") as f:
                f.write(f"[PID={pid}] Processed {i+1}/{chunk_size} queries in this chunk.\n")

    with open("chain_progress.log", "a") as f:
        f.write(f"[PID={pid}] Completed all {chunk_size} queries in this chunk.\n")

    return results


def evaluate_closest_user_chain(
    df_avgemb,
    user_train_docs,
    user_docs_test,
    tree_anon_closest_users_df,
    top_k=50
):
    """Chain-route each test query through tree-discovered contacts hop-by-hop until the target doc is found or max_hops reached."""
    anonid_to_embedding = df_avgemb.set_index('AnonID')['embedding'].to_dict()
    user_docs_dict = user_train_docs.groupby('AnonID')['DocIndex'].apply(set).to_dict()

    n_cores = max(1, cpu_count() - 1)
    chunks = np.array_split(user_docs_test, n_cores)

    with Pool(n_cores) as pool:
        results_lists = pool.starmap(
            _closest_user_chain_chunk,
            [
                (chunk, anonid_to_embedding, user_docs_dict, tree_anon_closest_users_df, 50)
                for chunk in chunks
            ]
        )

    all_results = [res for sublist in results_lists for res in sublist]
    results_df = pd.DataFrame(all_results)
    return results_df["found"].mean()
