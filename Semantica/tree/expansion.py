import copy
import random

from .plots import scatterplot_commoncounts


random.seed(42)


def _build_initial_topk(u_id, neighbors, cos_sims, max_k=20):
    """
    Quickly extract the top-K neighbors for user u_id from cos_sims,
    restricted to 'neighbors', using a vectorized approach.
    """
    if u_id not in cos_sims.index:
        return []

    valid_neighbors = [n for n in neighbors if n in cos_sims.columns]
    if not valid_neighbors:
        return []

    row_series = cos_sims.loc[u_id, valid_neighbors]
    top_series = row_series.nlargest(max_k)
    return list(top_series.items())


def _add_candidate_if_better_than_kth(u_id, cand_id, topk_list_local, cos_sims, max_k, all_shared):
    if (u_id not in cos_sims.index) or (cand_id not in cos_sims.columns):
        return topk_list_local
    cand_sim = cos_sims.loc[u_id, cand_id]

    if len(topk_list_local) < max_k:
        topk_list_local.append((cand_id, cand_sim))
        topk_list_local.sort(key=lambda x: x[1], reverse=True)
        return topk_list_local

    kth_sim = topk_list_local[max_k - 1][1]
    if cand_sim > kth_sim:
        all_shared.add(cand_id)
        topk_list_local.append((cand_id, cand_sim))
        topk_list_local.sort(key=lambda x: x[1], reverse=True)
        topk_list_local = topk_list_local[:max_k]
    return topk_list_local


def expand_neighbors_via_referral_all_users(
        user_ids,
        tree_anon_closest_users_df,
        cosine_sims,
        absolute_closest_users_df,
        DELTA,
        num_rounds=5,
        K=20
):
    """
    This function interleaves rounds across all users:
        Round 1: user1 asks once, user2 asks once, ...
        Round 2: user1 asks once, user2 asks once, ...
        etc.

    Returns a dict mapping each user -> (final_topk_set, all_shared_set).
    """
    user_to_topk = {}
    user_to_all_shared = {}
    user_to_asked_set = {}
    current_neighbors_dict = {}

    for u in user_ids:
        current_neighbors_dict[u] = tree_anon_closest_users_df[u]
        initial_topk_list = _build_initial_topk(u, current_neighbors_dict[u], cosine_sims, max_k=K)
        user_to_topk[u] = initial_topk_list
        user_to_all_shared[u] = set(current_neighbors_dict[u])
        user_to_asked_set[u] = set()

    for round_idx in range(num_rounds):
        scatterplot_commoncounts(user_to_topk, round_idx=round_idx, DELTA=DELTA,
                                 user_to_all_shared=user_to_all_shared,
                                 user_ids=user_ids,
                                 absolute_closest_users_df=absolute_closest_users_df)
        tree_anon_closest_users_df = copy.deepcopy(user_to_all_shared)

        for u in user_ids:
            topk_list = user_to_topk[u]
            asked_set = user_to_asked_set[u]
            all_shared = user_to_all_shared[u]

            if not topk_list:
                continue

            potential_referrers = [(nid, sim) for (nid, sim) in topk_list if nid not in asked_set]
            if not potential_referrers:
                continue

            referrer_id, _ = random.choice(potential_referrers)
            asked_set.add(referrer_id)

            if referrer_id not in tree_anon_closest_users_df:
                continue

            possible_referrals = [
                c for c in tree_anon_closest_users_df[referrer_id]
                if c != u and c not in all_shared
            ]

            for cand_id in possible_referrals:
                if (cand_id in cosine_sims.index) and (cand_id in cosine_sims.columns):
                    topk_list = _add_candidate_if_better_than_kth(u, cand_id, topk_list,
                                                                  cosine_sims, max_k=K,
                                                                  all_shared=all_shared)

            user_to_topk[u] = topk_list
            user_to_asked_set[u] = asked_set
            user_to_all_shared[u] = all_shared

    results = {}
    for u in user_ids:
        final_topk_set = {n_id for (n_id, _) in user_to_topk[u]}
        results[u] = (final_topk_set, user_to_all_shared[u])
    scatterplot_commoncounts(user_to_topk, round_idx=num_rounds, DELTA=DELTA,
                             user_to_all_shared=user_to_all_shared,
                             user_ids=user_ids,
                             absolute_closest_users_df=absolute_closest_users_df)
    return results
