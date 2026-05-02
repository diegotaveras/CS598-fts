import os
import warnings
from itertools import product

import matplotlib
matplotlib.use('Agg')

import pandas as pd

from embedding import construct_anon_embedding
from baselines.random_baseline import calculate_baseline
from tree import construct_tree
from plots import plot_data_overview

warnings.filterwarnings("ignore")

# Data-construction grid: each combination triggers one anon-embedding build.
nbr_train_recs_list = [20]
nbr_test_records_list = [10]
small_user_filter_for_test_set = [True]
perform_query_options = [None]  # OPTIONS: "Query-as-Node", "Closest-Users", "Closest-User-Chain"

# Algorithm grid: each combination runs once per data combo above.
record_limit_per_leafnode_list = [50]
N_list = [50]
M_multipliers = [1]
DELTA_list = [5e-4]
expansion_rounds_list = [0]
consider_contacts_options = [False]

plot_results = True


def run_experiment(
    *,
    df_avgemb, user_docs_train, user_docs_test, cosine_sim_df,
    perform_query, small_user_filter, nbr_train_recs, nbr_test_records,
    record_limit_per_leafnode, N, M_multiplier, DELTA, expansion_rounds,
    consider_contacts, exp_index,
):
    # N is capped at the actual number of users so we never request more neighbors than exist.
    N_filtered = min(N, df_avgemb['AnonID'].nunique())
    M = N_filtered * M_multiplier

    print()
    print(
        f'Running algorithm with: '
        f'EXPERIMENT NUMBER: {exp_index}, '
        f'expansion_rounds: {expansion_rounds}, '
        f'DELTA: {DELTA}, '
        f'N: {N_filtered}, '
        f'M: {M}, '
        f'small_user_filter: {small_user_filter}, '
        f'random_baseline_useContacts: {consider_contacts}, '
        f'record_limit_per_leafnode: {record_limit_per_leafnode}, '
        f'perform_query_analysis: {perform_query}, '
        f'nbr_train_recs: {nbr_train_recs}, '
        f'nbr_test_recs: {nbr_test_records}'
    )

    (cmn_counts_mean, cmn_counts_median, nbr_users_contacted, tree_overlap,
     absolute_overlap, tree_anon_closest_users_df, querylevel_accuracy) = construct_tree(
        record_limit_per_leafnode=record_limit_per_leafnode,
        N=N_filtered,
        DELTA=DELTA,
        df_avgemb=df_avgemb,
        user_docs_train=user_docs_train,
        user_docs_test=user_docs_test,
        cosine_sims=cosine_sim_df,
        M=M,
        perform_query_test=perform_query,
        EXPANSION_ROUNDS=expansion_rounds,
        print_hists=plot_results,
    )

    mean_random, std_random = calculate_baseline(
        number_experiments=1,
        tree_anon_closest_users_df=tree_anon_closest_users_df,
        user_docs_train=user_docs_train,
        user_docs_test=user_docs_test,
        df_avgemb=df_avgemb,
        consider_contacts=consider_contacts,
    )

    return {
        'nbr_train_recs': nbr_train_recs,
        'nbr_test_records': nbr_test_records,
        'DELTA': DELTA,
        'N': N_filtered,
        'M': M,
        'small_user_filter': small_user_filter,
        'baseline_consider_contacts': consider_contacts,
        'reclim_per_leafnode': record_limit_per_leafnode,
        'train_recs': nbr_train_recs,
        'test_recs': nbr_test_records,
        'cmn_counts_mean': cmn_counts_mean,
        'cmn_counts_median': cmn_counts_median,
        'nbr_users_contacted': nbr_users_contacted,
        'tree_overlap': tree_overlap,
        'absolute_overlap': absolute_overlap,
        'expansion_rounds': expansion_rounds,
        'querylevel_accuracy': querylevel_accuracy,
        'mean_random_baseline': mean_random,
        'std_random_baseline': std_random,
    }


def main():
    data_grid = list(product(
        perform_query_options,
        small_user_filter_for_test_set,
        nbr_train_recs_list,
        nbr_test_records_list,
    ))
    algo_grid = list(product(
        expansion_rounds_list,
        record_limit_per_leafnode_list,
        DELTA_list,
        N_list,
        M_multipliers,
        consider_contacts_options,
    ))
    total_experiments = len(data_grid) * len(algo_grid)
    print(f"STARTING {total_experiments} EXPERIMENTS")

    results = []
    exp_index = 0
    for perform_query, small_user_filter, nbr_train_recs, nbr_test_records in data_grid:
        df_avgemb, user_docs_train, user_docs_test, cosine_sim_df = construct_anon_embedding(
            nbr_train_recs=nbr_train_recs,
            nbr_test_records=nbr_test_records,
            sample_rdn_test=True,
            filter_out_users_with_few_docs=small_user_filter,
        )
        # Shuffle each artifact with a fixed seed so the order is reproducible across runs.
        df_avgemb = df_avgemb.sample(frac=1, random_state=613626)
        user_docs_train = user_docs_train.sample(frac=1, random_state=523414)
        user_docs_test = user_docs_test.sample(frac=1, random_state=876996)
        cosine_sim_df = cosine_sim_df.sample(frac=1, random_state=857494)

        if plot_results:
            plot_data_overview(cosine_sim_df, user_docs_train)

        for (expansion_rounds, record_limit_per_leafnode, DELTA, N,
             M_multiplier, consider_contacts) in algo_grid:
            exp_index += 1
            result = run_experiment(
                df_avgemb=df_avgemb,
                user_docs_train=user_docs_train,
                user_docs_test=user_docs_test,
                cosine_sim_df=cosine_sim_df,
                perform_query=perform_query,
                small_user_filter=small_user_filter,
                nbr_train_recs=nbr_train_recs,
                nbr_test_records=nbr_test_records,
                record_limit_per_leafnode=record_limit_per_leafnode,
                N=N,
                M_multiplier=M_multiplier,
                DELTA=DELTA,
                expansion_rounds=expansion_rounds,
                consider_contacts=consider_contacts,
                exp_index=exp_index,
            )
            print(result)
            results.append(result)

    print(results)
    # pd.DataFrame(records) is the right constructor for a list of dicts;
    # DataFrame.from_dict expects a single mapping.
    pd.DataFrame(results).to_pickle(os.path.join('results', 'all_results_df.pkl'))


if __name__ == "__main__":
    main()
