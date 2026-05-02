import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def scatterplot_commoncounts(user_to_topk, round_idx, DELTA, user_to_all_shared,
                             user_ids, absolute_closest_users_df):
    x = []
    list_contacts = []
    for u in user_ids:
        x.append(len(set(absolute_closest_users_df.loc[u].values).intersection(
            set([i[0] for i in user_to_topk[u]]))))
        list_contacts.append(len(user_to_all_shared[u]))
    plt.scatter(range(len(x)), x, s=0.1)
    plt.suptitle(f'Number of absolute 50 closest users caught in "known-users" list')
    plt.title(f'Expansion round {round_idx}, delta {DELTA}')
    plt.ylabel('Number of neighbors in absolute closest 50 list')
    plt.xlabel('User ID')
    plt.ylim(-2, 52)
    plt.show()
    print(f'Mean, std and median common counts for round {round_idx}: {np.mean(x)}, {np.std(x)}, {np.median(x)} '
          f'and mean, std and median number of connected-users: {np.mean(list_contacts)}, {np.std(list_contacts)}, {np.median(list_contacts)}')


def plot_construct_tree_histograms(DELTA, N, leaf_cosine_sims, total_nbr_connected,
                                   mean_similarities, nbr_users_per_leaf, common_counts_df,
                                   results_df_abs, results_df_tree, tree_clone_closest_users_df):
    print(f'Number of leaf nodes: {len(leaf_cosine_sims)}')
    plt.hist(total_nbr_connected)
    plt.title(f"number of other-AnonID-embeddings known by AnonID's")
    plt.xlabel(f"number of other-AnonID-embeddings known")
    plt.ylabel(f"number of AnonID's")
    plt.show()

    plt.hist(mean_similarities.values(), bins=100)
    plt.title(f'mean similarity of closest {N} users in the tree')
    plt.xlim(0.8, 1.1)
    plt.show()

    plt.hist(leaf_cosine_sims, bins=int(len(leaf_cosine_sims) / 50.0), density=True)
    plt.suptitle(f'Mean similarity between nodes in same leaf')
    plt.title(f'Delta = {DELTA}')
    plt.xlim(0.85, 1.05)
    plt.show()

    pd.Series(nbr_users_per_leaf).to_csv(os.path.join('csvs_and_graphs', f'nbr_users_per_leaf_delta{DELTA}.csv'))
    plt.hist(nbr_users_per_leaf, bins=100)
    plt.suptitle(f'Number of users per leaf node')
    plt.title(f'Delta = {DELTA}')
    plt.show()

    common_counts_df['Common_Count'].hist(bins=60)
    plt.suptitle(f'Absolutely Closest {N} Users for Each User in Tree')
    plt.title(f'Delta = {DELTA}')
    plt.xlabel(f'Number of Absolutely Closest {N}')
    plt.ylabel(f'Number of Users')
    plt.show()

    results_df_abs['OverlapPercentage'].hist(bins=20)
    plt.suptitle(f'% docs users looked for, held by abs-closest {N} other users')
    plt.title(f'Users hold min 20 docs, are looking for next 10 docs')
    plt.xlabel('Percentage')
    plt.xlim(-5, 105)
    plt.ylabel('Number of users')
    plt.show()

    results_df_tree['OverlapPercentage'].hist(bins=20)
    plt.suptitle(f'% docs users looked for, held by tree-closest {N} other users')
    plt.title(f'Users hold min 20 docs, are looking for next 10 docs')
    plt.xlabel('Percentage')
    plt.xlim(-5, 105)
    plt.ylabel('Number of users')
    plt.show()

    clones_per_user = tree_clone_closest_users_df.reset_index().groupby('AnonID')['NodeID'].count()
    clones_per_user.to_csv(os.path.join('csvs_and_graphs', f'clones_per_user_delta{DELTA}.csv'))
    plt.hist(clones_per_user, bins=clones_per_user.max() * 2)
    plt.suptitle("Number of clones per user")
    plt.title(f"Delta: {DELTA}")
    plt.xlabel('Number of clones')
    plt.ylabel('Number of users')
    plt.show()
