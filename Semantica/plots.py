from collections import defaultdict
from itertools import combinations

import matplotlib.pyplot as plt
import pandas as pd


def _count_common_docindexes(df):
    pair_counts = defaultdict(int)
    grouped = df.groupby('DocIndex')['AnonID']
    for _, anon_ids in grouped:
        unique_anon_ids = sorted(set(anon_ids))
        for pair in combinations(unique_anon_ids, 2):
            pair_counts[pair] += 1
    return pair_counts


def plot_data_overview(cosine_sim_df, user_docs_train):
    plt.plot(cosine_sim_df.max().sort_values(ascending=False).values)
    plt.title('Ranked users by cosine-similarity with best buddy')
    plt.xlabel('User')
    plt.ylabel('Cosine-similarity')
    plt.show()

    pair_overlap_counts = _count_common_docindexes(user_docs_train)
    overlap_df = pd.DataFrame(
        [(anon1, anon2, count) for (anon1, anon2), count in pair_overlap_counts.items()],
        columns=['AnonID_1', 'AnonID_2', 'Common_DocIndex_Count'],
    )
    overlap_df = overlap_df.groupby('AnonID_1')['Common_DocIndex_Count'].max()
    plt.plot(overlap_df.sort_values(ascending=False).values)
    plt.title('Ranked users by co-occurrence of documents with best buddy')
    plt.xlabel('User')
    plt.ylabel('Co-occurrence')
    plt.show()
