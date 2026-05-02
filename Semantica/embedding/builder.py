import os
import random
import numpy as np
import pandas as pd
import torch
from sklearn.metrics.pairwise import cosine_similarity

from .loader import load_user_doc_data, load_doc_embeddings


SEED = 42


def sample_and_select(group, nbr_train_recs, nbr_test_records, sample_rdn_test):
    if len(group) <= nbr_train_recs + nbr_test_records:
        return None
    if sample_rdn_test:
        sample_idx = random.sample(group.index.tolist(), nbr_test_records)
        return group.loc[sample_idx]
    max_start = len(group) - nbr_train_recs - nbr_test_records
    start_idx = np.random.randint(0, max_start + 1)
    return group.iloc[start_idx: start_idx + nbr_train_recs + nbr_test_records]


def construct_anon_embedding(nbr_train_recs, nbr_test_records=10, sample_rdn_test=True,
                             filter_out_users_with_few_docs=False):
    # Determinism: seed every RNG that sampling or torch-side embedding code can touch.
    random.seed(SEED)
    np.random.seed(SEED)
    torch.manual_seed(SEED)

    data = load_user_doc_data()
    docs_subset = load_doc_embeddings()

    # Split each user's history into train/test. Random branch picks test rows uniformly;
    # sequential branch keeps a contiguous window so train precedes test in time.
    if sample_rdn_test:
        test_data = data.groupby('AnonID', group_keys=False).apply(
            lambda group: sample_and_select(group, nbr_train_recs=nbr_train_recs,
                                            nbr_test_records=nbr_test_records,
                                            sample_rdn_test=sample_rdn_test)).dropna()
        test_data = test_data.reset_index(drop=False)

        if filter_out_users_with_few_docs:
            data = data[data['AnonID'].isin(test_data['AnonID'].unique())]
        data = data.drop(test_data['index'])
    else:
        data = data.groupby('AnonID', group_keys=False).apply(
            lambda group: sample_and_select(group, nbr_train_recs=nbr_train_recs,
                                            nbr_test_records=nbr_test_records,
                                            sample_rdn_test=sample_rdn_test)).dropna()
        test_data = data.groupby('AnonID').apply(
            lambda x: x.iloc[nbr_train_recs:nbr_train_recs + nbr_test_records]).reset_index(drop=True)
        data = data.groupby('AnonID').head(nbr_train_recs)

    nbr_users = data['AnonID'].nunique()
    print(f'Number of users for {nbr_train_recs} train records and {nbr_test_records} test records: {nbr_users}')

    # User vector = mean of the doc embeddings for that user's train rows.
    df = pd.merge(data, docs_subset[['DocIndex', 'embedding']], on='DocIndex', how='left')
    df_avgemb = df.groupby('AnonID')['embedding'].mean().reset_index()

    # Persisted artifacts — main.py and the baselines load these pickles directly.
    df_avgemb.to_pickle(os.path.join('results', 'average_embedding_small.pkl'))
    data.to_pickle(os.path.join('results', 'traindata.pkl'))
    test_data.to_pickle(os.path.join('results', 'testdata.pkl'))

    # Pairwise user-user cosine similarity. Diagonal is masked so a user is never their own neighbor.
    user_ids = df_avgemb['AnonID'].tolist()
    embeddings = np.array(df_avgemb['embedding'].tolist())

    cosine_sim_matrix = cosine_similarity(embeddings)
    np.fill_diagonal(cosine_sim_matrix, np.nan)

    cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=user_ids, columns=user_ids)
    cosine_sim_df.to_pickle(os.path.join('results', 'cosine_sims.pkl'))

    # Per-user nearest-neighbor similarity — a quick signal for how distinguishable users are.
    closest_user_cosine = []
    for i, row in enumerate(cosine_sim_matrix):
        row[i] = -1
        max_sim = np.max(row)
        closest_user_cosine.append(max_sim)

    df_avgemb['closest_user_cosine'] = closest_user_cosine

    print('Train data per user: ', data.groupby('AnonID')['DocIndex'].count().iloc[0])
    print('Test data per user: ', test_data.groupby('AnonID')['DocIndex'].count().iloc[0])
    print('Finished constructing user embeddings')
    # Ceiling: fraction of test rows whose DocIndex appears in any user's train set — anything above this is impossible.
    max_acc_attainable = test_data[test_data['DocIndex'].isin(data['DocIndex'].unique())].shape[0] / test_data.shape[0]
    print(f'Maximum accuracy attainable with current graph: {max_acc_attainable}')
    print()
    return df_avgemb, data, test_data, cosine_sim_df
