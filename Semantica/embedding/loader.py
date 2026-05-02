import os
import pandas as pd


def load_user_doc_data():
    data = pd.read_csv(os.path.join('dataset', 'data.csv'), sep='\t')
    doc = pd.read_csv(os.path.join('dataset', 'doc.csv'), sep='\t')
    doc = doc[(~doc['Title'].str.startswith('www'))]
    doc = doc[(~doc['Title'].str.contains(
        '404|403|401 authorization|401 unauthorized|406 blocked|406 error|error 500|error 406|error|access denied|502 bad gateway'))]
    data = data[data['DocIndex'].isin(doc['DocIndex'])]

    split_columns = data['CandiList'].str.split('\t', expand=True)
    split_columns.columns = [f'Candi{i+1}' for i in range(split_columns.shape[1])]

    data = pd.concat([data, split_columns], axis=1)
    data.drop(columns='CandiList', inplace=True)

    # TODO: CHECK WHETHER TAKING OUT QUERYTIME CHANGES PERFORMANCE
    data = data.sort_values('QueryTime', ascending=True).groupby(
        ['AnonID', 'DocIndex'], as_index=False).first()[['AnonID', 'DocIndex', 'QueryIndex']]

    return data


def load_doc_embeddings():
    return pd.read_pickle('embedded_docs_BERT_df.pkl')
