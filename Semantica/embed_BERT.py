import argparse
import os

from embedding.encoder import encode_csv_to_pickle


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'target',
        nargs='?',
        default='both',
        choices=['docs', 'queries', 'both'],
    )
    args = parser.parse_args()

    if args.target in ('docs', 'both'):
        encode_csv_to_pickle(
            input_csv=os.path.join('dataset', 'doc.csv'),
            text_col='Title',
            output_pkl='embedded_docs_BERT_df.pkl',
            log_every=100,
        )
    if args.target in ('queries', 'both'):
        encode_csv_to_pickle(
            input_csv=os.path.join('dataset', 'query.csv'),
            text_col='Query',
            output_pkl='embedded_queries_df_BERT.pkl',
            log_every=1000,
        )
