import logging
import os

import pandas as pd

from baselines.random_baseline import calculate_baseline
from embedding import construct_anon_embedding


def main():
    nbr_train_recs = 20
    nbr_test_records = 10
    small_user_filter = True
    n_max = 500

    experiment_name = (
        f'all_baseline_results_trainRecs{nbr_train_recs}_testRecs{nbr_test_records}'
        f'_smallUserFilter{small_user_filter}'
    )
    logging.basicConfig(
        filename=f'{experiment_name}.log',
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        force=True,
    )

    # construct_anon_embedding writes results/traindata.pkl, results/testdata.pkl,
    # etc., which calculate_baseline loads internally. The return values are unused here.
    construct_anon_embedding(
        nbr_train_recs=nbr_train_recs,
        nbr_test_records=nbr_test_records,
        sample_rdn_test=True,
        filter_out_users_with_few_docs=small_user_filter,
    )

    output_path = os.path.join('results', experiment_name + '.pkl')
    baseline_results = {}
    logging.info('Embeddings calculated, proceeding to estimate baselines.')

    # consider_contacts=False makes df_avgemb / tree_anon_closest_users_df unnecessary;
    # N and M directly override the values calculate_baseline would otherwise compute.
    for N in range(n_max):
        logging.info(f'Estimating baseline with N = {N}')
        mean_random, std_random = calculate_baseline(
            number_experiments=5, consider_contacts=False, N=N, M=N,
        )
        baseline_results[N] = (mean_random, std_random)
        # Checkpoint every iteration so a long run is recoverable.
        pd.DataFrame.from_dict(baseline_results, orient='index').to_pickle(output_path)
        print(f'{N}: {(round(mean_random, 3), round(std_random, 3))}')


if __name__ == '__main__':
    main()
