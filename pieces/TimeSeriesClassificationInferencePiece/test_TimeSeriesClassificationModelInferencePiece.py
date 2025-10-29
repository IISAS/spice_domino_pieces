import os
import tempfile

import numpy as np
from domino.testing import piece_dry_run
from domino.testing.utils import skip_envs


def run_piece(
    **kwargs
):
    return piece_dry_run(
        piece_name="TimeSeriesClassificationInferencePiece",
        input_data=kwargs
    )


@skip_envs('github')
def test_TimeSeriesClassificationInferencePiece():

    # load the data, see https://keras.io/examples/timeseries/timeseries_classification_from_scratch/
    data = np.loadtxt(
        'https://raw.githubusercontent.com/hfawaz/cd-diagram/master/FordA/FordA_TEST.tsv',
        delimiter='\t'
    )
    x_test = data[:, 1:]
    y_test = data[:, 0].astype(int)

    # reshape
    y_test = y_test.reshape((y_test.shape[0], 1))

    # standardize the data
    y_test[y_test == -1] = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # save preprocessed data
        test_data_path = os.path.join(tmpdir, 'test.tsv')
        np.savetxt(
            test_data_path,
            np.concatenate([x_test, y_test], axis=1),
            delimiter="\t",
            newline="\n"
        )

        piece_kwargs = {
            'input_data_path': test_data_path,
            'model_file_path': 'dry_run_results/best_model.keras',
            'target_column_idx': -1,
            'skiprows': 0,
        }
        output = run_piece(
            **piece_kwargs
        )