from typing import List

import numpy as np
from domino.testing import piece_dry_run
from tensorflow import keras
from tensorflow.keras.layers import Conv1D


def run_piece(
    train_data_path: str,
    num_layers: int,
    filters_per_layer: List[int],
    kernel_sizes: List[int],
    batch_size: int,
    epochs: int,
):
    return piece_dry_run(
        piece_name="TimeSeriesClassificationTrainPiece",
        input_data={
            'train_data_path': train_data_path,
            'num_layers': num_layers,
            'filters_per_layer': filters_per_layer,
            'kernel_sizes': kernel_sizes,
            'batch_size': batch_size,
            'epochs': epochs
        }
    )


def test_TimeSeriesClassificationTrainPiece():
    piece_kwargs = {
        'train_data_path': 'https://raw.githubusercontent.com/hfawaz/cd-diagram/master/FordA/FordA_TRAIN.tsv',
        'num_layers': 3,
        'filters_per_layer': [64] * 3,
        'kernel_sizes': [3] * 3,
        'batch_size': 32,
        'epochs': 5
    }
    output = run_piece(
        **piece_kwargs
    )
    m = keras.models.load_model(output['best_model_file_path'])
    conv1D_layers = [layer for layer in m.layers if isinstance(layer, Conv1D)]
    assert len(conv1D_layers) == piece_kwargs['num_layers']
    for layer, filters, kernel_size in zip(
        conv1D_layers,
        piece_kwargs['filters_per_layer'],
        piece_kwargs['kernel_sizes']
    ):
        assert layer.filters == filters
        assert layer.kernel_size == np.reshape(kernel_size, shape=(1))
