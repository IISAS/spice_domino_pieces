import logging
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from domino.base_piece import BasePiece
from tensorflow import keras
from tensorflow.keras.layers import BatchNormalization, Conv1D, GlobalAveragePooling1D, Dense, Input, ReLU
from tensorflow.keras.models import Sequential

from .models import InputModel, OutputModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set default level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # Log format
    datefmt="%Y-%m-%d %H:%M:%S",  # Timestamp format
    handlers=[
        logging.FileHandler("app.log"),  # Log to file
        logging.StreamHandler()  # Also log to console
    ]
)

logger = logging.getLogger(__name__)


class TimeSeriesClassificationTrainPiece(BasePiece):
    """
    based on https://keras.io/examples/timeseries/timeseries_classification_from_scratch/
    """

    def _readucr(self, filename):
        data = np.loadtxt(filename, delimiter="\t")
        x = data[:, 1:]
        y = data[:, 0]
        return x, y.astype(int)

    def _build_model(
        self,
        input_shape: tuple,
        num_classes,
        num_layers=3,
        filters_per_layer=[64] * 3,
        kernel_sizes=[3] * 3,
    ):
        """
        Builds a generic, parameterized 1D CNN model.

        Args:
            input_shape (tuple): Shape of input data (timesteps, features).
            num_layers (int): Number of convolutional layers.
            num_classes (int): Number of output classes.
            filters_per_layer (list): Number of filters for each conv layer.
            kernel_sizes (list): Kernel size for each conv layer.

        Returns:
            keras.Model: Compiled Keras 1D CNN model.
        """

        assert len(filters_per_layer) == num_layers, "filters_per_layer length must match num_layers"
        assert len(kernel_sizes) == num_layers, "kernel_sizes length must match num_layers"

        model = Sequential(name="Generic1DCNN")
        model.add(Input(shape=input_shape))

        # Convolutional layers
        for i in range(num_layers):
            model.add(Conv1D(
                filters=filters_per_layer[i],
                kernel_size=kernel_sizes[i],
                padding='same',
                name=f'conv_{i + 1}'
            ))
            model.add(BatchNormalization(name=f'bn_{i + 1}'))
            model.add(ReLU(name=f'relu_{i + 1}'))

        model.add(GlobalAveragePooling1D())

        # Dense head
        model.add(Dense(num_classes, activation='softmax', name='dense_1'))

        # Compile
        model.compile(
            optimizer='adam',
            loss='sparse_categorical_crossentropy',
            metrics=['sparse_categorical_accuracy'],
        )

        return model

    def piece_function(self, input_data: InputModel):
        logger.debug('piece function')

        # load data
        x_train, y_train = self._readucr(input_data.train_data_path)

        logger.info(f'x_train.shape: {x_train.shape}')
        logger.info(f'y_train.shape: {y_train.shape}')

        # reshape to multivariate
        x_train = x_train.reshape((x_train.shape[0], x_train.shape[1], 1))

        # infer the number of classes
        num_classes = len(np.unique(y_train))

        # shuffle
        idx = np.random.permutation(len(x_train))
        x_train = x_train[idx]
        y_train = y_train[idx]

        # standardize
        y_train[y_train == -1] = 0

        m = self._build_model(
            input_shape=x_train.shape[1:],
            num_layers=input_data.num_layers,
            num_classes=num_classes,
            filters_per_layer=input_data.filters_per_layer,
            kernel_sizes=input_data.kernel_sizes,
        )

        best_model_file_path = os.path.join(Path(self.results_path), 'best_model.keras')
        callbacks = [
            keras.callbacks.ModelCheckpoint(
                best_model_file_path, save_best_only=True, monitor="val_loss"
            ),
            keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss", factor=0.5, patience=20, min_lr=0.0001
            ),
            keras.callbacks.EarlyStopping(monitor="val_loss", patience=50, verbose=1),
        ]

        history = m.fit(
            x_train,
            y_train,
            batch_size=input_data.batch_size,
            epochs=input_data.epochs,
            callbacks=callbacks,
            validation_split=0.2,
            verbose=1,
        )

        last_model_file_path = os.path.join(Path(self.results_path), 'last_model.keras')
        m.save(last_model_file_path)

        metric = "sparse_categorical_accuracy"
        plt.figure()
        plt.plot(history.history[metric])
        plt.plot(history.history["val_" + metric])
        plt.title("Model " + metric)
        plt.ylabel(metric, fontsize="large")
        plt.xlabel("epoch", fontsize="large")
        plt.legend(["train", "val"], loc="best")

        fig_path = os.path.join(Path(self.results_path), f"training_{metric}.png")
        plt.savefig(fig_path, dpi=300, bbox_inches="tight")
        plt.close()

        # Set display result
        self.display_result = {
            'file_type': 'image/png',
            'file_path': fig_path
        }

        # Return output
        return OutputModel(
            best_model_file_path=best_model_file_path,
            last_model_file_path=last_model_file_path,
        )
