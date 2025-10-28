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
        loss_function_name='sparse_categorical_crossentropy'
    ):
        """
        Builds a generic, parameterized 1D CNN model.

        Args:
            input_shape (tuple): Shape of input data (timesteps, features).
            num_layers (int): Number of convolutional layers.
            num_classes (int): Number of output classes.
            filters_per_layer (list): Number of filters for each conv layer.
            kernel_sizes (list): Kernel size for each conv layer.
            loss_function_name (str): Loss function name.

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
            loss=loss_function_name,
            metrics=['sparse_categorical_accuracy'],
        )

        return model

    def _reshape_to_multivariate(self, x, num_vars=1):
        return x.reshape((x.shape[0], x.shape[1], num_vars))

    def _shuffle_data(self, x, y):
        idx = np.random.permutation(len(x))
        return x[idx], y[idx]

    def _standardize_labels(self, labels):
        unique_labels, standardized = np.unique(labels, return_inverse=True)
        mapping = {label: i + 1 for i, label in enumerate(unique_labels)}
        logger.debug('Standardized labels: %s' % str(standardized))
        logger.debug('Mapping: %s' % str(mapping))
        return standardized, mapping

    def piece_function(self, input_data: InputModel):

        logger.debug('piece function')

        # load training data
        x_train, y_train = self._readucr(input_data.train_data_path)
        logger.info(f'x_train.shape: {x_train.shape}')
        logger.info(f'y_train.shape: {y_train.shape}')

        # load validation data
        x_val = y_val = None
        if input_data.validation_data_path is not None and not input_data.validation_data_path.isspace():
            x_val, y_val = self._readucr(input_data.validation_data_path)
            logger.info(f'x_val.shape: {x_val.shape}')
            logger.info(f'y_val.shape: {y_val.shape}')

        # reshape to multivariate (train)
        if len(x_train.shape) < 3:
            x_train = self._reshape_to_multivariate(x_train, num_vars=1)

        # reshape to multivariate (val)
        if x_val is not None and y_val is not None:
            if len(x_val.shape) < 3:
                x_val = self._reshape_to_multivariate(x_val, num_vars=1)
            # number of variables in val must be equal to that of train
            assert x_val.shape[1:] == x_train.shape[1:]

        # resolve unique labels
        if x_val is not None and y_val is not None:
            # train + val
            unique_labels = np.unique(np.concatenate([y_train, y_val]))
        else:
            # train
            unique_labels = np.unique(y_train)

        # infer the number of labels
        num_unique_labels = len(unique_labels)

        # standardize labels
        label_mapping_file_path = None
        if input_data.standardize_labels:
            y_train, label_mapping = self._standardize_labels(y_train)
            # save label mapping to a file
            label_mapping_file_path = os.path.join(
                Path(self.results_path),
                OutputModel.model_fields['label_mapping_file_path'].default
            )
            np.save(label_mapping_file_path, label_mapping)
            # transform labels in val data to standard form
            if x_val is not None and y_val is not None:
                y_val = np.vectorize(label_mapping.get)(y_val)

        loss_function_name = 'sparse_categorical_crossentropy'

        m = self._build_model(
            input_shape=x_train.shape[1:],
            num_layers=input_data.num_layers,
            num_classes=num_unique_labels,
            filters_per_layer=input_data.filters_per_layer,
            kernel_sizes=input_data.kernel_sizes,
            loss_function_name=loss_function_name
        )

        best_model_file_path = os.path.join(
            Path(self.results_path),
            OutputModel.model_fields['best_model_file_path'].default
        )

        callbacks = [
            keras.callbacks.ModelCheckpoint(
                best_model_file_path, save_best_only=True, monitor="val_loss"
            ),
            keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss", factor=0.5, patience=20, min_lr=0.0001
            ),
            keras.callbacks.EarlyStopping(monitor="val_loss", patience=50, verbose=1),
        ]

        # shuffle train data
        if input_data.shuffle:
            x_train, y_train = self._shuffle_data(x_train, y_train)

        history = m.fit(
            x_train,
            y_train,
            batch_size=input_data.batch_size,
            epochs=input_data.epochs,
            callbacks=callbacks,
            verbose=1,
            shuffle=input_data.shuffle_before_epoch,
            **(
                {
                    'validation_data': (x_val, y_val),
                } if x_val is not None and y_val is not None else {
                    'validation_split': input_data.validation_split,
                }
            )
        )

        last_model_file_path = os.path.join(
            Path(self.results_path),
            OutputModel.model_fields['last_model_file_path'].default
        )
        m.save(last_model_file_path)

        metrics = ['sparse_categorical_accuracy', 'loss']

        # Create a single figure with one subplot per metric
        fig, axes = plt.subplots(len(metrics), 1, figsize=(8, 6))  # stacked vertically

        for i, metric in enumerate(metrics):
            metric_label = f'loss ({loss_function_name})' if metric == 'loss' else metric
            ax = axes[i] if len(metrics) > 1 else axes
            ax.plot(history.history[metric])
            ax.plot(history.history["val_" + metric])
            ax.set_title(f"Model {metric_label}")
            ax.set_ylabel(metric, fontsize="large")
            ax.set_xlabel("epoch", fontsize="large")
            ax.legend(["train", "val"], loc="best")

        plt.tight_layout()

        # Save single image with both graphs
        fig_path = os.path.join(Path(self.results_path), "training_metrics.png")
        plt.savefig(fig_path, dpi=300, bbox_inches="tight")
        plt.close(fig)

        # Set display result
        self.display_result = {
            'file_type': 'image/png',
            'file_path': fig_path
        }

        # Return output
        return OutputModel(
            best_model_file_path=best_model_file_path,
            last_model_file_path=last_model_file_path,
            **(
                {
                    'label_mapping_file_path': label_mapping_file_path
                } if input_data.standardize_labels else {}
            ),
        )
