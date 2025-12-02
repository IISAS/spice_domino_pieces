import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from domino.base_piece import BasePiece
from tensorflow import keras
from tensorflow.keras.layers import BatchNormalization, Conv1D, GlobalAveragePooling1D, Dense, Input, ReLU
from tensorflow.keras.models import Sequential

from .models import InputModel, OutputModel


# TODO: support for multivariate time series
class TimeSeriesClassificationTrainPiece(BasePiece):

    def _shuffle_data(self, x, y):
        self.logger.info("Shuffling data...")
        idx = np.random.permutation(len(x))
        self.logger.info("Shuffling data done.")
        return x[idx], y[idx]

    def _load_tsv_data(self, filename, target_column_idx: int = -1, skiprows: int = 0):
        self.logger.info("Loading TSV data: %s", filename)
        data = np.loadtxt(filename, delimiter="\t", skiprows=skiprows)
        Y = data[:, target_column_idx]
        self.logger.info("Shape Y: %s", Y.shape)
        X = np.delete(data, target_column_idx, axis=1)
        self.logger.info("Shape X: %s", X.shape)
        input_shape = X.shape[1:]
        # reshape
        if len(input_shape) > 3:
            msg = f"Input shape {input_shape} is not supported."
            self.logger.error(msg)
            raise Exception(msg)
        elif len(input_shape) == 2:
            X = np.reshape(X, (-1, input_shape[0], input_shape[1]))
            self.logger.info('X reshaped to: %s', X.shape)
        elif len(input_shape) == 1:
            X = np.reshape(X, (-1, input_shape[0], 1))
            self.logger.info('X reshaped to: %s', X.shape)
        else:
            msg = f"Invalid input shape {input_shape}."
            self.logger.error(msg)
            raise Exception(msg)
        self.logger.info('TSV data sucessfully loaded.')
        return X, Y

    def _build_model(
        self,
        input_shape: tuple,
        num_classes,
        num_layers=3,
        filters_per_layer=[64] * 3,
        kernel_sizes=[3] * 3,
        loss_function=keras.losses.SparseCategoricalCrossentropy()
    ):
        """
        Builds a generic, parameterized 1D CNN model.

        Args:
            input_shape (tuple): Shape of input data (timesteps, features).
            num_layers (int): Number of convolutional layers.
            num_classes (int): Number of output classes.
            filters_per_layer (list): Number of filters for each conv layer.
            kernel_sizes (list): Kernel size for each conv layer.
            loss_function (keras.losses.Loss): Loss function.

        Returns:
            keras.Model: Compiled Keras 1D CNN model.
        """

        self.logger.info("Building CNN model...")

        if len(filters_per_layer) != num_layers:
            msg = "filters_per_layer length must match num_layers"
            self.logger.error(msg)
            raise Exception(msg)

        if len(kernel_sizes) != num_layers:
            msg = "kernel_sizes length must match num_layers"
            self.logger.error(msg)
            raise Exception(msg)

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
            optimizer=keras.optimizers.Adam(),
            loss=loss_function,
            metrics=[
                keras.metrics.SparseCategoricalAccuracy()
            ],
        )

        self.logger.info('Model compiled.')

        return model

    def piece_function(self, input_data: InputModel):

        # load training data
        x_train, y_train = self._load_tsv_data(
            input_data.train_data_path,
            target_column_idx=input_data.target_column_idx,
            skiprows=input_data.skiprows_train
        )

        # load validation data
        x_val = y_val = None
        if input_data.validation_data_path is not None and not input_data.validation_data_path.isspace():
            x_val, y_val = self._load_tsv_data(
                input_data.validation_data_path,
                target_column_idx=input_data.target_column_idx,
                skiprows=input_data.skiprows_val
            )

        if x_val is not None and y_val is not None:
            # number of variables in val must be equal to that of train
            if x_val.shape[1:] != x_train.shape[1:]:
                msg = 'train and validation datasets must have the same shape of samples'
                self.logger.error(msg)
                raise Exception(msg)

        # resolve unique labels
        if x_val is not None and y_val is not None:
            # train + val
            unique_labels = np.unique(np.concatenate([y_train, y_val]))
        else:
            # train
            unique_labels = np.unique(y_train)
        self.logger.info("Unique labels: %s", unique_labels)

        # infer the number of labels
        num_unique_labels = len(unique_labels)
        self.logger.info('Number of unique labels: %s', num_unique_labels)

        loss_function = keras.losses.SparseCategoricalCrossentropy()

        m = self._build_model(
            input_shape=x_train.shape[1:],
            num_layers=input_data.num_layers,
            num_classes=num_unique_labels,
            filters_per_layer=input_data.filters_per_layer,
            kernel_sizes=input_data.kernel_sizes,
            loss_function=loss_function
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

        # shuffle train data before splitting to train and val
        if x_val is None and y_val is None:
            x_train, y_train = self._shuffle_data(x_train, y_train)

        self.logger.info('Training model...')
        history = m.fit(
            x_train,
            y_train,
            batch_size=input_data.batch_size,
            epochs=input_data.epochs,
            callbacks=callbacks,
            verbose=1,
            shuffle=input_data.shuffle,
            **(
                {
                    'validation_data': (x_val, y_val),
                } if x_val is not None and y_val is not None else {
                    'validation_split': input_data.validation_split,
                }
            )
        )

        self.logger.info('Saving model...')
        last_model_file_path = os.path.join(
            Path(self.results_path),
            OutputModel.model_fields['last_model_file_path'].default
        )
        m.save(last_model_file_path)
        self.logger.info('Model saved.')

        self.logger.info('Producing training report...')
        metrics = ['sparse_categorical_accuracy', 'loss']

        # Create a single figure with one subplot per metric
        fig, axes = plt.subplots(len(metrics), 1, figsize=(8, 6))  # stacked vertically

        for i, metric in enumerate(metrics):
            metric_label = f'loss ({loss_function.name})' if metric == 'loss' else metric
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
            'file_type': 'image',
            'file_path': fig_path
        }

        self.logger.info('Training report created.')

        # Return output
        return OutputModel(
            best_model_file_path=best_model_file_path,
            last_model_file_path=last_model_file_path,
        )
