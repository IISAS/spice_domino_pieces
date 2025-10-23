import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from domino.base_piece import BasePiece
from tensorflow import keras
from tensorflow.keras.layers import BatchNormalization, Conv1D, GlobalAveragePooling1D, Dense, Input
from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam

from .models import InputModel, OutputModel


class AIKeras1DCNNPiece(BasePiece):

    def _readucr(self, filename):
        data = np.loadtxt(filename, delimiter="\t")
        y = data[:, 0]
        x = data[:, 1:]
        return x, y.astype(int)

    def _build_model(
        self,
        input_shape: tuple,
        num_layers=3,
        filters_per_layer=[32, 64, 128],
        kernels_per_layer=[3, 3, 3],
        num_classes=10,
        learning_rate=1e-3,
    ):
        """
        Builds a generic, parameterized 1D CNN model.

        Args:
            input_shape (tuple): Shape of input data (timesteps, features).
            num_layers (int): Number of convolutional layers.
            filters_per_layer (list): Number of filters for each conv layer.
            kernels_per_layer (list): Kernel size for each conv layer.
            num_classes (int): Number of output classes.

        Returns:
            keras.Model: Compiled Keras 1D CNN model.
        """

        assert len(filters_per_layer) == num_layers, "filters_per_layer length must match num_layers"
        assert len(kernels_per_layer) == num_layers, "kernels_per_layer length must match num_layers"

        model = Sequential(name="Generic1DCNN")
        model.add(Input(shape=input_shape))

        # Convolutional layers
        for i in range(num_layers):
            model.add(Conv1D(
                filters=filters_per_layer[i],
                kernel_size=kernels_per_layer[i],
                padding='same',
                name=f'conv_{i + 1}'
            ))
            model.add(BatchNormalization(name=f'bn_{i + 1}'))

        model.add(GlobalAveragePooling1D())

        # Dense head
        model.add(Dense(num_classes, activation='softmax', name='dense_1'))

        # Compile
        model.compile(
            optimizer=Adam(learning_rate=learning_rate),
            loss='sparse_categorical_crossentropy',
            metrics=['sparse_categorical_accuracy']
        )

        return model

    def piece_function(self, input_data: InputModel):
        x_train, y_train = self._readucr(input_data.train_data_path)
        x_train = x_train.reshape((x_train.shape[0], x_train.shape[1], 1))
        num_classes = len(np.unique(y_train))

        m = self._build_model(
            input_shape=x_train.shape[1:],
            num_layers=input_data.num_layers,
            filters_per_layer=input_data.filters_per_layer,
            kernels_per_layer=input_data.kernels_per_layer,
            num_classes=num_classes
        )

        callbacks = [
            keras.callbacks.ModelCheckpoint(
                "best_model.keras", save_best_only=True, monitor="val_loss"
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

        model_file_path = os.path.join(Path(self.results_path), 'model.h5')
        m.save(model_file_path)

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
            model_file_path=model_file_path
        )
