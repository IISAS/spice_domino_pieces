import json
import os
from pathlib import Path

import numpy as np
from domino.base_piece import BasePiece
from tensorflow import keras

from .models import InputModel, OutputModel


class TimeSeriesClassificationInferencePiece(BasePiece):

    def _load_tsv_data(self, filename, target_column_idx: int = None, skiprows: int = 0):
        self.logger.info("Loading TSV data: %s", filename)
        data = np.loadtxt(filename, delimiter="\t", skiprows=skiprows)
        Y = data[:, target_column_idx] if target_column_idx is not None else None
        self.logger.info("Shape Y: %s", Y.shape if Y is not None else None)
        X = np.delete(data, target_column_idx, axis=1) if target_column_idx is not None else data
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

    def piece_function(self, input_data: InputModel):

        m = keras.models.load_model(input_data.model_file_path)

        # load input data
        x_input, y_input = self._load_tsv_data(
            input_data.input_data_path,
            target_column_idx=input_data.target_column_idx,
            skiprows=input_data.skiprows
        )

        output_data_path = os.path.join(
            Path(self.results_path),
            OutputModel.model_fields['inference_output_data_path'].default
        )

        self.logger.info('Model inference running...')
        y_pred = m.predict(x_input)
        self.logger.info('Model inference done.')

        if input_data.output_format.lower() == 'classes':
            self.logger.info('Computing prediction classes from probabilities...')
            y_pred = np.argmax(y_pred, axis=1)
            self.logger.info('Prediction classes computed.')
            self.logger.info('Saving prediction classes to %s.', output_data_path)
            np.savetxt(output_data_path, y_pred, delimiter="\t", newline="\n")
            self.logger.info('Predictions classes saved.')

        # there's a ground truth, so we can perform evaluation
        evaluation_output_file_path = None
        if y_input is not None:
            evaluation_output_file_path = os.path.join(
                self.results_path,
                OutputModel.model_fields['evaluation_output_file_path'].default
            )
            self.logger.info('Evaluating model...')
            eval_results = m.evaluate(x_input, y_input, return_dict=True)
            self.logger.info('Model evaluation done.')
            self.logger.info('Saving evaluation results to %s.', evaluation_output_file_path)
            with open(evaluation_output_file_path, 'w') as fp:
                json.dump(eval_results, fp)
                self.logger.info('Evaluation results saved.')

        # Set display result
        # TODO: confusion matrix
        self.display_result = {
        }

        # Return output
        return OutputModel(
            inference_output_data_path=output_data_path,
            **(
                {
                    'evaluation_output_file_path': evaluation_output_file_path
                } if evaluation_output_file_path else {}
            )
        )
