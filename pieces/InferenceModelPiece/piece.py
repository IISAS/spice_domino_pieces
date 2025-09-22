from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
import plotly.express as px
import pickle as pk
from sklearn.metrics import mean_squared_error, r2_score


class InferenceModelPiece(BasePiece):

    def read_data_from_file(self, path):
        """
        Read data from a file.
        """
        if path.endswith(".csv"):
            return pd.read_csv(path)
        elif path.endswith(".json"):
            return pd.read_json(path)
        else:
            raise ValueError("File type not supported.")

    def piece_function(self, input_data: InputModel):
        """
        Predict data.
        """
        test_data = self.read_data_from_file(path=input_data.test_data_path)

        if input_data.target_column not in test_data.columns:
            raise ValueError("Target column not found in data with name '" + input_data.target_column + "'." +
                             "Columns in the DataFrame are: " + test_data.columns)

        # Load model
        with open(input_data.trained_model_path, "rb") as f:
            model = pk.load(f)

        predictions = model.predict(test_data.drop(input_data.target_column, axis=1))

        # Save predictions
        predictions_path = str(Path(self.results_path) / "predictions.csv")
        test_data['predictions'] = predictions
        test_data.to_csv(predictions_path, index=False)

        y_test = test_data[input_data.target_column]

        md_text = ""
        if model.oob_score:
            oob_score = model.oob_score_
            md_text = f"**OOB score:** {oob_score}  \n"

        mse = mean_squared_error(y_test, predictions)
        md_text += f"**MSE:** {mse}  \n"
        # print(f'Mean Squared Error: {mse}')

        r2 = r2_score(y_test, predictions)
        md_text += f"**R2:** {r2}  \n"
        # print(f'R-squared: {r2}')

        statistics_path = str(Path(self.results_path) / "statistics.md")

        with open(statistics_path, "w") as f:
            f.write(md_text)

        self.display_result = {
            "file_type": "md",
            "file_path": statistics_path
        }

        # Plot predictions
        # self.display_result = {
        #     'file_type': 'md',
        #     'file_path': results_path
        # }

        message = f"Inference finished successfully"

        # Return output
        return OutputModel(
            message=message,
            data_path=str(predictions_path)
        )
