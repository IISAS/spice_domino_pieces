from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path
from sklearn.ensemble import RandomForestRegressor
import plotly.express as px
import pickle as pk


class TrainRandomForestRegressorPiece(BasePiece):

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
        Train Random Forest Regressor the data into training and test sets.
        """
        train_data = self.read_data_from_file(path=input_data.train_data_path)

        if input_data.target_column not in train_data.columns:
            raise ValueError("Target column not found in data with name '" + input_data.target_column + "'." +
                             "Columns in the DataFrame are: " + train_data.columns)

        # Train model
        regressor = RandomForestRegressor(
            n_estimators=input_data.n_estimators,
            random_state=input_data.random_state,
            oob_score=input_data.oob_score,
            criterion=input_data.criterion,
            max_depth=input_data.max_depth,
            bootstrap=input_data.bootstrap,
            n_jobs=input_data.n_jobs,
            max_samples=input_data.max_samples,
            verbose=1
        )

        regressor.fit(train_data.drop(columns=[input_data.target_column], axis=1), train_data[input_data.target_column])

        feature_imp = pd.Series(
            regressor.feature_importances_,
            index=train_data.drop(input_data.target_column, axis=1).columns
        ).sort_values(ascending=True)

        fig = px.bar(x=feature_imp.values, y=feature_imp.index, orientation='h')
        fig.update_layout(
            xaxis_title='Feature Importance Score',
            yaxis_title='Features',
            title='Feature Importance',
            plot_bgcolor='white',
            xaxis=dict(gridcolor='lightgray'),
            yaxis=dict(gridcolor='lightgray')
        )

        fig_path = str(Path(self.results_path) / "feature_importance.json")
        fig.write_json(fig_path)

        self.display_result = {
            'file_type': 'plotly_json',
            'file_path': fig_path
        }

        model_path = str(Path(self.results_path) / "random_forest_model.pkl")
        with open(model_path, "wb") as f:
            pk.dump(regressor, f)

        message = f"Regressor mode successfully"

        # Return output
        return OutputModel(
            message=message,
            random_forest_model_path=str(model_path)
        )
