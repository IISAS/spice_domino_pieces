from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class IsDayPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        def add_is_day_bool(row):
            # ret = 0
            # if row['isDayProb'] > 0.03:
            #     ret = 1
            # return ret
            return row['isDayProb'] > 0.03

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file)
        df_sel_data['isDayBool'] = df_sel_data.apply(add_is_day_bool, axis=1)

        message = f"Meteo and FVE data is day column added successfully"
        file_path = str(Path(self.results_path) / "FVE.csv")
        df_sel_data.to_csv(file_path, index=False)

        # Set display result
        self.display_result = {
            "file_type": "csv",
            "file_path": file_path
        }

        # Return output
        return OutputModel(
            message=message,
            file_path=file_path
        )
