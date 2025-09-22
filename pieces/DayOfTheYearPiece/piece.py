from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class DayOfTheYearPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        def day_of_the_year(row):
            return row['DateTime'].timetuple().tm_yday

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file, parse_dates=['DateTime'])
        df_sel_data['DayOfYear'] = df_sel_data.apply(day_of_the_year, axis=1)

        message = f"Meteo and FVE data day of the year column added successfully"
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
