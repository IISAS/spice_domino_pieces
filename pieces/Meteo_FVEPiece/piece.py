from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class Meteo_FVEPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_fve = pd.read_csv(input_data.fve_input_file)
        df_meteo = pd.read_csv(input_data.meteo_input_file)

        df_sel_data = pd.merge(df_meteo, df_fve, on='DateTime', how='inner')

        message = f"SQl joined Meteo and FVE executed successfully"
        file_path = str(Path(self.results_path) / "FVE_Data_joined.csv")
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
