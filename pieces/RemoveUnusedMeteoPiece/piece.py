from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class RemoveUnusedMeteoPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file)
        df_final = df_sel_data.drop(["DateTime", "isDayBool", "Near1_vlhkost", "Near2_vlhkost", "Near3_vlhkost", "Near4_vlhkost", "Near1_teplota", "Near2_teplota", "Near3_teplota", "Near4_teplota", "Near1_ziarenie", "Near2_ziarenie", "Near3_ziarenie", "Near4_ziarenie"], axis='columns')

        message = f"Meteo and FVE data advanced meteo columns added successfully"
        file_path = str(Path(self.results_path) / "FVE.csv")
        df_final.to_csv(file_path, index=False)

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
