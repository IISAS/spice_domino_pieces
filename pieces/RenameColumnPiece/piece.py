from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class RenameColumnPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file)
        df_sel_data = df_sel_data.rename(columns={input_data.original_column_name: input_data.new_column_name})

        message = f"Meteo and FVE data column renamed successfully"
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
