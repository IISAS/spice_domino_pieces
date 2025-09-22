from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class CiselnikyPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_ciselniky = pd.read_csv(input_data.ciselniky_input_file)
        df_cisleniky_loc = df_ciselniky.loc[df_ciselniky['FVELocationID'] == input_data.location]

        message = f"Ciselniky filtered for location: {input_data.location} executed successfully"
        file_path = str(Path(self.results_path) / "lokality_FVE_ciselnik_filtered.csv")
        df_cisleniky_loc.to_csv(file_path, index=False)

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
