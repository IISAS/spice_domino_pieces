from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path


class MeteoPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_cisleniky_loc = pd.read_csv(input_data.ciselniky_input_file)
        df_meteo = pd.read_csv(input_data.meteo_input_file)

        columns = ['ziarenie_', 'teplota_', 'vlhkost_']
        columns_new = []
        for col in columns:
            columns_new = columns_new + [col + df_cisleniky_loc.iloc[0]['NearMeteo1'], col + df_cisleniky_loc.iloc[0]['NearMeteo2'], col + df_cisleniky_loc.iloc[0]['NearMeteo3'], col + df_cisleniky_loc.iloc[0]['NearMeteo4']]

        sel_meteo_data = df_meteo.filter(['DateTime'] + columns_new)

        sel_meteo_data.columns = ['DateTime', 'Near1_ziarenie', 'Near2_ziarenie', 'Near3_ziarenie', 'Near4_ziarenie', 'Near1_teplota', 'Near2_teplota', 'Near3_teplota', 'Near4_teplota', 'Near1_vlhkost', 'Near2_vlhkost', 'Near3_vlhkost', 'Near4_vlhkost',]

        message = f"Meteo filtered for Ciselniky executed successfully"
        file_path = str(Path(self.results_path) / "Meteo_Data_filtered.csv")
        sel_meteo_data.to_csv(file_path, index=False)

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
