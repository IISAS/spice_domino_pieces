from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path
import math


class AddSunPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_slnko = pd.read_csv(input_data.slnko_input_file)

        def add_sun_attr(row):
            denX = row['DayOfYear']
            minX = row['MinOfDay']
            cas_1 = df_slnko.iloc[denX - 1]['VychodSlnkaMin']
            cas_2 = df_slnko.iloc[denX - 1]['ZapadSlnkaMin']
            ret = 0.0
            if cas_1 <= minX <= cas_2:
                # aproximujem to sinusom
                # resTab.isDayProb(k) = sin(pi*(minX - casy(1)) / (casy(2) - casy(1)));
                ret = math.sin(math.pi*(minX-cas_1)/(cas_2 - cas_1))
            return ret

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file)
        df_sel_data['isDayProb'] = df_sel_data.apply(add_sun_attr, axis=1)

        message = f"Meteo and FVE data is day probability column added successfully"
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
