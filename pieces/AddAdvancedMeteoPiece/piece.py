from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path
import math


class AddAdvancedMeteoPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        def mypow(x, k):
            if x > 0:
                sign = 1
            elif x == 0:
                sign = 0
            else:
                sign = -1
            return sign * math.pow(abs(x), k)

        def add_advanced_meteo_attr(row):
            Near1_Adv_vlhkost = (1 - 0.01 * row['Near1_vlhkost']) * row['isDayBool']
            Near2_Adv_vlhkost = (1 - 0.01 * row['Near2_vlhkost']) * row['isDayBool']
            Near3_Adv_vlhkost = (1 - 0.01 * row['Near3_vlhkost']) * row['isDayBool']
            Near4_Adv_vlhkost = (1 - 0.01 * row['Near4_vlhkost']) * row['isDayBool']

            Near1_Adv_teplota = mypow(row['Near1_teplota'], 2.2)
            Near2_Adv_teplota = mypow(row['Near2_teplota'], 2.2)
            Near3_Adv_teplota = mypow(row['Near3_teplota'], 2.2)
            Near4_Adv_teplota = mypow(row['Near4_teplota'], 2.2)

            Near1_Adv_ziarenie = (row['Near1_ziarenie'] / 1260) * row['isDayBool']
            Near2_Adv_ziarenie = (row['Near2_ziarenie'] / 1260) * row['isDayBool']
            Near3_Adv_ziarenie = (row['Near3_ziarenie'] / 1260) * row['isDayBool']
            Near4_Adv_ziarenie = (row['Near4_ziarenie'] / 1260) * row['isDayBool']
            return Near1_Adv_vlhkost, Near2_Adv_vlhkost, Near3_Adv_vlhkost, Near4_Adv_vlhkost, Near1_Adv_teplota, Near2_Adv_teplota, Near3_Adv_teplota, Near4_Adv_teplota, Near1_Adv_ziarenie, Near2_Adv_ziarenie, Near3_Adv_ziarenie, Near4_Adv_ziarenie

        df_sel_data = pd.read_csv(input_data.meteo_fve_input_file)
        df_sel_data[["Near1_Adv_vlhkost", "Near2_Adv_vlhkost", "Near3_Adv_vlhkost", "Near4_Adv_vlhkost", "Near1_Adv_teplota", "Near2_Adv_teplota", "Near3_Adv_teplota", "Near4_Adv_teplota", "Near1_Adv_ziarenie", "Near3_Adv_ziarenie", "Near3_Adv_ziarenie", "Near4_Adv_ziarenie"]] = df_sel_data.apply(add_advanced_meteo_attr, axis='columns', result_type='expand')

        message = f"Meteo and FVE data advanced meteo columns added successfully"
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
