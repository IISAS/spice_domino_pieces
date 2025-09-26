from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone


class SelectDatesPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        df_data = pd.read_csv(input_data.fve_input_file, parse_dates=['DateTime'])
        self.logger.info(df_data.iloc[0]['DateTime'], input_data.date_start, input_data.date_end, datetime(input_data.date_start, tzinfo=timezone.utc), datetime(input_data.date_end, tzinfo=timezone.utc))
        print(df_data.iloc[0]['DateTime'], input_data.date_start, input_data.date_end)

        df_sel_data = (df_data['DateTime'] > input_data.date_start) & (df_data['DateTime'] <= input_data.date_end)

        message = f"Data successfully filtered by start and end dates"
        file_path = str(Path(self.results_path) / "data_filtered.csv")
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
