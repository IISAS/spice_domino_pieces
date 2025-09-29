from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
from pathlib import Path


class InputDataSEPSPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        # Set display result
        # self.display_result = {
        #     "file_type": "csv",
        #     "file_path": file_path
        # }

        # Return output
        return OutputModel(
            fve_input_file=input_data.fve_input_file,
            location=input_data.location,
            meteo_input_file=input_data.meteo_input_file,
            ciselniky_input_file=input_data.ciselniky_input_file,
            date_start=input_data.date_start,
            date_end=input_data.date_end
        )
