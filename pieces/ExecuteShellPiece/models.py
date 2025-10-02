from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    ExecuteShell Piece Input Model
    """

    fve_input_file: str = Field(
        title="Input data",
        # default='/home/shared_storage/FVE_2021+2022upr.csv',
        description="The path to the Input Data",
    )


class OutputModel(BaseModel):
    """
    ExecuteShell Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the Output Data"
    )
