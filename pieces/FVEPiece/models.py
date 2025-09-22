from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    FVE Piece Input Model
    """

    fve_input_file: str = Field(
        title="FVE input data",
        default='/home/shared_storage/FVE_2021+2022upr.csv',
        description="The path to the FVE Data",
    )
    location: str = Field(
        title="Choose the location",
        default='Loc01',
        description="Location to filter the FVE Data",
    )


class OutputModel(BaseModel):
    """
    FVE Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the filtered FVE file by the specified Location"
    )
