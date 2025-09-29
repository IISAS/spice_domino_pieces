from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    Ciselniky Piece Input Model
    """

    ciselniky_input_file: str = Field(
        title="Ciselniky input data",
        # default='/home/shared_storage/lokality_FVE_ciselnik.csv',
        description="The path to the Ciselniky Data",
    )

    location: str = Field(
        title="Choose the location",
        # default='Loc01',
        description="Location to filter the Ciselniky Data",
    )


class OutputModel(BaseModel):
    """
    Ciselniky Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the filtered Ciselniky file by the specified Location"
    )
