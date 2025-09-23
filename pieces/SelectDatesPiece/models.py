from pydantic import BaseModel, Field
from datetime import datetime


class InputModel(BaseModel):
    """
    SelectDates Piece Input Model
    """

    fve_input_file: str = Field(
        title="FVE data",
        description="The path to the joined Meteo and FVE data",
        # json_schema_extra={"from_upstream": "always"}
    )

    date_start: datetime = Field(
        title="Start date",
        description="Start date for FVE data",
        # json_schema_extra={"from_upstream": "always"}
    )

    date_end: datetime = Field(
        title="End date",
        description="End date for FVE data",
        # json_schema_extra={"from_upstream": "always"}
    )


class OutputModel(BaseModel):
    """
    SelectDates Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the filtered FVE data by the entered dates"
    )
