from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    AddSun Piece Input Model
    """

    meteo_fve_input_file: str = Field(
        title="Meteo and FVE data",
        description="The path to the joined Meteo and FVE data",
        # json_schema_extra={"from_upstream": "always"}
    )

    slnko_input_file: str = Field(
        title="Sun hours data",
        default='/home/shared_storage/slnkoCasy2.csv',
        description="The path to the Sun hours data",
    )


class OutputModel(BaseModel):
    """
    AddSun Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the Meteo and FVE data with added is day probability column"
    )
