from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    Meteo and FVE Piece Input Model
    """

    fve_input_file: str = Field(
        title="Filtered FVE data",
        description="Output from FVE Data piece",
        # json_schema_extra={"from_upstream": "always"}
    )
    meteo_input_file: str = Field(
        title="Filtered Meteo data",
        description="Output from Meteo Data piece",
        # json_schema_extra={"from_upstream": "always"}
    )


class OutputModel(BaseModel):
    """
    Meteo and FVE Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the SQl joined Meteo and FVE file"
    )
