from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    Meteo Piece Input Model
    """

    meteo_input_file: str = Field(
        title='Meteo data',
        # default='/home/shared_storage/meteo_2021+2022upr.csv',
        description="The path to the Meteo Data",
    )

    ciselniky_input_file: str = Field(
        title="Filtered Ciselniky data",
        description="Output from Ciselniky Data piece",
        # json_schema_extra={"from_upstream": "always"}
    )


class OutputModel(BaseModel):
    """
    Meteo Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the filtered Meteo file by the specified Ciselniky Data"
    )
