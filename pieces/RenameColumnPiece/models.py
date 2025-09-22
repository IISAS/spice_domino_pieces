from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    RenameColumn Piece Input Model
    """

    meteo_fve_input_file: str = Field(
        title="Meteo and FVE data",
        description="The path to the joined Meteo and FVE data",
        # json_schema_extra={"from_upstream": "always"}
    )

    original_column_name: str = Field(
        title="Original column name",
        default="Loc01",
        description="The name of original column"
    )

    new_column_name: str = Field(
        title="New column name",
        default="FVE",
        description="The name of the renamed column"
    )


class OutputModel(BaseModel):
    """
    RenameColumn Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    file_path: str = Field(
        description="The path to the Meteo and FVE data with renamed column"
    )
