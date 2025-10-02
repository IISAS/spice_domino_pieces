from pydantic import BaseModel, Field
from datetime import date


class InputModel(BaseModel):
    """
    InputDataSEPS Piece Input Model
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

    meteo_input_file: str = Field(
        title='Meteo data',
        default='/home/shared_storage/meteo_2021+2022upr.csv',
        description="The path to the Meteo Data",
    )

    ciselniky_input_file: str = Field(
        title="Ciselniky input data",
        default='/home/shared_storage/lokality_FVE_ciselnik.csv',
        description="The path to the Ciselniky Data",
    )

    date_start: date = Field(
        title="Start date",
        description="Start date for FVE data",
    )

    date_end: date = Field(
        title="End date",
        description="End date for FVE data",
    )


class OutputModel(BaseModel):
    """
    InputDataSEPS Piece Output Model
    """
    fve_input_file: str = Field(
        description="The path to the FVE Data",
    )

    location: str = Field(
        description="Location to filter the FVE Data",
    )

    meteo_input_file: str = Field(
        description="The path to the Meteo Data",
    )

    ciselniky_input_file: str = Field(
        description="The path to the Ciselniky Data",
    )

    date_start: date = Field(
        description="Start date for FVE data",
    )

    date_end: date = Field(
        description="End date for FVE data",
    )
