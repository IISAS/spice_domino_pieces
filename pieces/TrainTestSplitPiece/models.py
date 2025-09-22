from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    TrainTestSplit Piece Input Model
    """

    data_path: str = Field(
        title="Data Path",
        description="The path to the data to be split.",
        # json_schema_extra={"from_upstream": "always"}
    )

    test_data_size: float = Field(
        default=0.2,
        description="The size (%) of the test data.",
        title="Test Data Ratio",
    )

    random_state: int = Field(
        default=42,
        description="The random state for the split.",
        title="Random State",
    )

    target_column: str = Field(
        title="Target column",
        default="FVE",
        description="The name of the target column.",
    )


class OutputModel(BaseModel):
    """
    TrainTestSplit Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    train_data_path: str = Field(
        description="The path to the train data set"
    )

    test_data_path: str = Field(
        description="The path to the test data set"
    )
