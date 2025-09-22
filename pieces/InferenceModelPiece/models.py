from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class Criterion(str, Enum):
    squared_error = "squared_error"
    absolute_error = "absolute_error"
    friedman_mse = "friedman_mse"
    poisson = "poisson"


class InputModel(BaseModel):
    """
    InferenceModel Piece Input Model
    """

    test_data_path: str = Field(
        title="Data path",
        description="Data path to inference on.",
        # json_schema_extra={"from_upstream": "always"}
    )

    trained_model_path: str = Field(
        title="Model path",
        description="Path to the model to use for inference.",
        # json_schema_extra={"from_upstream": "always"}
    )

    target_column: str = Field(
        title="Target column",
        default="FVE",
        description="The name of the target column.",
    )


class OutputModel(BaseModel):
    """
    InferenceModel Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    data_path: str = Field(
        description="The path to the predicted values."
    )
