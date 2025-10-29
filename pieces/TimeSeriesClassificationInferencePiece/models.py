from typing import Literal

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    TimeSeriesClassificationInferencePiece Input Model
    """

    model_file_path: str = Field(
        title="model_file_path",
        description="Path to the model file.",
    )

    input_data_path: str = Field(
        title="input_data_path",
        description="Path to the data to be used for inference.",
    )

    skiprows: int = Field(
        title="skiprows_train",
        description="The number of rows to skip in the input data.",
    )

    target_column_idx: int | None = Field(
        title="target_column_idx",
        description="Index of the target column if present (e.g. in a test dataset).",
        default=None
    )

    output_format: Literal['probabilities', 'classes'] = Field(
        title="output_format",
        description="Output format for predictions.",
        default="probabilities"
    )


class OutputModel(BaseModel):
    """
    TimeSeriesClassificationInferencePiece Output Model
    """
    inference_output_data_path: str = Field(
        title="inference_output_data_path",
        description="Path to be used for saving the inference output.",
        default='output_inference.tsv'
    )
    evaluation_output_file_path: str = Field(
        title="evaluation_output_file_path",
        description="Path to be used for saving the evaluation output.",
        default='output_evaluation.json'
    )
