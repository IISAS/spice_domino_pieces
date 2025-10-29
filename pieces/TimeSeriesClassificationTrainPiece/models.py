from typing import List

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    TimeSeriesClassificationTrainPiece Input Model
    """

    train_data_path: str = Field(
        title="train_data_path",
        description="Path to the train data.",
        default="https://raw.githubusercontent.com/hfawaz/cd-diagram/master/FordA/FordA_TRAIN.tsv"
        # json_schema_extra={"from_upstream": "always"}
    )

    validation_data_path: str = Field(
        title="validation_data_path",
        description="Path to the validation data.",
        default=None
    )

    target_column_idx: int = Field(
        title="target_column_idx",
        description="Index of the target column.",
        default=-1
    )

    skiprows_train: int = Field(
        title="skiprows_train",
        description="The number of rows to skip in the train data",
    )

    skiprows_val: int = Field(
        title="skiprows_val",
        description="The number of rows to skip in the validation data (if specified)",
    )

    num_layers: int = Field(
        title="num_layers",
        default=3,
        description="Number of convolutional layers."
    )

    filters_per_layer: List[int] = Field(
        title="filters_per_layer",
        default=[64, 64, 64],
        description="Number of filters for each convolutional layer."
    )

    kernel_sizes: List[int] = Field(
        title="kernel_sizes",
        default=[3, 3, 3],
        description="Kernel size for each convolutional layer."
    )

    batch_size: int = Field(
        title="batch_size",
        default=32,
        description="Batch size."
    )

    epochs: int = Field(
        title="epochs",
        default=500,
        description="Number of epochs."
    )

    shuffle: bool = Field(
        title="shuffle",
        default=False,
        description="Shuffle training data before each epoch."
    )

    validation_split: float = Field(
        title="validation_split",
        default=0.2,
        description="Validation split if no validation data provided."
    )


class OutputModel(BaseModel):
    """
    TimeSeriesClassificationTrainPiece Output Model
    """
    best_model_file_path: str = Field(
        title="best_model_file_path",
        default="best_model.keras",
        description="Path to the saved best model."
    )
    last_model_file_path: str = Field(
        title="last_model_file_path",
        default="last_model.keras",
        description="Path to the saved last model."
    )
