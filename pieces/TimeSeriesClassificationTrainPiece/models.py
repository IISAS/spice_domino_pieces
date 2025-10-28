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
        default=True,
        description="Shuffle data before training."
    )

    shuffle_before_epoch: bool = Field(
        title="shuffle_before_epoch",
        default=False,
        description="Shuffle training data before each epoch."
    )

    validation_split: float = Field(
        title="validation_split",
        default=0.2,
        description="Validation split if no validation data provided."
    )

    standardize_labels: bool = Field(
        title="standardize_labels",
        default=True,
        description="Standardize labels before training and provide label mapping."
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
    label_mapping_file_path: str = Field(
        title="label_mapping_file_path",
        default="label_mapping.npy",
        description="Path to the label mapping file produced when standardize_labels is True."
    )
