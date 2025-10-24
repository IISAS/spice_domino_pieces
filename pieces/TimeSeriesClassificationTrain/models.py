from typing import List

from pydantic import BaseModel, Field


class InputModel(BaseModel):
    """
    AIKeras1DCNNTrain Piece Input Model
    """

    train_data_path: str = Field(
        title="train data path",
        description="Path to the train data.",
        default="https://raw.githubusercontent.com/hfawaz/cd-diagram/master/FordA/"
        # json_schema_extra={"from_upstream": "always"}
    )

    num_layers: int = Field(
        title="number of layers",
        default=3,
        description="Number of convolutional layers."
    )

    filters_per_layer: List[int] = Field(
        title="number of filters",
        default=[64, 64, 64],
        description="Number of filters for each convolutional layer."
    )

    kernel_sizes: List[int] = Field(
        title="kernel sizes",
        default=[3, 3, 3],
        description="Kernel size for each convolutional layer."
    )

    batch_size: int = Field(
        title="batch size",
        default=32,
        description="Batch size."
    )

    epochs: int = Field(
        title="epochs",
        default=500,
        description="Number of epochs."
    )


class OutputModel(BaseModel):
    """
    TimeSeriesClassificationTrain Piece Output Model
    """
    best_model_file_path: str = Field(
        description="Path to the saved best model."
    )
    last_model_file_path: str = Field(
        description="Path to the saved last model."
    )
