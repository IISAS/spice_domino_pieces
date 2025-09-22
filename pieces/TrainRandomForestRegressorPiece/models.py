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
    TrainRandomForestRegressor Piece Input Model
    """

    train_data_path: str = Field(
        title="Train Data Path",
        description="The path to the train data to train the data.",
        # json_schema_extra={"from_upstream": "always"}
    )

    n_estimators: int = Field(
        title="Number of Estimators",
        description="The number of trees in the forest.",
        default=100,
    )

    criterion: Criterion = Field(
        title="Criterion",
        description="The function to measure the quality of a split.",
        default=Criterion.squared_error,
    )

    max_depth: Optional[int] = Field(
        title="Max Depth",
        description="The maximum depth of the tree.",
        default=None,
    )

    bootstrap: bool = Field(
        description="Whether bootstrap samples are used when building trees."
                    "If False, the whole dataset is used to build each tree.",
        default=True,
    )

    oob_score: bool = Field(
        description="Whether to use out-of-bag samples to estimate the generalization score."
                    "By default, r2_score is used."
                    "Provide a callable with signature metric(y_true, y_pred) to use a custom metric."
                    "Only available if bootstrap=True.",
        default=True,
    )

    n_jobs: Optional[int] = Field(
        title="Number of jobs in parallel",
        description="The number of jobs to run in parallel. fit, predict, decision_path and apply are all parallelized over the trees. "
                    "-1 means using all processors.",
        default=1,
    )

    random_state: int = Field(
        title="Random state",
        description="Controls both the randomness of the bootstrapping of the samples used when building trees (if bootstrap=True) and the sampling of the features to consider when looking for the best split at each node (if max_features < n_features)",
        default=42,
    )

    max_samples: Optional[int] = Field(
        title="Max samples",
        description="If bootstrap is True, the number of samples to draw from X to train each base estimator.",
        default=None,
    )

    target_column: str = Field(
        title="Target column",
        default="FVE",
        description="The name of the target column.",
    )


class OutputModel(BaseModel):
    """
    TrainRandomForestRegressor Piece Output Model
    """
    message: str = Field(
        default="",
        description="Output message to log"
    )

    random_forest_model_path: str = Field(
        description="The path to the random forest regressor"
    )
