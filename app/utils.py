import os
import mlflow
import pandas as pd
from app.core.config import config
from app.core.errors import error


def load_model():
    model_name = config.MODEL_NAME
    model_stage = config.MODEL_STAGE
    print("Loading model")
    model = mlflow.sklearn.load_model(f"models:/{model_name}/{model_stage}")
    return model


def prepare_for_predict(movie_data):
    if not movie_data:
        raise error.MovieNotFoundException()

    if len(movie_data) != 7:
        raise error.MovieNotEnoughException()

    audiacc = max(row.audiacc for row in movie_data)
    showacc = sum(row.showcnt for row in movie_data)
    scrnacc = sum(row.scrncnt for row in movie_data)
    repgenrenm = movie_data[0].repgenrenm

    df = pd.DataFrame(
        {
            "audiacc": [audiacc],
            "showAcc": [showacc],
            "scrnAcc": [scrnacc],
            "repgenrenm": [repgenrenm],
        }
    )

    return df
