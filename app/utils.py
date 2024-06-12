import os
import mlflow
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from app.core.config import config


def load_model():

    os.environ["AWS_ACCESS_KEY_ID"] = config.AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.AWS_SECRET_ACCESS_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = config.MLFLOW_S3_ENDPOINT_URL
    mlflow.set_tracking_uri("http://10.178.0.3:5000")
    print("Loading model")
    model_name = config.MODEL_NAME
    model_stage = config.MODEL_STAGE
    model = mlflow.sklearn.load_model(f"models:/{model_name}/{model_stage}")
    return model