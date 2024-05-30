import os
import mlflow
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from app.core.config import config

os.environ["AWS_ACCESS_KEY_ID"] = config.AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = config.AWS_SECRET_ACCESS_KEY
# os.environ['MLFLOW_S3_ENDPOINT_URL'] = config.MLFLOW_S3_ENDPOINT_URL

# def get_latest_run_id(experiment_name: str) -> str:
#     client = MlflowClient()
#     experiment = client.get_experiment_by_name(experiment_name)
#     if experiment is None:
#         raise Exception(f"Experiment with name '{experiment_name}' does not exist")

#     runs = client.search_runs(experiment_ids=[experiment.experiment_id], order_by=["start_time DESC"], max_results=1)
#     if not runs:
#         raise Exception(f"No runs found for experiment '{experiment_name}'")

#     return runs[0].info.run_id


def load_model():

    os.environ["AWS_ACCESS_KEY_ID"] = config.AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.AWS_SECRET_ACCESS_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = config.MLFLOW_S3_ENDPOINT_URL

    print("Loading model")
    # run_id = get_latest_run_id(config.EXPERIMENT_NAME)

    model = mlflow.sklearn.load_model("models:/{config.EXPERIMENT_NAME}/production")
    return model
