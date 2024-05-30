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

    mlflow.set_tracking_uri(config.MLFLOW_S3_ENDPOINT_URL)
    model_uri = "s3://mlflow/1/c916bd20aaec424c91ede491068e533c/artifacts/model"
    model = mlflow.pyfunc.load_model(model_uri)
    return model


def preprocess(df):
    df.replace(" ", np.nan, inplace=True)
    df.dropna(inplace=True)
    df[["date", "opendt"]] = df[["date", "opendt"]].apply(
        pd.to_datetime, format="%Y-%m-%d"
    )
    df["dateAfter7Days"] = df["opendt"] + pd.Timedelta(days=7)
    df["sameOpenDtCnt"] = df.groupby("opendt")["moviecd"].transform("nunique")
    df["showAcc"] = np.nan
    df["scrnAcc"] = np.nan

    for moviecd in df["moviecd"].unique():
        subset = df[df["moviecd"] == moviecd]
        if not subset.empty:
            mask = (df["date"] >= subset["opendt"].values[0]) & (
                df["date"] <= subset["dateAfter7Days"].values[0]
            )
            filtered_data = df[mask & (df["moviecd"] == moviecd)]

            if not filtered_data.empty:
                chowcnt_sum = filtered_data["showcnt"].sum()
                scrncnt_sum = filtered_data["scrncnt"].sum()

                df.loc[df["moviecd"] == moviecd, "showAcc"] = chowcnt_sum
                df.loc[df["moviecd"] == moviecd, "scrnAcc"] = scrncnt_sum

    df.dropna(inplace=True)
    tz_kst = pytz.timezone("Asia/Seoul")
    today_kst = datetime.now(tz_kst)
    yesterday_kst = today_kst - timedelta(days=1)
    yesterday_str = yesterday_kst.strftime("%Y-%m-%d")

    last = (
        df.sort_values("date")
        .groupby("moviecd")
        .last()
        .reset_index()[["date", "moviecd", "audiacc"]]
    )
    last = last[last["date"] != yesterday_str]
    last = last.rename(columns={"audiacc": "total"})
    last.drop(columns="date", inplace=True)

    df = pd.merge(df, last, how="left", on="moviecd")
    df = df[df["date"] == df["dateAfter7Days"]]
    df.dropna(inplace=True)
    # df = df[["showcnt", "scrncnt", "audiacc", "sameOpenDtCnt", "showAcc", "scrnAcc", "total"]]
    df = df[["showcnt", "scrncnt", "audiacc", "sameOpenDtCnt", "showAcc", "scrnAcc"]]

    return df
