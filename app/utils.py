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


def check_movie_count(df: pd.DataFrame, moviename: str) -> bool:
    movie_count = df[df["movienm"] == moviename].shape[0]
    return movie_count >= 7


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    # TODO
    # 날짜별로 정렬, 최근 7개만 가져옴
    # audiacc의 쵀다값, showcnt의 합, scrncnt의 합 계산
    # dataframe 생성 // column이 3개: audiacc, showacc, scrnacc
    # 최근 모델 가져오기
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("category")

    return df
