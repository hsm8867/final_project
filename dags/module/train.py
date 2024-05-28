import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from airflow.providers.postgres.hooks.postgres import PostgresHook

import optuna
from optuna.storages import RDBStorage
import numpy as np
from sklearn.model_selection import KFold
from xgboost import XGBRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.metrics import f1_score

from module.load_data import load
from module.preprocess_data import preprocess

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository

import datetime
from datetime import timedelta
import os


def train_fn(experiment_name: str, **context):
    mlflow.set_experiment(experiment_name)
    _data = load()
    data = preprocess(_data)
    y = data["total"]
    X = data.drop(columns=["total"])

    def manipulate(x_train, x_valid):
        tmp_x_train = x_train.copy()
        tmp_x_valid = x_valid.copy()

        tmp_x_train = tmp_x_train.reset_index(drop=True)
        tmp_x_valid = tmp_x_valid.reset_index(drop=True)

        num_columns = tmp_x_train.columns

        # scaling
        scaler = StandardScaler()
        tmp_x_train[num_columns] = scaler.fit_transform(tmp_x_train[num_columns])
        tmp_x_valid[num_columns] = scaler.transform(tmp_x_valid[num_columns])

        return tmp_x_train, tmp_x_valid

    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
            "learning_rate": trial.suggest_uniform("learning_rate", 0.01, 0.3),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "subsample": trial.suggest_uniform("subsample", 0.5, 1),
            "colsample_bytree": trial.suggest_uniform("colsample_bytree", 0.5, 1),
            "reg_alpha": trial.suggest_loguniform("reg_alpha", 1e-5, 10),
            "reg_lambda": trial.suggest_loguniform("reg_lambda", 1e-5, 10),
            "objective": "reg:squarederror",
            "random_state": 42,
        }
        rmses = []
        kf = KFold(n_splits=3, shuffle=True, random_state=42)

        for i, (trn_idx, val_idx) in enumerate(kf.split(data, y)):
            x_train, y_train = data.iloc[trn_idx, :], y.iloc[trn_idx]
            x_valid, y_valid = data.iloc[val_idx, :], y.iloc[val_idx]

            # 전처리
            x_train, x_valid = manipulate(x_train, x_valid)
            model = XGBRegressor(**params)
            model.fit(
                x_train,
                y_train,
                eval_set=[(x_train, y_train), (x_valid, y_valid)],
                eval_metric="rmse",
                early_stopping_rounds=20,
                verbose=False,
            )

            preds = model.predict(x_valid)
            rmse = mean_squared_error(y_valid, preds, squared=False)
            rmses.append(rmse)

        return np.mean(rmses)

    # optuna 저장
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    storage = RDBStorage(url=hook.get_uri().replace("/raw", "/optuna"))
    # optuna
    study = optuna.create_study(
        study_name="movies", direction="minimize", storage=storage, load_if_exists=True
    )

    study.optimize(objective, n_trials=3)
    best_params = study.best_params

    model = XGBRegressor(**best_params)
    x_train, x_valid, y_train, y_valid = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    x_train_scaled, x_valid_scaled = manipulate(x_train, x_valid)
    model.fit(x_train_scaled, y_train)

    # 메트릭 계산
    preds = model.predict(x_valid_scaled)
    rmse = mean_squared_error(y_valid, preds, squared=False)
    metrics = {"rmse": rmse}

    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        model_info = mlflow.sklearn.log_model(model, "model")

    context["ti"].xcom_push(key="run_id", value=model_info.run_id)
    context["ti"].xcom_push(key="model_uri", value=model_info.model_uri)
    context["ti"].xcom_push(key="eval_metric", value="rmse")
    print(
        f"Done Train model, run_id: {model_info.run_id}, model_uri: {model_info.model_uri}"
    )

    return best_params
