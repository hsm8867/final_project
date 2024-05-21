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


def train_fn(**context):
    os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_ACCESS_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = ENDPOINT_URL
    mlflow.set_experiment("movie_model")

    processed_data = preprocess(load())
    # dictionary를 DataFrame으로 변환
    data = pd.DataFrame.from_dict(processed_data)
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

    study.optimize(objective, n_trials=100)
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


# def create_model_version(model_name: str, **context):
#     run_id = context["ti"].xcom_pull(key="run_id")
#     model_uri = context["ti"].xcom_pull(key="model_uri")
#     eval_metric = context["ti"].xcom_pull(key="eval_metric")

#     # Create Model Version

#     client = MlflowClient()
#     try:
#         client.create_registered_model(model_name)
#     except Exception as e:
#         print("Model already exists")

#     current_metric = client.get_run(run_id).data.metrics[eval_metric]
#     model_source = RunsArtifactRepository.get_underlying_uri(model_uri)
#     model_version = client.create_model_version(
#     model_name, model_source, run_id, description=f"{eval_metric}: {current_metric}"
#     )

#     context["ti"].xcom_push(key="model_version", value=model_version.version)
#     print(f"Done Create model version, model_version: {model_version}")


#     def transition_model_stage(model_name: str, **context):
#         version = context["ti"].xcom_pull(key="model_version")
#         eval_metric = context["ti"].xcom_pull(key="eval_metric")

#         client = MlflowClient()
#         production_model = None
#         current_model = client.get_model_version(model_name, version)

#         filter_string = f"name='{current_model.name}'"
#         results = client.search_model_versions(filter_string)

#         for mv in results:
#             if mv.current_stage == "Production":
#                 production_model = mv

#             if production_model is None:
#                 client.transition_model_version_stage(
#                     current_model.name, current_model.version, "Production"
#                 )
#                 production_model = current_model
#             else:
#                 current_metric = client.get_run(current_model.run_id).data.metrics[eval_metric]
#                 production_metric = client.get_run(production_model.run_id).data.metrics[
#                     eval_metric
#                 ]

#                 if current_metric > production_metric:
#                     client.transition_model_version_stage(
#                         current_model.name,
#                         current_model.version,
#                         "Production",
#                         archive_existing_versions=True,
#                     )
#                     production_model = current_model

#             context["ti"].xcom_push(key="production_version", value=production_model.version)
#             print(
#             f"Done Deploy Production_Model, production_version: {production_model.version}"
#             )
