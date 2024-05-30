import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error

import optuna
from optuna.storages import RDBStorage

from airflow.providers.postgres.hooks.postgres import PostgresHook

import mlflow
from module.load_data import load
from module.preprocess_data import preprocess

def train_fn(experiment_name: str, **context):
    mlflow.set_experiment(experiment_name)
    data = load()
    data = preprocess(data)

    y = data["total"]
    X = data.drop(columns=["total"])

    x_train, x_valid, y_train, y_valid = train_test_split(
        X, y, test_size=0.3, shuffle=True)
    x_train = x_train.reset_index(drop=True)
    x_valid = x_valid.reset_index(drop=True)

    cat_columns = ['repgenrenm']
    num_columns = [c for c in X.columns if c not in cat_columns]

    preprocessor = ColumnTransformer(
        transformers=[
            ("scaler", StandardScaler(), num_columns),
            ("encoding", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_columns)
        ]
    )

    x_train = preprocessor.fit_transform(x_train)
    x_valid = preprocessor.transform(x_valid)

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

        return rmse

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    storage = RDBStorage(url=hook.get_uri().replace("/raw", "/optuna"))

    # Optuna study
    study = optuna.create_study(
        study_name="movies", direction="minimize", storage=storage, load_if_exists=True
    )
    study.optimize(objective, n_trials=10)
    best_params = study.best_params

    model = XGBRegressor(**best_params)
    model.fit(x_train, y_train)

    # Model Log
    pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])

    metrics = {
        "mse": mean_squared_error(y_valid, model.predict(x_valid), squared=False)
    }

    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        model_info = mlflow.sklearn.log_model(pipeline, "model")

    context["ti"].xcom_push(key="run_id", value=model_info.run_id)
    context["ti"].xcom_push(key="model_uri", value=model_info.model_uri)
    context["ti"].xcom_push(key="eval_metric", value="mse")
    print(
        f"Done Train model, run_id: {model_info.run_id}, model_uri: {model_info.model_uri}"
    )
