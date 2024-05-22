import mlflow
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository

def create_model_version(model_name: str, **context):
    run_id = context["ti"].xcom_pull(key="run_id")
    model_uri = context["ti"].xcom_pull(key="model_uri")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")

    # Create Model Version

    client = MlflowClient()
    try:
        client.create_registered_model(model_name)
    except Exception as e:
        print("Model already exists")

    current_metric = client.get_run(run_id).data.metrics[eval_metric]
    model_source = RunsArtifactRepository.get_underlying_uri(model_uri)
    model_version = client.create_model_version(
        model_name, model_source, run_id, description=f"{eval_metric}: {current_metric}"
    )

    context["ti"].xcom_push(key="model_version", value=model_version.version)
    print(f"Done Create model version, model_version: {model_version}")