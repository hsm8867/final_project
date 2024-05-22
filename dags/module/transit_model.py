from mlflow.tracking import MlflowClient

def transition_model_stage(model_name: str, **context):
    version = context["ti"].xcom_pull(key="model_version")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")

    client = MlflowClient()
    production_model = None
    current_model = client.get_model_version(model_name, version)

    filter_string = f"name='{current_model.name}'"
    results = client.search_model_versions(filter_string)

    for mv in results:
        if mv.current_stage == "Production":
            production_model = mv

    if production_model is None:
        client.transition_model_version_stage(
            current_model.name, current_model.version, "Production"
        )
        production_model = current_model
    else:
        current_metric = client.get_run(current_model.run_id).data.metrics[eval_metric]
        production_metric = client.get_run(production_model.run_id).data.metrics[
            eval_metric
        ]

        if current_metric > production_metric:
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Production",
                archive_existing_versions=True,
            )
            production_model = current_model

    context["ti"].xcom_push(key="production_version", value=production_model.version)
    print(
        f"Done Deploy Production_Model, production_version: {production_model.version}"
    )