import mlflow
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def transition_model_stage(**context: dict) -> None:
    s = time.time()
    ti = context["ti"]
    model_name: str = ti.xcom_pull(key="model_name")
    version = ti.xcom_pull(key="model_version")
    eval_metric = ti.xcom_pull(key="eval_metric")  # eval_metric은 f1 score로 되어 있다.
    client = MlflowClient()

    current_model = client.get_model_version(model_name, version)
    filter_string = f"name='{current_model.name}'"
    results = client.search_model_versions(filter_string)

    production_model = None
    for mv in results:
        if mv.current_stage == "Production":
            production_model = mv

    current_metric = client.get_run(current_model.run_id).data.metrics[eval_metric]

    if production_model is None:
        client.transition_model_version_stage(
            current_model.name, current_model.version, "Production"
        )
        production_model = current_model
        ti.xcom_push(key="production_version", value=production_model.version)
        logger.info(f"Production model deployed: version {production_model.version}")
    else:
        production_metric = client.get_run(production_model.run_id).data.metrics[
            eval_metric
        ]

        # 최근 경향을 최대한 반영하기 위해 7일마다 교체하도록 설정 (과거의 모델 성능이 최고점인 상태가 오래 지속될 경우 최근 데이터 경향을 반영못할 가능성때문)
        update_period_days = 7
        last_update_timestamp = (
            production_model.creation_timestamp / 1000
        )  # 프로덕션 모델의 생성 기준으로 timestamp불러옴. 시간이 밀리초여서 이걸 초로 변환
        last_update_date = datetime.fromtimestamp(
            last_update_timestamp, tz=timezone.utc
        )
        current_date = datetime.now(timezone.utc)
        days_since_last_update = (
            current_date - last_update_date
        ).days  # 모델생성 기준으로 현재시간과의 차이를 계산

        if (
            current_metric > production_metric
            or days_since_last_update >= update_period_days
        ):
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Production",
                archive_existing_versions=True,
            )
            production_model = current_model
            ti.xcom_push(key="production_version", value=production_model.version)
            logger.info(
                f"Production model deployed: version {production_model.version}"
            )
        elif current_metric >= 0.610:
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Staging",
                archive_existing_versions=True,
            )
            logger.info(f"Candidate model registered: version {current_model.version}")
        else:
            # Run 정보 가져오기
            run_info = client.get_run(current_model.run_id)
            # 아티팩트 경로 추출
            artifact_uri = run_info.info.artifact_uri
            # MinIO에서 아티팩트 삭제

            parsed_uri = urlparse(artifact_uri)
            bucket_name = parsed_uri.netloc
            artifact_path = parsed_uri.path.lstrip("/")

            s3 = boto3.client(
                "s3",
                endpoint_url="10.178.0.2:9000",  # MinIO 엔드포인트
                aws_access_key_id="mlflow_admin",
                aws_secret_access_key="mlflow_admin",
            )

            # 아티팩트 목록 가져오기
            objects_to_delete = s3.list_objects_v2(
                Bucket=bucket_name, Prefix=artifact_path
            )
            delete_keys = [
                {"Key": obj["Key"]} for obj in objects_to_delete.get("Contents", [])
            ]

            if delete_keys:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
                logger.info(f"Artifacts deleted from MinIO: {artifact_path}")
            else:
                logger.info("Artifacts not found.")

    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
