from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dags.module.delay import delay_start_10
from dags.module.create_table import create_table_fn
from dags.module.save_raw_data import (
    collect_and_load_data_fn,
)
from dags.module.preprocess import preprocess_data_fn
from airflow.models import Variable
import asyncio


def sync_collect_and_load_data():
    asyncio.run(collect_and_load_data_fn())


def sync_preprocess_data_fn(**context):
    asyncio.run(preprocess_data_fn(**context))


@dag(
    dag_id="data_pipeline",
    schedule_interval="*/5 * * * *",  # 5분마다 실행
    start_date=datetime(2024, 10, 23, 0, 0),
    catchup=False,
    default_args={
        "owner": "admin",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["UPBIT_BTC_KRW"],
)
def data_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    delay_task = PythonOperator(
        task_id="delay_10seconds",
        python_callable=delay_start_10,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table_fn,
    )

    save_data_task = PythonOperator(
        task_id="save_raw_data_from_UPBIT_API",
        python_callable=sync_collect_and_load_data,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    (
        start_task
        >> delay_task
        >> create_table_task
        >> save_data_task
        >> preprocess_task
        >> end_task
    )


data_pipeline()
