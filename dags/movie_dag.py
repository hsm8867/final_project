from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from module import get_data, preprocess_data


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def movie_pipeline():
    start_task = EmptyOperator(task_id="start_task")
    get_data_task = PythonOperator(
        task_id="get_data_task", python_callable=get_data.get
    )
    preprocess_task = PythonOperator(
        task_id="preprocess_task", python_callable=preprocess_data.preprocess
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> get_data_task >> preprocess_task >> end_task


movie_pipeline()
