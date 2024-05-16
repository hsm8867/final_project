from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from module import get_data


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def movie_pipeline():
    start_task = EmptyOperator(task_id="start_task")
    train_task = PythonOperator(
        task_id="train_task",
        python_callable=get_data.get,
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> train_task >> end_task


movie_pipeline()