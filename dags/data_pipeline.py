from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from dags.module import data_modules


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def data_receiving_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    boxoffice_task = PythonOperator(
        task_id="box_office",
        python_callable=data_modules.update_daily,
    )

    movieinfo_task = PythonOperator(
        task_id="movie_info",
        python_callable=data_modules.update_movie_info,
    )

    end_task = EmptyOperator(task_id="end_task")

    start_task >> [boxoffice_task, movieinfo_task] >> end_task


data_receiving_pipeline()
