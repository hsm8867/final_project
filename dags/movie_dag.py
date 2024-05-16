from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from module import get_data, preprocess_data, train, load_data


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def movie_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    @task
    def load_data_task(**kwargs):
        data = load_data.load(**kwargs)
        return data

    @task
    def preprocess_task(data_dict):
        processed_data = preprocess_data.preprocess(data_dict)
        return processed_data

    @task
    def train_task(processed_data):
        train.train_fn(processed_data)

    end_task = EmptyOperator(task_id="end_task")

    data = load_data_task()
    processed_data = preprocess_task(data)

    start_task >> data >> processed_data >> train_task(processed_data) >> end_task


movie_pipeline()
