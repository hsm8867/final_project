from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from module import train, get_data


@dag(
    start_date=datetime(2024, 5, 15),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def fetch_and_upload():
    # Fetch weather data from api server
    weather_data = get_data.get_weather_data()
    # Upload data to the database
    get_data.upload_data(weather_data)


def weather_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    data_task = PythonOperator(
        task_id="data_task",
        python_callable=fetch_and_upload,
    )
    train_task = PythonOperator(
        task_id="train_task",
        python_callable=train.train_fn,
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> data_task >> train_task >> end_task


weather_pipeline()
