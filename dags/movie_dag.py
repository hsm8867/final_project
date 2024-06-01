from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from module import train
from module.model_version import create_model_version
from module.transit_model import transition_model_stage


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
        python_callable=train.train_fn,
        op_kwargs={"experiment_name": "movie_model"},
    )
    model_create_task = PythonOperator(
        task_id="model_create_task",
        python_callable=create_model_version,
        op_kwargs={"model_name": "movie_model"},
    )
    model_transition_task = PythonOperator(
        task_id="model_transition_task",
        python_callable=transition_model_stage,
        op_kwargs={"model_name": "movie_model"},
    )

    end_task = EmptyOperator(task_id="end_task")

    start_task >> train_task >> model_create_task >> model_transition_task >> end_task


movie_pipeline()
