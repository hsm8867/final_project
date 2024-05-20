# load_data.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def load(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    stmt = """
            SELECT *
              FROM data.movies
            """

    data = pd.read_sql(stmt, conn)

    # 데이터프레임을 딕셔너리로 변환하여 반환
    return data.to_dict(orient="list")
