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

    
    return data
