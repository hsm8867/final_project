from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd


def load(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    stmt = """
            SELECT m.*, mi.repgenrenm
            FROM data.movies m
            LEFT JOIN data.movie_info mi
            ON m.moviecd = mi.moviecd;
 
            """

    data = pd.read_sql(stmt, conn)

    
    return data
