import torch
import torch.nn as nn

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

USERNAME = "postgres"
PASSWORD = "mlecourse"
HOST = "34.64.174.26"
PORT = "5432"  # Default port for PostgreSQL
DATABASE = "raw"
ENGINE_PATH = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
engine = create_engine(ENGINE_PATH)


def get(**context):
    query = "SELECT * FROM data.movies"

    data = pd.read_sql(query, engine)
