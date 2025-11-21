# app/data/datasets.py
import pandas as pd
from app.data.db import connect_database

def get_all_datasets():
    conn = connect_database()
    df = pd.read_sql_query("SELECT * FROM datasets_metadata", conn)
    conn.close()
    return df