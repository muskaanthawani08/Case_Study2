import snowflake.connector
import os
import pandas as pd
import logging
from io import StringIO

def load_to_snowflake(ti):
    df = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_join', key='cleaned_data')))
    df = df.fillna({'quantity': 0, 'rating': 0})

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    with open("sql/load_to_snowflake.sql", "r") as f:
        create_sql = f.read()
    cur.execute(create_sql)

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO daily_summary (product_id, quantity, rating) VALUES (%s, %s, %s)",
            (row['product_id'], row['quantity'], row['rating'])
        )

    conn.commit()
    cur.close()
    conn.close()

