import snowflake.connector
import os
import pandas as pd
import logging
from io import StringIO

def load_to_snowflake(ti):
    # Pull JSON string from XCom and parse to DataFrame
    json_str = ti.xcom_pull(task_ids='transform_and_join', key='cleaned_data')
    df = pd.read_json(StringIO(json_str))

    # Fill missing numeric values and sanitize date column
    df = df.fillna({'quantity': 0, 'rating': 0})
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'].notna()]
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')  # Format as string for SQL

    # Convert DataFrame rows to list of tuples
    insert_data = df[['product_id', 'quantity', 'rating', 'date']].values.tolist()

    # Connect to Snowflake using environment variables
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    try:
        # Create table with properly quoted column name
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                product_id STRING,
                quantity NUMBER,
                rating FLOAT,
                "date" TIMESTAMP
            );
        """)

        # Use single quotes around SQL string to allow double quotes in column names
        insert_stmt = 'INSERT INTO daily_summary (product_id, quantity, rating, "date") VALUES (%s, %s, %s, %s)'
        cur.executemany(insert_stmt, insert_data)

        conn.commit()
    finally:
        cur.close()
        conn.close()
