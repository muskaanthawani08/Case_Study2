from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import snowflake.connector
import requests
from datetime import datetime, timedelta
from io import StringIO  # ✅ For safe read_json

load_dotenv()
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1
}

dag = DAG(
    dag_id='ecommerce_etl_pipeline_v2',
    default_args=default_args,
    schedule=None,
    catchup=False
)

start = EmptyOperator(task_id='start', dag=dag)

def fetch_product_data(ti):
    url = "https://retoolapi.dev/L8OKa9/product_data"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API failed with status {response.status_code}")
    df = pd.DataFrame(response.json())

    # Normalize columns
    if 'name' in df.columns:
        df['name'] = df['name'].str.title()
    if 'category' in df.columns:
        df['category'] = df['category'].str.lower()
    if 'product_id' in df.columns:
        df['product_id'] = df['product_id'].astype(str)

    ti.xcom_push(key='product_data', value=df.to_json())

fetch_product = PythonOperator(
    task_id='fetch_product_data',
    python_callable=fetch_product_data,
    dag=dag
)

def extract_snowflake(ti):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    # Extract from Snowflake
    sales = pd.DataFrame(cur.execute("SELECT * FROM sales_data").fetchall(),
                         columns=[col[0] for col in cur.description])
    feedback = pd.DataFrame(cur.execute("SELECT * FROM feedback_data").fetchall(),
                            columns=[col[0] for col in cur.description])

    # Log columns
    logging.info("Sales columns: %s", sales.columns.tolist())
    logging.info("Feedback columns: %s", feedback.columns.tolist())

    # Rename if needed
    sales.rename(columns=lambda x: x.lower(), inplace=True)
    feedback.rename(columns=lambda x: x.lower(), inplace=True)

    # Ensure key columns exist
    for df in [sales, feedback]:
        if 'product_id' in df.columns:
            df['product_id'] = df['product_id'].astype(str)
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(str)

    ti.xcom_push(key='sales_data', value=sales.to_json())
    ti.xcom_push(key='feedback_data', value=feedback.to_json())

    cur.close()
    conn.close()

extract_snowflake_data = PythonOperator(
    task_id='extract_snowflake_data',
    python_callable=extract_snowflake,
    dag=dag
)

def transform_and_check(ti):
    # Load from XCom safely
    sales = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='sales_data')))
    feedback = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='feedback_data')))
    product = pd.read_json(StringIO(ti.xcom_pull(task_ids='fetch_product_data', key='product_data')))

    # Log column headers
    logging.info("Sales: %s", sales.columns.tolist())
    logging.info("Feedback: %s", feedback.columns.tolist())
    logging.info("Product: %s", product.columns.tolist())

    # Check key columns
    for df_name, df in [('sales', sales), ('feedback', feedback), ('product', product)]:
        if 'product_id' not in df.columns:
            raise KeyError(f"'product_id' missing in {df_name}")
        df['product_id'] = df['product_id'].astype(str)

    for df_name, df in [('sales', sales), ('feedback', feedback)]:
        if 'user_id' not in df.columns:
            raise KeyError(f"'user_id' missing in {df_name}")
        df['user_id'] = df['user_id'].astype(str)

    # Join sales with product
    sales_product = sales.merge(product, on='product_id', how='left')

    # Join feedback
    full_df = sales_product.merge(feedback, on=['product_id', 'user_id'], how='left')

    # Aggregation
    if 'quantity' not in full_df.columns or 'rating' not in full_df.columns:
        raise KeyError("Missing 'quantity' or 'rating' column in joined data.")

    result = full_df.groupby('product_id').agg({
        'quantity': 'sum',
        'rating': 'mean'
    }).reset_index()

    # Log first few rows of transformed data
    logging.info("Transformed Summary:\n%s", result.head())

    # Validate ratings
    if not result['rating'].between(0, 5).all():
        logging.warning("⚠ Some feedback ratings fall outside 0–5 range.")

    ti.xcom_push(key='cleaned_data', value=result.to_json())

    ti.xcom_push(key='cleaned_data', value=result.to_json())

transform_data = PythonOperator(
    task_id='transform_and_validate_data',
    python_callable=transform_and_check,
    dag=dag
)

def load_to_snowflake(ti):
    df = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_validate_data', key='cleaned_data')))

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO analytics_table (product_id, quantity, rating) VALUES (%s, %s, %s)",
            (row['product_id'], row['quantity'], row['rating'])
        )

    conn.commit()
    cur.close()
    conn.close()

load_data = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

start >> fetch_product >> extract_snowflake_data >> transform_data >> load_data
