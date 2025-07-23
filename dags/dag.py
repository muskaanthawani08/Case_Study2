from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import snowflake.connector
import requests
from datetime import datetime, timedelta

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

    sales = pd.DataFrame(cur.execute("SELECT * FROM sales_data").fetchall(),
                         columns=[col[0] for col in cur.description])
    feedback = pd.DataFrame(cur.execute("SELECT * FROM feedback_data").fetchall(),
                            columns=[col[0] for col in cur.description])

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
    sales = pd.read_json(ti.xcom_pull(task_ids='extract_snowflake_data', key='sales_data'))
    feedback = pd.read_json(ti.xcom_pull(task_ids='extract_snowflake_data', key='feedback_data'))
    product = pd.read_json(ti.xcom_pull(task_ids='fetch_product_data', key='product_data'))

    product['product_name'] = product['product_name'].str.title()
    product['category'] = product['category'].str.lower()

    df = sales.merge(product, on='product_id', how='left').merge(feedback, on='product_id', how='left')

    result = df.groupby('product_id').agg({
        'sales_amount': 'sum',
        'feedback_score': 'mean'
    }).reset_index()

    if result['product_id'].isnull().any():
        raise ValueError("Nulls in product_id")
    if not result['product_id'].isin(product['product_id']).all():
        raise ValueError("Sales contain unknown product_ids")
    if not result['feedback_score'].between(1, 5).all():
        raise ValueError("Feedback score out of expected range")

    ti.xcom_push(key='cleaned_data', value=result.to_json())

transform_data = PythonOperator(
    task_id='transform_and_validate_data',
    python_callable=transform_and_check,
    dag=dag
)

def load_to_snowflake(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='transform_and_validate_data', key='cleaned_data'))

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
            "INSERT INTO analytics_table (product_id, sales_amount, feedback_score) VALUES (%s, %s, %s)",
            (row['product_id'], row['sales_amount'], row['feedback_score'])
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
