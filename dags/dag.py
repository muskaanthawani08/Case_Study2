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
    df['name'] = df['name'].str.title()
    df['category'] = df['category'].str.lower()
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

    sales = pd.DataFrame(cur.execute("SELECT * FROM sales_data").fetchall(),
                         columns=[col[0] for col in cur.description])
    logging.info("Sales columns: %s", sales.columns.tolist())

    feedback = pd.DataFrame(cur.execute("SELECT * FROM feedback_data").fetchall(),
                            columns=[col[0] for col in cur.description])
    logging.info("Feedback columns: %s", feedback.columns.tolist())

    # Rename if necessary
    if 'ProductID' in sales.columns:
        sales.rename(columns={'ProductID': 'product_id'}, inplace=True)
    if 'ProductID' in feedback.columns:
        feedback.rename(columns={'ProductID': 'product_id'}, inplace=True)

    if 'product_id' in sales.columns:
        sales['product_id'] = sales['product_id'].astype(str)
    if 'user_id' in sales.columns:
        sales['user_id'] = sales['user_id'].astype(str)

    if 'product_id' in feedback.columns:
        feedback['product_id'] = feedback['product_id'].astype(str)
    if 'user_id' in feedback.columns:
        feedback['user_id'] = feedback['user_id'].astype(str)

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

    sales['product_id'] = sales['product_id'].astype(str)
    feedback['product_id'] = feedback['product_id'].astype(str)
    product['product_id'] = product['product_id'].astype(str)

    sales['user_id'] = sales['user_id'].astype(str)
    feedback['user_id'] = feedback['user_id'].astype(str)

    # Join sales with product
    sales_with_product = sales.merge(product, on='product_id', how='left')

    # Join with feedback
    full_df = sales_with_product.merge(feedback, on=['product_id', 'user_id'], how='left')

    # Aggregate quantity and rating
    result = full_df.groupby('product_id').agg({
        'quantity': 'sum',
        'rating': 'mean'
    }).reset_index()

    if result['product_id'].isnull().any():
        raise ValueError("Missing product_id after join.")
    if 'rating' in result.columns and not result['rating'].between(0, 5).all():
        logging.warning("Some ratings are outside expected range (0–5).")

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
