from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.simple_http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import snowflake.connector
import json

load_dotenv()
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Start Task
start = EmptyOperator(task_id='start', dag=dag)

# Check REST API
check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='product_api_connection',
    endpoint='L8OKa9/product_data',
    response_check=lambda response: "product_name" in response.text,
    poke_interval=10,
    timeout=60,
    dag=dag
)

# Fetch Product Data
fetch_product_data = SimpleHttpOperator(
    task_id='fetch_product_data',
    http_conn_id='product_api_connection',
    endpoint='L8OKa9/product_data',
    method='GET',
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag
)

# Extract Snowflake Data
def extract_snowflake(**kwargs):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    sales_query = "SELECT * FROM sales_data"
    feedback_query = "SELECT * FROM feedback_data"

    sales_df = pd.DataFrame(cur.execute(sales_query).fetchall(), columns=[col[0] for col in cur.description])
    feedback_df = pd.DataFrame(cur.execute(feedback_query).fetchall(), columns=[col[0] for col in cur.description])

    sales_df.to_pickle('/tmp/sales.pkl')
    feedback_df.to_pickle('/tmp/feedback.pkl')

    cur.close()
    conn.close()

extract_data_snowflake = PythonOperator(
    task_id='extract_snowflake_data',
    python_callable=extract_snowflake,
    dag=dag
)

# Transform & Validate
def transform_and_check():
    sales = pd.read_pickle('/tmp/sales.pkl')
    feedback = pd.read_pickle('/tmp/feedback.pkl')
    product = pd.read_json('/tmp/fetched_product_data.json')

    product['product_name'] = product['product_name'].str.title()
    product['category'] = product['category'].str.lower()

    df = sales.merge(product, on='product_id', how='left').merge(feedback, on='product_id', how='left')

    result = df.groupby('product_id').agg({
        'sales_amount': 'sum',
        'feedback_score': 'mean'
    }).reset_index()

    result.to_pickle('/tmp/cleaned_data.pkl')

    # Quality checks
    if result['product_id'].isnull().any():
        raise ValueError("Nulls in product_id")

    if not result['product_id'].isin(product['product_id']).all():
        raise ValueError("Sales contain unknown product_ids")

    if not result['feedback_score'].between(1, 5).all():
        raise ValueError("Feedback score out of expected range")

transform_data = PythonOperator(
    task_id='transform_and_validate_data',
    python_callable=transform_and_check,
    dag=dag
)

# Load to Snowflake
def load_to_snowflake():
    df = pd.read_pickle('/tmp/cleaned_data.pkl')
    
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

# Slack Alert
slack_alert = SlackWebhookOperator(
    task_id='slack_alert_failure',
    http_conn_id='slack_connection',
    message="⚠️ Data quality check failed in ecommerce_etl_pipeline",
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)

# DAG Dependencies
start >> check_api >> fetch_product_data >> extract_data_snowflake >> transform_data >> load_data
transform_data >> slack_alert
