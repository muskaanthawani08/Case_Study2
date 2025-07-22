from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from datetime import timedelta, datetime
import os
import math
import pandas as pd
import logging
import mysql.connector
from dotenv import load_dotenv
import snowflake.connector as sf
 
# Load environment variables
load_dotenv()
 
# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
 
 
def download_data():
    try:
        conn = sf.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
 
        cur = conn.cursor()
 
        query = "SELECT * FROM daily_sales"
        cur.execute(query)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        df = pd.DataFrame(rows, columns=columns)
 
        logging.info("data preview:\n%s", df.head().to_string(index=False))  
 
        df.to_pickle('/tmp/daily_sales.pkl')
        logging.info(f"Fetched {len(df)} rows from Snowflake.")
        logging.info("data preview after pushing:\n%s", df.head().to_string(index=False))  
 
    except Exception as e:
        logging.error(f"Snowflake error: {e}")
        raise
 
    finally:
        cur.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
 
dag = DAG(
    dag_id='daily_sales_pipeline',
    default_args=default_args,
    catchup=False
)

download_task = PythonOperator(
    task_id='download_sales_data',
    python_callable=download_data,
    dag=dag
)