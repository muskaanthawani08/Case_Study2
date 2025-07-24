from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import pandas as pd
import os
import logging
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime, timedelta
from io import StringIO

# Load environment variables and configure logging
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

# ✅ Step 1: Use SimpleHttpOperator to fetch product data
fetch_product = SimpleHttpOperator(
    task_id='fetch_product_data',
    http_conn_id='product_api',
    endpoint='L8OKa9/product_data',
    method='GET',
    response_filter=lambda response: response.text,
    log_response=True,
    do_xcom_push=True,
    dag=dag
)

# ✅ Step 2: Extract data from Snowflake
def extract_snowflake(ti):
    try:
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

        logging.info("Sales columns: %s", sales.columns.tolist())
        logging.info("Feedback columns: %s", feedback.columns.tolist())

        sales.rename(columns=lambda x: x.lower(), inplace=True)
        feedback.rename(columns=lambda x: x.lower(), inplace=True)

        for df in [sales, feedback]:
            if 'product_id' in df.columns:
                df['product_id'] = df['product_id'].astype(str)
            if 'user_id' in df.columns:
                df['user_id'] = df['user_id'].astype(str)

        ti.xcom_push(key='sales_data', value=sales.to_json())
        ti.xcom_push(key='feedback_data', value=feedback.to_json())

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Snowflake extraction failed: {e}")
        raise

extract_snowflake_data = PythonOperator(
    task_id='extract_snowflake_data',
    python_callable=extract_snowflake,
    dag=dag
)

# ✅ Step 3: Transformation and Aggregation
def transform_and_check(ti):
    try:
        sales = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='sales_data')))
        feedback = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='feedback_data')))
        product_raw = ti.xcom_pull(task_ids='fetch_product_data')
        product = pd.read_json(product_raw)

        logging.info("Sales: %s", sales.columns.tolist())
        logging.info("Feedback: %s", feedback.columns.tolist())
        logging.info("Product: %s", product.columns.tolist())

        for df_name, df in [('sales', sales), ('feedback', feedback), ('product', product)]:
            if 'product_id' not in df.columns:
                raise KeyError(f"'product_id' missing in {df_name}")
            df['product_id'] = df['product_id'].astype(str)

        for df_name, df in [('sales', sales), ('feedback', feedback)]:
            if 'user_id' not in df.columns:
                raise KeyError(f"'user_id' missing in {df_name}")
            df['user_id'] = df['user_id'].astype(str)

        product['name'] = product['name'].str.title()
        product['category'] = product['category'].str.lower()

        sales_product = sales.merge(product, on='product_id', how='left')
        full_df = sales_product.merge(feedback, on=['product_id', 'user_id'], how='left')

        if 'quantity' not in full_df.columns or 'rating' not in full_df.columns:
            raise KeyError("Missing 'quantity' or 'rating' column in joined data.")

        result = full_df.groupby('product_id').agg({
            'quantity': 'sum',
            'rating': 'mean'
        }).reset_index()

        result['rating'] = result['rating'].fillna(0)

        logging.info("Transformed Summary:\n%s", result.head())

        if not result['rating'].between(0, 5).all():
            logging.warning("⚠ Some feedback ratings fall outside 0–5 range.")

        ti.xcom_push(key='cleaned_data', value=result.to_json())
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

transform_data = PythonOperator(
    task_id='transform_and_validate_data',
    python_callable=transform_and_check,
    dag=dag
)

# ✅ Step 4: Load into Snowflake
def load_to_snowflake(ti):
    try:
        df = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_validate_data', key='cleaned_data')))

        df = df.fillna(value={"quantity": 0, "rating": 0})
        logging.info("Cleaned DataFrame before insert:\n%s", df.head())

        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analytics_table (
                product_id STRING,
                quantity NUMBER,
                rating FLOAT
            );
        """)

        for _, row in df.iterrows():
            cur.execute(
                "INSERT INTO analytics_table (product_id, quantity, rating) VALUES (%s, %s, %s)",
                (row['product_id'], row['quantity'], row['rating'])
            )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to load data into Snowflake: {e}")
        raise

load_data = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

start >> fetch_product >> extract_snowflake_data >> transform_data >> load_data
