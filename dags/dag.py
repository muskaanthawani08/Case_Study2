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
 
def fetch_product_data(ti):
    try:
        url = "https://retoolapi.dev/L8OKa9/product_data"
        response = requests.get(url)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
 
        if 'name' in df.columns:
            df['name'] = df['name'].str.title()
        if 'category' in df.columns:
            df['category'] = df['category'].str.lower()
        if 'product_id' in df.columns:
            df['product_id'] = df['product_id'].astype(str)
 
        ti.xcom_push(key='product_data', value=df.to_json())
    except Exception as e:
        logging.error(f"Failed to fetch or process product data: {e}")
        raise
 
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
 
def transform_and_check(ti):
    try:
        # Extract data from XComs
        sales = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='sales_data')))
        feedback = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_snowflake_data', key='feedback_data')))
        product = pd.read_json(StringIO(ti.xcom_pull(task_ids='fetch_product_data', key='product_data')))

        # Sanitize 'product_id' and 'user_id'
        for df_name, df in [('sales', sales), ('feedback', feedback), ('product', product)]:
            if 'product_id' not in df.columns:
                raise KeyError(f"'product_id' missing in {df_name}")
            df['product_id'] = df['product_id'].astype(str)

        for df_name, df in [('sales', sales), ('feedback', feedback)]:
            if 'user_id' not in df.columns:
                raise KeyError(f"'user_id' missing in {df_name}")
            df['user_id'] = df['user_id'].astype(str)

        # Drop rows with null primary keys
        sales.dropna(subset=['product_id', 'user_id'], inplace=True)
        feedback.dropna(subset=['product_id', 'user_id'], inplace=True)
        product.dropna(subset=['product_id'], inplace=True)
        logging.info(f"Primary key validation complete. Remaining rows: sales={sales.shape[0]}, feedback={feedback.shape[0]}, product={product.shape[0]}")

        # Filter only SKU-prefixed product IDs
        for df_name, df in [('sales', sales), ('feedback', feedback), ('product', product)]:
            df = df[df['product_id'].str.startswith('SKU')]
            logging.info(f"{df_name} filtered to SKU-prefixed product_id. Remaining: {df.shape[0]}")
            if df_name == 'sales': sales = df
            elif df_name == 'feedback': feedback = df
            elif df_name == 'product': product = df

        # Enforce referential integrity: only keep sales with valid product_id
        sales = sales[sales['product_id'].isin(product['product_id'])]
        logging.info(f"Referential integrity enforced. Valid sales records: {sales.shape[0]}")

        # Filter feedback scores between 1–5
        if 'rating' not in feedback.columns:
            raise KeyError("'rating' column missing in feedback data.")
        feedback = feedback[feedback['rating'].between(1, 5, inclusive='both')]
        logging.info(f"Feedback score range validated (1–5). Remaining feedback records: {feedback.shape[0]}")

        # Merge sales with product and feedback data
        sales_product = sales.merge(product, on='product_id', how='left')
        full_df = sales_product.merge(feedback, on=['product_id', 'user_id'], how='left')

        # Confirm required columns exist
        if 'quantity' not in full_df.columns or 'rating' not in full_df.columns:
            raise KeyError("Missing 'quantity' or 'rating' column in joined data.")

        # Group and aggregate
        result = full_df.groupby('product_id').agg({
            'quantity': 'sum',
            'rating': 'mean'
        }).reset_index()

        # Replace NaN ratings with 0
        result['rating'] = result['rating'].fillna(0)

        # Final sanity check for ratings
        if not result['rating'].between(0, 5).all():
            logging.warning("Some aggregated ratings fall outside 0–5 range.")

        # Log and push results
        logging.info("Final Transformed Summary:\n%s", result.head())
        ti.xcom_push(key='cleaned_data', value=result.to_json())

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

 
def load_to_snowflake(ti):
    try:
        df = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_validate_data', key='cleaned_data')))
 
        # Replace NaNs with None and default quantity to 0
        df = df.fillna(value={"quantity": 0, "rating": 0})
 
        # Preview inserted data
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
            CREATE TABLE IF NOT EXISTS daily_summary (
                product_id STRING,
                quantity NUMBER,
                rating FLOAT
            );
        """)
 
        for _, row in df.iterrows():
            cur.execute(
                "INSERT INTO daily_summary (product_id, quantity, rating) VALUES (%s, %s, %s)",
                (row['product_id'], row['quantity'], row['rating'])
            )
 
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Failed to load data into Snowflake: {e}")
        raise
 
fetch_product = PythonOperator(
    task_id='fetch_product_data', 
    python_callable=fetch_product_data, 
    dag=dag)

extract_snowflake_data = PythonOperator(
    task_id='extract_snowflake_data', 
    python_callable=extract_snowflake, 
    dag=dag)

transform_data = PythonOperator(
    task_id='transform_and_validate_data', 
    python_callable=transform_and_check, 
    dag=dag)

load_data = PythonOperator(
    task_id='load_to_snowflake', 
    python_callable=load_to_snowflake, 
    dag=dag)
 
start >> fetch_product >> extract_snowflake_data >> transform_data >> load_data