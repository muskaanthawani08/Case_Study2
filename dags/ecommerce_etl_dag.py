from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from extract.extract_product_data import extract_product_data
from extract.extract_sales_data import extract_sales_data
from extract.extract_feedback_data import extract_feedback_data
from transformations.transform_logic import transform_and_join, run_data_quality_checks
from load.load_to_snowflake import load_to_snowflake
from alerts.send_alert import send_alert

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1
}

dag = DAG(
    dag_id='ecommerce_etl_pipeline_modular',
    default_args=default_args,
    schedule=None,
    catchup=False
)

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

extract_product = PythonOperator(task_id='extract_product_data', python_callable=extract_product_data, dag=dag)
extract_sales = PythonOperator(task_id='extract_sales_data', python_callable=extract_sales_data, dag=dag)
extract_feedback = PythonOperator(task_id='extract_feedback_data', python_callable=extract_feedback_data, dag=dag)

transform = PythonOperator(task_id='transform_and_join', python_callable=transform_and_join, dag=dag)
quality_check = BranchPythonOperator(task_id='run_data_quality_checks', python_callable=run_data_quality_checks, dag=dag)

load = PythonOperator(task_id='load_to_snowflake', python_callable=load_to_snowflake, dag=dag)
alert = PythonOperator(task_id='send_alert', python_callable=send_alert, dag=dag)

start >> extract_product >> extract_sales >> extract_feedback >> transform >> quality_check
quality_check >> load >> end
quality_check >> alert >> end
