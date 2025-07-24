import pandas as pd
import logging
from io import StringIO

def transform_and_join(ti):
    sales = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_sales_data', key='sales_data')))
    feedback = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_feedback_data', key='feedback_data')))
    product = pd.read_json(StringIO(ti.xcom_pull(task_ids='extract_product_data', key='product_data')))

    for df in [sales, feedback, product]:
        df['product_id'] = df['product_id'].astype(str)

    for df in [sales, feedback]:
        df['user_id'] = df['user_id'].astype(str)

    sales.dropna(subset=['product_id', 'user_id'], inplace=True)
    feedback.dropna(subset=['product_id', 'user_id'], inplace=True)
    product.dropna(subset=['product_id'], inplace=True)

    sales = sales[sales['product_id'].str.startswith('SKU')]
    feedback = feedback[feedback['product_id'].str.startswith('SKU')]
    product = product[product['product_id'].str.startswith('SKU')]

    sales = sales[sales['product_id'].isin(product['product_id'])]
    feedback = feedback[feedback['rating'].between(1, 5)]

    sales_product = sales.merge(product, on='product_id', how='left')
    full_df = sales_product.merge(feedback, on=['product_id', 'user_id'], how='left')

    result = full_df.groupby('product_id').agg({'quantity': 'sum', 'rating': 'mean'}).reset_index()
    result['rating'] = result['rating'].fillna(0)

    ti.xcom_push(key='cleaned_data', value=result.to_json())

def run_data_quality_checks(ti):
    result = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_join', key='cleaned_data')))
    if result['rating'].between(0, 5).all():
        return 'load_to_snowflake'
    else:
        logging.warning("Data quality check failed")
        return 'send_alert'
