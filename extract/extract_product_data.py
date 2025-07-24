import pandas as pd
import requests
import logging

def extract_product_data(ti):
    url = "https://retoolapi.dev/L8OKa9/product_data"
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df['name'] = df['name'].str.title()
        df['category'] = df['category'].str.lower()
        df['product_id'] = df['product_id'].astype(str)
        ti.xcom_push(key='product_data', value=df.to_json())
    except Exception as e:
        logging.error(f"Failed to fetch product data: {e}")
        raise
