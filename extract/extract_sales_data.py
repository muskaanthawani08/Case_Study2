import snowflake.connector
import os
import pandas as pd
import logging

def extract_sales_data(ti):
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
        cur.close()
        conn.close()

        sales.columns = map(str.lower, sales.columns)
        sales['product_id'] = sales['product_id'].astype(str)
        sales['user_id'] = sales['user_id'].astype(str)
        ti.xcom_push(key='sales_data', value=sales.to_json())
    except Exception as e:
        logging.error(f"Sales data extraction failed: {e}")
        raise
