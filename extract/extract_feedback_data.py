import snowflake.connector
import os
import pandas as pd
import logging

def extract_feedback_data(ti):
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
        feedback = pd.DataFrame(cur.execute("SELECT * FROM feedback_data").fetchall(),
                                columns=[col[0] for col in cur.description])
        cur.close()
        conn.close()

        feedback.columns = map(str.lower, feedback.columns)
        feedback['product_id'] = feedback['product_id'].astype(str)
        feedback['user_id'] = feedback['user_id'].astype(str)
        ti.xcom_push(key='feedback_data', value=feedback.to_json())
    except Exception as e:
        logging.error(f"Feedback data extraction failed: {e}")
        raise
