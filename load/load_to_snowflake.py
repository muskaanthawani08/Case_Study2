def load_to_snowflake(ti):
    df = pd.read_json(StringIO(ti.xcom_pull(task_ids='transform_and_join', key='cleaned_data')))
    df = df.fillna({'quantity': 0, 'rating': 0})
    
    # Convert and sanitize date column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df[df['date'].notna()]
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Prepare list of tuples for executemany
    insert_data = df[['product_id', 'quantity', 'rating', 'date']].values.tolist()

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
            rating FLOAT,
            date DATE
        );
    """)

    cur.executemany(
        "INSERT INTO daily_summary (product_id, quantity, rating, date) VALUES (%s, %s, %s, %s)",
        insert_data
    )

    conn.commit()
    cur.close()
    conn.close()
