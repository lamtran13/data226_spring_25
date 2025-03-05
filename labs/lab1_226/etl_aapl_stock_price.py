from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import requests

def return_snowflake_conn():
    """Returns a Snowflake cursor connection."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Extract Task
@task
def extract(url):
    """Fetches stock price data from Alpha Vantage API."""
    r = requests.get(url)
    data = r.json()
    return data

# Transform Task
@task
def transform(data):
    """Transforms raw API data into a structured format."""
    results = []
    for d in data["Time Series (Daily)"]:
        stock_info = data["Time Series (Daily)"][d]
        stock_info['date'] = d  
        stock_info['symbol'] = 'AAPL'  
        results.append(stock_info)
    return results

# Load Task
@task
def load(results, target_table):
    """Loads transformed data into Snowflake."""
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")

        # Ensure the table exists
        cur.execute(f"""
        CREATE OR REPLACE TABLE {target_table}(
            date DATE PRIMARY KEY,
            symbol VARCHAR(10),
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT
        )""")

        for i in results:
            date = datetime.strptime(i['date'], '%Y-%m-%d').date()  
            symbol = i['symbol']
            open_price = float(i['1. open'])
            high_price = float(i['2. high'])
            low_price = float(i['3. low'])
            close_price = float(i['4. close'])
            volume = int(i['5. volume'])

            sql = """INSERT INTO dev.raw.aapl_price (date, symbol, open, high, low, close, volume)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cur.execute(sql, (date, symbol, open_price, high_price, low_price, close_price, volume))

        cur.execute("COMMIT;")
        print(" Data successfully loaded into Snowflake.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f" Error loading data: {e}")
        raise e

# Define Airflow DAG
with DAG(
    dag_id='aapl_stock_price',
    start_date=datetime(2025, 3, 4),
    catchup=False,
    tags=['ETL'],
    schedule='0 14 * * 1-5'  
) as dag:
    
    target_table = 'dev.raw.aapl_price'
    
    url = Variable.get('aapl_url')

    extract_task = extract(url)
    transform_task = transform(extract_task)
    load_task = load(transform_task, target_table)

    extract_task >> transform_task >> load_task
