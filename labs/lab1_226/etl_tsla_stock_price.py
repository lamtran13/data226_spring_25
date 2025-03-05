from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()


# extract
@task
def extract(url):
  r = requests.get(url)
  data = r.json()
  return data

# transform
@task
def transform(data):
  results = []
  for d in data["Time Series (Daily)"]:
    stock_info = data["Time Series (Daily)"][d]
    stock_info['date'] = d
    results.append(stock_info)
    stock_info['symbol'] = 'TSLA'
  return results

@task
def load(cur, results, target_table):
  try:
    cur.execute("BEGIN;")
    cur.execute(f"""
    CREATE OR REPLACE TABLE {target_table}(
      date TIMESTAMP_NTZ PRIMARY KEY,
      symbol VARCHAR(10),
      open FLOAT,
      high FLOAT,
      low FLOAT,
      close FLOAT,
      volume INT
    )""")

    for i in results:
      date = i['date']
      symbol = i['symbol']
      open_price = i['1. open']
      high_price = i['2. high']
      low_price = i['3. low']
      close_price = i['4. close']
      volume = i['5. volume']


      sql = f"""
            INSERT INTO {target_table} (date, symbol, open, high, low, close, volume)
            VALUES (TO_TIMESTAMP_NTZ('{date}', 'YYYY-MM-DD'), '{symbol}', {open_price}, {high_price}, {low_price}, {close_price}, {volume})
            """
      cur.execute(sql)

    cur.execute("COMMIT;")

  except Exception as e:
    cur.execute("ROLLBACK;")
    print(e)
    raise e


with DAG(
    dag_id='etl_data_new_tsla',
    start_date=datetime(2025, 3, 2),  
    catchup=False,
    tags=['ETL'],
    schedule_interval='0 6 * * 1-5'  # Runs at 6 AM, Monday to Friday
) as dag:
    
    target_table = 'dev.raw.tsla_price'
    url = Variable.get('alpha_vantage_api_url')
    cur = return_snowflake_conn()

    # tasks ETL
    extract_task = extract(url)
    transform_task = transform(extract_task)
    load_task = load(cur, transform_task, target_table)
    
    #     # Fetch the full API URL (ensure this variable exists in Airflow)
    # url = Variable.get("alpha_vantage_api_url")

    # # ETL Workflow
    # extracted_data = extract(url)
    # transformed_data = transform(extracted_data)
    # load(transformed_data, target_table)