""" 
Name: Lam Tran
Class: Data 226
Due Date: Thursday, March 6th, 2025
Assignment 5: Porting homework #4 to Airflow
"""

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import json

# Function to establish Snowflake connection
def return_snowflake_conn():
    """
    Returns a cursor object for executing queries in Snowflake.
    Uses Airflow's SnowflakeHook to retrieve the connection.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Extract Task
@task
def extract(url):
    """
    Extract stock data from the Alpha Vantage API.

    Args:
        url (str): The API endpoint to fetch stock data.

    Returns:
        dict: A dictionary containing stock data for multiple dates.
    """
    print(f"Extracting data from URL: {url}")  # Debugging print statement
    
    # Send GET request to the API
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    
    # Parse JSON response
    data = response.json()
    
    # Validate API response
    if "Time Series (Daily)" not in data:
        raise Exception("Invalid API response, check API key or request limits")
    
    return data["Time Series (Daily)"]  # Return only the daily time series data

# Transform Task
@task
def transform(data):
    """
    Transform the raw API JSON response into structured records.

    Args:
        data (dict): Dictionary containing stock data with dates as keys.

    Returns:
        list: A list of dictionaries, each representing a stock record.
    """
    records = []
    
    # Iterate through each date entry in the dataset
    for date, values in data.items():
        record = {
            "symbol": "TSLA",  # Hardcoded symbol for Tesla stock
            "date": date,  # Extract date
            "open": float(values["1. open"]),  # Convert open price to float
            "high": float(values["2. high"]),  # Convert high price to float
            "low": float(values["3. low"]),  # Convert low price to float
            "close": float(values["4. close"]),  # Convert close price to float
            "volume": int(values["5. volume"])  # Convert volume to integer
        }
        records.append(record)  # Append transformed record to the list
    
    return records  # Return transformed data

# Load Task
@task
def load(records, target_table):
    """
    Load the transformed data into a Snowflake table.

    Args:
        records (list): A list of transformed stock records.
        target_table (str): The name of the Snowflake table to insert data into.
    """
    cur = return_snowflake_conn()  # Get Snowflake connection cursor
    
    try:
        # Begin transaction
        cur.execute("BEGIN;")
        
        # Create table if it does not exist
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol STRING,
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT
            );
        """)

        # Remove old data before inserting new to avoid duplication
        cur.execute(f"DELETE FROM {target_table} WHERE symbol = 'TSLA';")

        # Insert new records into Snowflake table
        for r in records:
            sql = f"""
                INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                VALUES ('{r["symbol"]}', '{r["date"]}', {r["open"]}, {r["high"]}, {r["low"]}, {r["close"]}, {r["volume"]})
            """
            cur.execute(sql)
        
        # Commit transaction after successful insert
        cur.execute("COMMIT;")
        print(f"Successfully inserted {len(records)} records into {target_table}")
    
    except Exception as e:
        # Rollback transaction in case of failure
        cur.execute("ROLLBACK;")
        print(f"Load failed: {e}")
        raise

# Define the DAG
with DAG(
    dag_id='etl_data_tsla',  # DAG name
    start_date=datetime(2025, 3, 2),  # Start date for DAG execution
    catchup=False,  # Disable catchup to prevent backfilling
    tags=['ETL'],  # Tag for better categorization in Airflow UI
    schedule_interval='0 6 * * 1-5'  # Schedule: Runs at 6 AM, Monday to Friday
) as dag:

    target_table = "STOCK_DATA_HW4.RAW.STOCK_DATA"  # Snowflake target table
    
    # Fetch the full API URL from Airflow variables 
    url = Variable.get("alpha_vantage_api_url")

    # Define ETL Workflow
    extracted_data = extract(url)  # Extract data from API
    transformed_data = transform(extracted_data)  # Transform extracted data
    load(transformed_data, target_table)  # Load data into Snowflake
