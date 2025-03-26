from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    """
    Returns a cursor object for executing queries in Snowflake.
    Uses Airflow's SnowflakeHook to retrieve the connection.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_raw_tables():
    cursor = return_snowflake_conn()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dev.raw.user_session_channel (
            userId int NOT NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dev.raw.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp
        );
    """)

@task
def populate_raw_tables():
    cursor = return_snowflake_conn()
    
    cursor.execute("""
        CREATE OR REPLACE STAGE dev.raw.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """)

    cursor.execute("""
        COPY INTO dev.raw.user_session_channel
        FROM @dev.raw.blob_stage/user_session_channel.csv;
    """)

    cursor.execute("""
        COPY INTO dev.raw.session_timestamp
        FROM @dev.raw.blob_stage/session_timestamp.csv;
    """)

with DAG(
    dag_id='etl_raw_tables_dag',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    tags=['ETL', 'Snowflake'],
) as dag:

    # Task chaining
    create_raw_tables_task = create_raw_tables()
    populate_raw_tables_task = populate_raw_tables()

    create_raw_tables_task >> populate_raw_tables_task
