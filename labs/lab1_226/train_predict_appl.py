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




@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
    - Generate predictions and store the results in `forecast_table`.
    - Union the predictions with historical data and create `final_table`.
    """
    try:
        # Run the forecasting function
        cur.execute(f"""
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
        """)

        # Store predictions in the table
        cur.execute(f"""
            CREATE OR REPLACE TABLE {forecast_table} AS 
            SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        """)

        # Create the final table by combining historical and forecasted data
        cur.execute(f"""
            CREATE OR REPLACE TABLE {final_table} AS
            SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
            UNION ALL
            SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table};
        """)

    except Exception as e:
        print(f"Error in predict task: {e}")
        raise



with DAG(
    dag_id = 'TrainPredict_AAPL',
    start_date = datetime(2025,3,4),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '30 14 * * 1-5'
) as dag:

    train_input_table = "dev.raw.aapl_price"
    train_view = "dev.adhoc.aapl_price_view"
    forecast_table = "dev.adhoc.aapl_price_forecast"
    forecast_function_name = "dev.analytics.predict_aapl_price"
    final_table = "dev.analytics.aapl_price_7days_prediction"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)