from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_session_summary():
    cur = return_snowflake_conn()

    # Create session_summary table with JOIN and de-dup using ROW_NUMBER
    cur.execute("BEGIN;")
    cur.execute("""
        CREATE OR REPLACE TABLE analytics.session_summary AS
        SELECT sessionId, userId, channel, ts
        FROM (
            SELECT 
                c.userId,
                c.sessionId,
                c.channel,
                t.ts,
                ROW_NUMBER() OVER (PARTITION BY c.sessionId ORDER BY t.ts DESC) AS row_num
            FROM dev.raw.user_session_channel c
            JOIN dev.raw.session_timestamp t
            ON c.sessionId = t.sessionId
        ) deduped
        WHERE row_num = 1;
    """)
    cur.execute("COMMIT;")

@task
def check_duplicates(table, primary_key):
    cur = return_snowflake_conn()

    if primary_key is not None:
        sql = f"""
        SELECT {primary_key}, COUNT(1) AS cnt
        FROM {table}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1
        """
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()
        print(result, result[1])
        if int(result[1]) > 1:
            print("!!!!!!!!!!!!!")
            raise Exception(f"Primary key uniqueness failed: {result}")

with DAG(
    dag_id='elt_session_summary_dag',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    tags=['ELT', 'dedup', 'analytics'],
) as dag:

    create_task = create_session_summary()
    check_dup_task = check_duplicates("analytics.session_summary", "sessionId")

    create_task >> check_dup_task
