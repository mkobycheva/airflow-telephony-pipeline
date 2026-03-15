from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import json
import duckdb
from datetime import timedelta
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

@dag(
    schedule="@hourly",
    start_date=datetime(2026, 3, 14),
    description="Fetching new calls from MySql DB",
    max_consecutive_failed_dag_runs=3
)
def load_new_calls():

    @task
    def detect_new_calls(ti):
        duck_hook = DuckDBHook.get_hook('duckdb_assignment2')
        conn = duck_hook.get_conn()
        watermark = conn.execute("SELECT MAX(last_processed) FROM watermarks WHERE pipeline_name = 'calls';").fetchall()[0][0]
        next_hour = watermark + timedelta(hours=1)

        call_ids = []
        request = "select call_id from a2_airflow.calls where (call_time > %s) AND (call_time <  %s);"
        mysql_hook = MySqlHook(mysql_conn_id='mysql_assignment2', schema='a2_airflow')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request, (watermark, next_hour))
        for call in cursor.fetchall():
            call_ids.append(call[0])
        ti.xcom_push(key='call_ids', value=call_ids)
        print("Watermark:", watermark)
        print("Detected calls:", call_ids)

    @task
    def load_telephony_details(ti):
        telephony = []
        call_ids = ti.xcom_pull(key='call_ids', task_ids="detect_new_calls")
        if not call_ids:
            ti.xcom_push(key="telephony", value=pd.DataFrame())
            return
        for call_id in call_ids:
            with open(f"/usr/local/airflow/include/telephony_json/call_{call_id}.json") as f:
                data = json.load(f)
            telephony.append(pd.json_normalize([data]))
        telephony_df = pd.concat(telephony)
        ti.xcom_push(key='telephony', value=telephony_df.to_dict(orient='records'))

    @task
    def transform_and_load_duckdb(ti):
        records = ti.xcom_pull(key='telephony', task_ids="load_telephony_details")
        telephony_df = pd.DataFrame(records)
        call_ids = tuple(ti.xcom_pull(key='call_ids', task_ids="detect_new_calls"))

        mysql_hook = MySqlHook(mysql_conn_id='mysql_assignment2', schema='a2_airflow')

        if not call_ids:
            print("No new calls")
            return

        format_strings = ','.join(['%s'] * len(call_ids))
        calls_df = mysql_hook.get_pandas_df(
            f"SELECT * FROM a2_airflow.calls WHERE call_id IN ({format_strings})",
            parameters=tuple(call_ids)
        )

        employees_df = mysql_hook.get_pandas_df("select * from a2_airflow.employees")

        merge1 = pd.merge(calls_df, employees_df, how='left', left_on='employee_id', right_on='employee_id')
        merged = pd.merge(merge1, telephony_df, how='left', left_on='call_id', right_on='call_id')

        duck_hook = DuckDBHook.get_hook('duckdb_assignment2')
        conn = duck_hook.get_conn()
        try:
            conn.register("merged_df", merged)
            conn.execute("INSERT OR IGNORE INTO support_call_enriched SELECT * FROM merged_df")
            new_watermark = merged['call_time'].max()
            conn.execute(
                "UPDATE watermarks SET last_processed = ? WHERE pipeline_name='calls'",
                [new_watermark]
            )
            print(f"Success! Loaded {len(merged)} rows. New watermark: {new_watermark}")
        finally:
            conn.close()

    detect_new_calls() >> load_telephony_details() >> transform_and_load_duckdb()

load_new_calls()

