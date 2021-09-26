from datetime import datetime
from textwrap import dedent
import json
from pandas import json_normalize
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from data_functions import set_to_dataframe

default_args = {
    'start_date':datetime(2021,9,25)
}

with DAG(
    'FortuneCompanyLabelDAG',
    default_args=default_args,
    schedule_interval='@hourly',
    description='DAG for mini project',
    catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup(group_id='group1') as available_check:
        is_api_ok = HttpSensor(
            task_id='api_ok',
            http_conn_id='fortune_symbol_api',
            endpoint='test',
            method='GET'
        )

        is_table_ok = PostgresOperator(
            task_id='check_tables',
            postgres_conn_id="fortune_dev",
            sql='SELECT * FROM fortune."Companies_DIM"',
        )
    select_data= BashOperator(
        task_id='dataframe',
        bash_command='sleep 3',
    )

start >> available_check >> select_data
#/query?function=OVERVIEW&symbol=IBM&apikey=YKZILVIKOISR52AZ'