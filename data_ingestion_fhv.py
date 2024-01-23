import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from ingest_script_fhv import ingest_fhv

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.environ.get("PG_HOST", '8e5a578d746b')
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "password")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "ny_taxi")

url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
url_template = url_prefix + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
output_file_template = AIRFLOW_HOME + '/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
table_name_template = 'fhv_{{ execution_date.strftime(\'%Y_%m\') }}'

local_workflow = DAG(
    "LocalIngestionDag_Fhv",
    schedule_interval = "0 8 2 * *",
    start_date = datetime(2023,1,1),
    end_date = datetime(2023,11,1),
    catchup=True,
    max_active_runs=2
)

with local_workflow:
    wget_task = BashOperator(
        task_id = 'wget',
        bash_command =f'curl -sSL {url_template} > {output_file_template}'
    )

    ingest_task = PythonOperator(
        task_id = 'ingest',
        python_callable = ingest_fhv,
        op_kwargs = dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name = table_name_template,
            csv_file = output_file_template
        ),
    )

    remove_task = BashOperator(
        task_id = 'remove_files',
        bash_command = f'rm {output_file_template}'
    )

    wget_task >> ingest_task >> remove_task

