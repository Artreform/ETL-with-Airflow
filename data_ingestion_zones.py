import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from ingest_zones import ingest_zones

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.environ.get("PG_HOST", '8e5a578d746b')
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "password")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "ny_taxi")

url_template = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
output_file_template = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
table_name_template = 'taxi_zone_lookup'

local_workflow = DAG(
    "taxi_zone",
    schedule_interval = "@once",
    start_date = datetime(2023,1,1),
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
        python_callable = ingest_zones,
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

