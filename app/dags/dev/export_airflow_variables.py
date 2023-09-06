import datetime
import pendulum
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator
AIRFLOW_GCS_BUCKET= None
if  os.environ.get("GCS_BUCKET", "no bucket name"):
    AIRFLOW_GCS_BUCKET = os.environ.get("GCS_BUCKET", "no bucket name")
project_id = os.environ['GCP_PROJECT']
env = ''
if project_id == 'gcp-wow-wpay-paydat-dev':
    env = 'dev'
elif project_id == 'gcp-wow-wpay-paydathub-uat':
    env = 'uat'
elif project_id == 'gcp-wow-wpay-paydathub-prod':
    env='prod'
    
local_tz = pendulum.timezone("Australia/NSW")

default_dag_args = {
    'start_date': datetime.datetime(2020, 6, 18, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id='export_airflow_variables',
         schedule_interval='00 3 * * *',
         default_args=default_dag_args,
         catchup=False,
         user_defined_macros={
             'env': os.environ,
             "project_id": project_id
         }) as dag:


    start=DummyOperator(
        task_id = f'start',
        )

    export_task=BashOperator(
        task_id = 'export_var_task',
        bash_command = f'airflow variables --export variables.json; gsutil cp variables.json gs://{AIRFLOW_GCS_BUCKET}/dags/dag_variables/{env}/variables.json',
    )


start >> export_task

