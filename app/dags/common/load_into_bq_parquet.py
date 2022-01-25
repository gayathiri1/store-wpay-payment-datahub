from __future__ import print_function
import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator

default_dag_args={
    'start_date' : datetime.datetime(2020, 5, 10)
}

dag = DAG(
    'load_csv_gcs_to_bq_parquet',
    schedule_interval=None,
    default_args=default_dag_args
)


def load_config(**kwargs):
    input_params = kwargs['dag_run'].conf
    logging.info('input_params : {}'.format(input_params))
    bucket = input_params['conf']['bucket']
    file_name = input_params['conf']['file_name']
    object_name = file_name.split('/')[2]
    logging.info('bucket is : {}'.format(bucket))
    logging.info('file_name is : {}'.format(file_name))
    kwargs['ti'].xcom_push(key='bucket', value=bucket)
    kwargs['ti'].xcom_push(key='file_name', value=file_name)
    kwargs['ti'].xcom_push(key='object_name', value=object_name)


def load_into_table(**kwargs):
    task_instance = kwargs['ti']
    file_name = task_instance.xcom_pull(task_ids='load_config', key='file_name')
    bucket = task_instance.xcom_pull(task_ids='load_config', key='bucket')
    object_name = task_instance.xcom_pull(task_ids='load_config', key='object_name')
    load_into_table_t = GoogleCloudStorageToBigQueryOperator(
        task_id='load_into_bq_from_parquet',
        bucket=bucket,
        source_objects=[file_name],
        destination_project_dataset_table=Variable.get("v_gfs_datasets", deserialize_json=True)['project_name'] + ':'
                                          + Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['dataset_name'] + '.'
                                          + Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['table_name'],
        write_disposition="WRITE_APPEND",
        schema_object=Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['schema'],
        schema_fields=None,
        source_format='parquet',
        autodetect=False,
        dag=dag
    )
    load_into_table_t.execute(dict())


load_config_t = PythonOperator(
    task_id='load_config',
    provide_context=True,
    python_callable=load_config,
    dag=dag
)

load_into_table_t = PythonOperator(
    task_id='load_into_table',
    provide_context=True,
    python_callable=load_into_table,
    dag=dag
)

load_config_t >> load_into_table_t

