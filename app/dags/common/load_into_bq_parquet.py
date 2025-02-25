from __future__ import print_function
import datetime
import logging
from airflow import DAG
from airflow.models import Variable
#from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage,pubsub_v1
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json
import os
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.gcs import GCSHook

default_dag_args={
    'start_date' : datetime.datetime(2020, 5, 10)
}

dag_name = "load_csv_gcs_to_bq_parquet"
dag = DAG(
    'load_csv_gcs_to_bq_parquet',
    schedule_interval=None,
    default_args=default_dag_args
)

# https://stackoverflow.com/a/70397050/482899
log_prefix = "[pdh_batch_pipeline]"+"["+dag_name+"]"
exec_time_aest = get_current_time_str_aest()


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):        
        return log_prefix+" "+msg, kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")

publisher = pubsub_v1.PublisherClient()
project_id = os.environ.get('PDH_PROJECT_ID',"gcp-wow-wpay-paydat-dev")
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
logging.info(f"topic_path => {topic_path}")

def load_config(**kwargs):
    try:
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
        event_message = "Task load_config executed successfully."
        fname = "gs://"+bucket+"/"+file_name
        client = storage.Client(project_id)
        bucket_name = client.get_bucket(bucket)
        size_in_bytes = bucket_name.get_blob(file_name).size  
        event = Event(
            dag_name=dag_name,
            file_name=fname,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,
            biz_type="inbound",            
            target_name=object_name,
            file_size=int(size_in_bytes),
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        logging.info('Event published successfully')
    except Exception as e:
        event_message = f"Exception raised while executing load_config task: {str(e)}"        
        event = Event(
            dag_name=dag_name,            
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            biz_type="inbound",
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))


def load_into_table(**kwargs):
    try:
        task_instance = kwargs['ti']
        file_name = task_instance.xcom_pull(task_ids='load_config', key='file_name')
        bucket = task_instance.xcom_pull(task_ids='load_config', key='bucket')
        object_name = task_instance.xcom_pull(task_ids='load_config', key='object_name')
        
        logging.info(f'file_name : {file_name}')
        logging.info(f'bucket : {bucket}')
        logging.info(f'object_name : {object_name}')
        
        gsSourceUri = "gs://" + bucket + "/"  + file_name
        logging.info (f'gsSourceUri: {gsSourceUri}') 
        
        destination_project_dataset_table= Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['dataset_name'] + '.' + Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['table_name']
        logging.info (f'destination_project_dataset_table: {destination_project_dataset_table}')
        
        #Changes for composer 2.6.3 upgrade
        
        schema = Variable.get("v_gfs_datasets", deserialize_json=True)[object_name]['schema']
        print (f'schema is: {schema}')
        try:
            schemafield = json.loads(GCSHook().download(bucket,schema).decode("utf-8"))        
        except Exception as e:
            print(f'Error fetching schema file from GCS path: {e}')
        
        bq_load_client = bigquery.Client()
                                 
        try:
            job_config = bigquery.LoadJobConfig(
                    schema=schemafield,                 
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition = 'WRITE_APPEND',                    
                ) 
            logging.info (f'Succesful job config')
            
        except Exception as e:
                print(f'Exception in getting job_config:{job_config}, {e}')
        
        try:
                load_job = bq_load_client.load_table_from_uri(gsSourceUri, destination_project_dataset_table, job_config=job_config)  # API request
                load_job.result()               # Waits for the job to complete.
                 
        except Exception as e:
                print(f'Exception in load_job {e}')
        
        
        event_message = "Task load_into_table executed successfully."
        fname = "gs://"+bucket+"/"+file_name
        client = storage.Client(project_id)
        bucket_name = client.get_bucket(bucket)
        size_in_bytes = bucket_name.get_blob(file_name).size  
        event = Event(
            dag_name=dag_name,
            file_name=fname,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,
            biz_type="inbound",            
            target_name=object_name,          
            file_size=int(size_in_bytes),
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    except Exception as e:
        event_message = f"Exception raised while executing load_into_table task: {str(e)}" 
        event = Event(          
            dag_name=dag_name,            
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            biz_type="inbound",
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))



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

