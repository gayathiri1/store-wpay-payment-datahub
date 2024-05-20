from __future__ import print_function
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from google.cloud import bigquery,pubsub_v1
from google.oauth2 import service_account
from zlibpdh import pdh_utilities as pu
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json,os
import pendulum

#Fix to handle daylight savings
local_tz = pendulum.timezone("Australia/Sydney")

#Set project_id here.
logging.info(f"ENV PROJECT ID is {os.environ.get('PROJECT_ID')}")
logging.info(f"ENV GCP_PROJECT ID is {os.environ.get('GCP_PROJECT')}")
IS_PROD = False
project_id = os.environ.get('PDH_PROJECT_ID',"gcp-wow-wpay-paydat-dev")
logging.info(f"Project id is => {project_id}")
#Based on Project ID set start data here.
if project_id.lower() == "gcp-wow-wpay-paydathub-prod":
    logging.info(f"Current project is PROD =>{project_id}")
    IS_PROD = True
    start_date = datetime(2024,5,16, tzinfo=local_tz)
else:
    start_date = datetime(2024,5,12, tzinfo=local_tz)

default_dag_args= {
    'start_date' : start_date,
    'max_active_runs': 1,
    'retry_delay': timedelta(9000),
    'retries': 0,
}

dag_name = "load_wdp_to_bq"
#project_id = os.environ.get('PROJECT_ID',"gcp-wow-wpay-paydat-dev")
# https://stackoverflow.com/a/70397050/482899
#log_prefix = f"[pdh_batch_pipeline][{dag_name}]"

try:
    if IS_PROD:
        dag = DAG('load_wdp_to_bq', catchup=False, default_args=default_dag_args,schedule_interval="30 09 * * *")
    else:
        dag = DAG('load_wdp_to_bq', catchup=False, default_args=default_dag_args,schedule_interval=None)  
except Exception as e:
    logging.info("Exception in setting DAG schedule:{}".format(e)) 
    
log_prefix = "[pdh_batch_pipeline]"+"["+dag_name+"]"
exec_time_aest = get_current_time_str_aest()


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        #return f"{log_prefix} {msg}", kwargs
        return log_prefix+" "+msg, kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")

publisher = pubsub_v1.PublisherClient()
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
logging.info(f"topic_path => {topic_path}")
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))

def load_wdp_run_config(**kwargs):
    logging.info('Running def load_wdp_run_config')
    load_param = []
    load_type_param = []
    config = Variable.get("wdp_pos", deserialize_json=True)
    load_param.append(config['project_name'])
    load_config = config['load_config']
    for i in load_config:
        load_type_param.append(load_config[i]['load_type'])
        load_param.append(load_config[i]['dataset'])
        load_param.append(load_config[i]['table'])
        load_param.append(load_config[i]['src_dataset'])
        load_param.append(load_config[i]['src_project'])
        load_param.append(load_config[i]['src_view'])
    kwargs['task_instance'].xcom_push(key='load_param',value=load_param)
    kwargs['task_instance'].xcom_push(key='load_type_param', value=load_type_param)


def get_bq_client():
    key_path = '/home/airflow/gcs/data/wdp_creds.json'

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    return client


def parse_load_type(**kwargs):
    task_instance = kwargs['task_instance']
    load_type_param = task_instance.xcom_pull(task_ids='load_wdp_run_config', key='load_type_param')
    load_param = task_instance.xcom_pull(task_ids='load_wdp_run_config', key='load_param')
    for i in load_type_param:
        if i == 'I':
            incremental_load(load_param)
        elif i == 'H':
            initial_load(load_param)
        else:
            logging.info('Wrong value of load type. It should be I or H.')


def initial_load(load_param):
    logging.info('Historical load initiated.Truncating table.')
    client = get_bq_client()
    project_name, dataset, table_name, src_dataset, src_project, src_view = [load_param[i] for i in range(0, 6)]
    load_param[1:6] = []
    truncate = """Truncate table {0}.{1}.{2}""".format(
                                                    project_name,
                                                    dataset,
                                                    table_name)
    truncate_job = client.query(truncate)
    for row in truncate_job:
        print(row)

    query = """
        Insert into {0}.{1}.{2}
        SELECT *
        FROM `{3}.{4}.{5}`
    """.format(project_name,
               dataset,
               table_name,
               src_project,
               src_dataset,
               src_view
               )
    query_job = client.query(query)
    for rows in query_job:
        print(rows)


def incremental_load(load_param):
    print('load_param : {}'.format(load_param))
    project_name, dataset, table_name, src_dataset, src_project, src_view = [load_param[i] for i in range(0,6)]
    load_param[1:6] = []
    print('table_name:{}'.format(table_name))
    max_load_dttm = """select max(load_dttm) as max_load_dttm
                    from  `{0}.{1}.{2}`""".format(project_name,dataset,table_name)
    print('SQL ==> {}'.format(max_load_dttm))
    client = get_bq_client()
    query_job = client.query(max_load_dttm)
    for row in query_job:
        load_dttm = row[0]
    print('load_dttm:{}'.format(load_dttm.date()))

    create_temp = query =  """
          CREATE OR REPLACE TABLE {0}.{1}.temp_{2}  as
          SELECT *
          FROM `{3}.{4}.{5}`
          where cast(load_dttm as date) > date_sub(@load_dttm,INTERVAL 3 DAY);
      """.format(project_name,dataset,str(load_dttm.date()).replace('-',''),src_project,src_dataset,src_view,)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("load_dttm", "DATE", load_dttm.date()),
        ]
    )
    print('TEMP : {}'.format(create_temp))
    query_create_temp = client.query(create_temp,job_config=job_config)
    for row in query_create_temp:
        print(row)

    query =  """
          Insert into {0}.{1}.{2}
          select temp.* from {3}.{4}.temp_{5} as temp
          left join (
          SELECT load_dttm 
          FROM `{6}.{7}.{8}` group by 1) as tbl on temp.load_dttm = tbl.load_dttm where tbl.load_dttm is null 
      """.format(project_name,dataset,table_name,project_name,
                 dataset,str(load_dttm.date()).replace('-',''),
                 project_name,dataset,table_name)
    print('INSERT ====> {}'.format(query))
    query_job = client.query(query)
    for row in query_job:
        print(row)

    drop_temp_sql = """drop table {0}.{1}.temp_{2}""".format(project_name,dataset,str(load_dttm.date()).replace('-',''))
    print('DROP TEMP SQL ====> {}'.format(drop_temp_sql))

    query_job = client.query(drop_temp_sql)
    for row in query_job:
        print(row)



def load_completed():
    try:
        EmailTo = Variable.get("wdp_pos", deserialize_json=True)['emailTo']
        logging.info('load into big query completed')
        pu.PDHUtils.send_email(EmailTo, 'WDP Load Status (***Prod***)', 'WDP Load completed')        
        execTimeInAest = pendulum.now("Australia/Sydney")
        event_message = "WDP Load Status (***Prod***)" + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    except Exception as e:
        logging.info("Exception Raised :{}".format(e))
        event_message = f"Exception Raised in WDP Load Task :{e}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))


load_wdp_run_config_t = PythonOperator(
    task_id='load_wdp_run_config',
    provide_context=True,
    python_callable=load_wdp_run_config,
    dag=dag
)


parse_load_type_t = PythonOperator(
    task_id='parse_load_type',
    provide_context=True,
    python_callable=parse_load_type,
    dag=dag
    )


load_completed_t = PythonOperator(
    task_id='load_completed',
    python_callable=load_completed,
    dag=dag
    )


load_wdp_run_config_t >> parse_load_type_t >> load_completed_t
