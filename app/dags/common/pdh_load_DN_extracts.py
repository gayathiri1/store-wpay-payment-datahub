import os
import datetime
from zlibpdh import spark_init as si
from airflow import DAG, macros
from airflow.models import Variable
from pytz import timezone
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from zlibpdh import pdh_utilities as pu
from zlibpdh import pdh_dataproc_utils as du
import json
import uuid
import logging
from pdh_logging.utils import get_current_time_str_aest
from google.cloud import pubsub_v1, storage
from pdh_logging.event import Event
from dataclasses import asdict


default_dag_args = {
    'start_date': datetime.datetime(2020, 5, 10)
}

dag_name = "pdh_load_DN_extracts"
dag = DAG(
    'pdh_load_DN_extracts',
    schedule_interval=None,
    default_args=default_dag_args
)

# https://stackoverflow.com/a/70397050/482899
log_prefix = f"[pdh_batch_pipeline][{dag_name}]"
exec_time_aest = get_current_time_str_aest()


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):        
        return log_prefix+" "+msg, kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")

publisher = pubsub_v1.PublisherClient()
project_id = os.environ.get('PROJECT_ID', "gcp-wow-wpay-paydat-dev")
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)

def load_config_and_validate_run(**kwargs):
    input_params = kwargs['dag_run'].conf
    task_instance = kwargs['ti']
    pattern = None
    uid = task_instance.xcom_pull("generate_uuid")
    logging.info(f'{input_params=}')
    run_config = input_params['conf']
    object_name = input_params['conf']['file_name'].split('/')[0]
    lkp_config = input_params['conf']['file_name'].split('/')[2]
    logging.info(f'file name : {object_name}')
    logging.info(f'{lkp_config=}')
    file_name = input_params['conf']['file_name']
    if 'gfs' in object_name:
        config = Variable.get("v_gfs_datasets",deserialize_json=True)
        logging.info(f'{config=}')
        curate_flag = config[lkp_config]['curate_flag']        
        if "delimiter" in config[lkp_config]: 
            logging.info("Delimiter condition satisfied")
            delimiter=config[lkp_config]['delimiter']# Get delimiter from variable 
        else:
            logging.info("Delimiter condition is not satisfied")
            delimiter=','
        if lkp_config in config and curate_flag[0:1] == 'Y' and 'curated' not in file_name:
            src_file = input_params['conf']['bucket'] + '/' + file_name
            trgt_file = src_file.replace('landing','curated')
            pu.PDHUtils.curate_csv_files(src_file,trgt_file,curate_flag)
            return 'skip_dataproc'
        elif lkp_config in config:
            dataset, table_name, pattern = get_config(lkp_config, config)
        else:
            return 'skip_dataproc'
    else:
        config = Variable.get("v_non_gfs_load_params", deserialize_json=True)
        curate_flag = config[lkp_config]['curate_flag']        
        if "delimiter" in config[lkp_config]: 
            logging.info("Delimiter condition satisfied")
            delimiter=config[lkp_config]['delimiter']# Get delimiter from variable
        else:
            logging.info("Delimiter condition is not satisfied")
            delimiter=','
        if lkp_config in config and curate_flag[0:1] == 'Y' and 'curated' not in file_name:
            src_file = input_params['conf']['bucket'] + '/' + file_name
            trgt_file = src_file.replace('landing', 'curated')
            pu.PDHUtils.curate_csv_files(src_file, trgt_file,curate_flag)
            return 'skip_dataproc'
        if lkp_config in config:
            dataset, table_name, pattern = get_config(lkp_config, config)
        else:
            return 'skip_dataproc'
    config['run_config'] = run_config
    config['delimiter_used'] = delimiter
    logging.info(f'{config=}')
    flag = pu.PDHUtils.check_delta(dataset, table_name, file_name)    
    if flag != 1:
        logging.info(f'uuid1 :=>{uid}')
        kwargs['ti'].xcom_push(key='input_params' + uid, value=config)
        kwargs['ti'].xcom_push(key='payload' + uid, value=input_params)
        dataproc = du.ListDataProcClusters(config['project_name'], 'us-central1')
        dataproc_status = dataproc.list_clusters()
        dataproc_jobs = dataproc.list_jobs()
        if pattern == 'csv' or (pattern == 'txt' and delimiter == '|'):
            #check for empty flag.            
            if "empty_flag" in config[lkp_config]:
                logging.info("Empty file flag is set...")
                empty_flag=config[lkp_config]['empty_flag']# Get flag from variable
            else:
                logging.info("Empty file flag is not set...")
                empty_flag='N'
            try:
                kwargs['ti'].xcom_push(key="empty_flag", value=empty_flag)
                logging.info(f'{empty_flag=}')
            except Exception as e:
                logging.info(f"Exception: {e}")     
            return 'load_csv_file'           
        elif dataproc_status is None:
            return 'start_cluster'
        elif dataproc_jobs <= 2:
            return 'spark_submit'
        else:
            logging.info('Re-Updating the Queue')
            return 'update_queue'
    else:
        # already loaded, skip the run
        return 'skip_dataproc'


def get_config(lkp_config,config):
    if lkp_config in config:
        dataset = config[lkp_config]['dataset_name']
        table_name = config[lkp_config]['table_name']
        pattern = config[lkp_config]['pattern']
        return dataset,table_name,pattern


def load_csv_file(**kwargs):
    try:
        task_instance = kwargs['ti']
        uid = task_instance.xcom_pull("generate_uuid")
        input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)    
        logging.info(f'uuid2 :=>{uid}')
        try:        
            empty_flag = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='empty_flag')
        except Exception as e:
            logging.info(f"Exception: {e}")
        #Empty file check logic here.
        if empty_flag == 'Y':
            logging.info("Empty File Check logic...")
            status = is_empty_file(task_instance)
            logging.info(f'{status=}')
        #BAU logic here.
        logging.info('BAU logic....')
        pu.PDHUtils.load_csv_to_bq(dag,uid,**input_params)
        logging.info('load_csv_file completed')    
        event_message = "Task load_csv_file executed successfully."
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    except Exception as e:
        logging.info(f"Exception: {e}")
        event_message = f"Exception raised while executing load_csv_file task: {str(e)}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        
#Empty file check function.
def is_empty_file(task_instance) -> bool:    
    #Get input paramters here.    
    try:
        uid = task_instance.xcom_pull("generate_uuid")
        input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)    
        bucket = input_params['run_config']['bucket']
        file_name = input_params['run_config']['file_name']     
        object_name = file_name.split('/')[0]
        lkp_config = file_name.split('/')[2]
    except Exception as e:
        logging.info(f"Exception: {e}")
    is_empty = False #deafult value
    #Get paramters from variables.
    if 'gfs' in object_name:
        config = Variable.get("v_gfs_datasets", deserialize_json=True)
    else:
        config = Variable.get("v_non_gfs_load_params", deserialize_json=True)    
    #check for delimiter
    if "delimiter" in config[lkp_config]:
        logging.info("is_csv_file_empty -->Delimiter is set in airflow var...")
        delimiter=config[lkp_config]['delimiter']# Get delimiter from variable         
    else:
        logging.info("is_csv_file_empty -->Delimiter is no set in airflow var")
        delimiter=','
    #check for header
    if "header" in config[lkp_config]:
        logging.info("Header is set in airflow var...")        
        header=config[lkp_config]['header']# Get header from variable
        logging.info(f'{header=}')
    else:
        logging.info("Header is not set,using default value...")        
        header=0
        logging.info(f'{header=}')
    #Get filepath.    
    file_path = 'gs://' + bucket + '/' + file_name
    is_empty = pu.PDHUtils.is_csv_file_empty(file_path,delimiter,header)    
    if is_empty == True:        
        logging.info(f'Empty file detected =>{file_name}')           
        emailTo = input_params['emailTo']
        pu.PDHUtils.send_email(emailTo,'PDH Prod Load Status',
                           f'Empty File:=>{file_name} detected into PDH.')        
    else:          
        logging.info(f'Non-Empty file detected =>{file_name}')        
    return is_empty


def load_target_table(**kwargs):
    try:
        tz = timezone('Australia/Sydney')
        task_instance = kwargs['ti']
        uid = task_instance.xcom_pull("generate_uuid")
        input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
        file_name = input_params['run_config']['file_name']
        if 'quickcilver' in file_name.lower():
            file_date = file_name.split('.')[0][-19:-9].replace('_','')
        elif 'merfee' in file_name.lower():
            file_date = file_name.split('/')[3].split('.')[0][-15:-7]
        elif 'isg_wpay' in file_name.lower() or 'tn70_mc' in file_name.lower()\
        or 'tc33_visa' in file_name.lower():
            file_date = file_name.split('/')[3].split('.')[0][-10:-2]
        elif 'lmi_storedatafeed' in file_name.lower()\
        or 'lmi_datafeed' in file_name.lower():        
            now_val = datetime.datetime.now(tz)
            yy_val = now_val.strftime("%Y%m%d")[:2]               
            file_date = file_name.split('/')[3].split('.')[2][1:]
            file_date = str(yy_val)+file_date
        elif 'edp_wallet' in file_name.lower()\
        or 'dn_wpaygfssst' in file_name.lower():
            file_date = file_name.split('/')[3].split('.')[0][-14:-6]
        elif 'saphybris_sales_report' in file_name.lower():
            file_date = file_name.split('/')[3].split('.')[0][-13:-5]
            file_date = datetime.datetime.strptime(file_date, '%Y%m%d')
            file_date = file_date.strftime('%Y%m%d')
        else:
            file_date = file_name.split('/')[3].split('.')[0][-8:]
        now = datetime.datetime.now(tz)
        pdh_load_time = now.strftime('%Y-%m-%d %H:%M:%S')
        payload_id = now.strftime("%Y%m%d%H%M%S")
        object_name = file_name.split('/')[2]
        load_type = input_params[object_name]['load_type']
        if load_type == 'H':
            path = file_name.split('/')
            path[2] = '*'
            source_objects = ['/'.join(path)][0]
        elif load_type == 'I':
            logging.info(f'{file_name=}')
            source_objects = file_name
        else:
            logging.info('Wrong value for parameter load_type. Should be H or I')
            exit(1)
        dataset = input_params[object_name]['dataset_name']
        table_name = input_params[object_name]['table_name']
        flag = pu.PDHUtils.check_delta(dataset,table_name,file_name)
        if flag != 1:
            if 'curate_sql' not in input_params[object_name]:
                sql = pu.PDHUtils.reflect_bq_schema(dataset,table_name,file_date, source_objects, pdh_load_time, payload_id,uid)
                event_message = f'Data loaded successfully into {dataset}.{table_name} on {file_date}'
                event = Event(
                    dag_name=dag_name,
                    event_status="success",
                    event_message=event_message,
                    start_time=exec_time_aest,
                )
                publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            else:
                sql = pu.PDHUtils.reflect_bq_schema_curated(table_name, file_date, source_objects, pdh_load_time,payload_id, uid)
                event_message = f'Data loaded successfully into {table_name} on {file_date}'
                event = Event(
                    dag_name=dag_name,
                    event_status="success",
                    event_message=event_message,
                    start_time=exec_time_aest,
                )
                publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        else:
            logging.info(f'File {file_name} already loaded')
            event_message = f'File {file_name} already loaded'
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    
    except Exception as e:
        logging.info(f'Exception caught :=> {str(e)}')
        event_message = f'Exception raised while executing load_target_table task: {str(e)}'
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))



def start_cluster(**kwargs):
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    task_instance = kwargs['ti']
    logging.info(f'task_instance :=> {task_instance}')
    spark_start = si.SparkInit(dag, 'start-cluster', config['project_name'],config['project_name'],
                               config['project_name'], '','','')
    try:
        t1 = spark_start.start_cluster()
        t1.execute(task_instance.get_template_context())
        return 'spark_submit'

    except Exception as e:
        logging.info(f'Exception caught :=> {e}')
        return 'update_queue'

def spark_submit(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    file_name = input_params['run_config']['file_name']
    object_name = file_name.split('/')[2]
    #MCI.AR.T1G0.S.E0086464.D231225.T191300.A001.txt #MCI.AR.TFL6.S.E0086464.D230108.T195107.A001.txt
    if object_name == "mastercard_TFL6" or object_name == "mastercard_T1G0":
      temp_fname = file_name.split('/')[3]
      fname = (temp_fname.split('.')[0] + "_" + temp_fname.split('.')[2] +"_" + temp_fname.split('.')[5]).lower()
    else:
        fname = (file_name.split('/')[3]).split('.')[0]
        
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    logging.info(f'{object_name=}')
    logging.info(f'Pypath : {config[object_name]}')
    # Dynamic setup pypath
    # https://woolworthsdigital.atlassian.net/browse/DATPAY-3735
    gcs_bucket = os.environ.get("GCS_BUCKET")
    pypath = config[object_name]["pyPath"]
    logging.info(f"load pypath from variable: {pypath}")
    if gcs_bucket:
        path_list = pypath.split("/")
        # get rid of fixed bucket name
        # us-central1-pdh-composer-de-9fdde59d-bucket/dags/zSparkSubmit/load_wpay.py
        if path_list[0].startswith("us-central1-pdh-composer") and path_list[0].endswith("-bucket"):
            path_list[0] = gcs_bucket
        else:
            path_list.insert(0, gcs_bucket)
        pypath = "/".join(path_list)
        logging.info(f"dynamic setup pypath: {pypath}")
    pypath = "gs://" + pypath
        

    spark_sub= si.SparkInit(dag, 'spark-submit', config['project_name'], config['project_name'],
                            config['project_name'], pypath, config[object_name]['args'],fname)
    t2 = spark_sub.submit_cluster()
    try:
        t2.execute(task_instance.get_template_context())
        event_message = f'Spark job submitted successfully for {file_name}'
        event=Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return 'check_tasks'
    except Exception as e:
        logging.info(f'Exception :=> {e}')
        event_message = f'Exception raised while Spark job submission: {str(e)}'
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return 'update_queue'


def check_tasks():
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    tasks = du.ListDataProcClusters.list_task_in_queue()
    dataproc = du.ListDataProcClusters(config['project_name'], 'us-central1')
    dataproc_jobs = dataproc.list_jobs()
    logging.info(f'Current number of airflow tasks :=> {tasks} and dataproc_jobs:=> {dataproc_jobs}')
    # stop_clusterip if either conditions are true
    if tasks is None or dataproc_jobs == 0:
        return 'stop_cluster'
    else:
        return 'skip_dataproc'


def stop_cluster(**kwargs):
    task_instance = kwargs['ti']
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    spark_stop= si.SparkInit(dag, 'stop-cluster', config['project_name'], config['project_name'],
                             config['project_name'], '','','')
    t3 = spark_stop.stop_cluster()
    t3.execute(task_instance.get_template_context())
    #return t3


def load_failed(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    emailTo = input_params['emailTo']
    file_name = input_params['run_config']['file_name']
    pu.PDHUtils.send_email(emailTo,'PDH Prod Load Status',
                           f'File:=>{file_name} loading failed.Please check file contents.')


def load_completed(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    emailTo = input_params['emailTo']
    file_name = input_params['run_config']['file_name']
    #pu.PDHUtils.send_email(emailTo,'PDH Prod Load Status',
    #                       f'File:=>{file_name} loaded successfully')


def skip_dataproc():
    config = Variable.get("v_non_gfs_load_params", deserialize_json=True)
    emailTo = config['emailTo']
    # pu.PDHUtils.send_email(emailTo,'spark init skipped','Cluster already in running state or task queue is not empty')


def update_queue(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='payload' + uid)
    payload = json.dumps(params)
    converted_payload = payload.encode()
    du.ListDataProcClusters.push_message_to_queue(converted_payload)
    logging.info('Queue update requested')



generate_uuid = PythonOperator(
    task_id='generate_uuid',
    python_callable=lambda: str(uuid.uuid4().fields[-1])[:5],
    dag=dag
   )


load_config_and_validate_run_t = BranchPythonOperator(
    task_id='load_config_and_validate_run',
    provide_context=True,
    python_callable=load_config_and_validate_run,
    dag=dag
)

start_cluster_t = BranchPythonOperator(
    task_id='start_cluster',
    provide_context=True,
    python_callable=start_cluster,
    dag=dag
)

check_tasks = BranchPythonOperator(
    task_id='check_tasks',
    python_callable=check_tasks,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)



load_csv_file_t = PythonOperator(
    task_id='load_csv_file',
    provide_context=True,
    python_callable=load_csv_file,
    dag=dag
)

load_target_table_t = PythonOperator(
    task_id='load_target_table',
    provide_context=True,
    python_callable=load_target_table,
    dag=dag
)

spark_submit = BranchPythonOperator(
    task_id='spark_submit',
    provide_context=True,
    python_callable=spark_submit,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)


load_failed_t = PythonOperator(
    task_id='load_failed',
    provide_context=True,
    python_callable=load_failed,
    dag=dag
)

load_completed_t = PythonOperator(
    task_id='load_completed',
    provide_context=True,
    python_callable=load_completed,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

skip_dataproc = PythonOperator(
    task_id='skip_dataproc',
    python_callable=skip_dataproc,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

update_queue = PythonOperator(
    task_id='update_queue',
    provide_context=True,
    python_callable=update_queue,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

stop_cluster_t = PythonOperator(
    task_id='stop_cluster',
    python_callable=stop_cluster,
    dag=dag
)


generate_uuid >> load_config_and_validate_run_t >> load_csv_file_t >> load_target_table_t >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> load_failed_t
generate_uuid >> load_config_and_validate_run_t >> start_cluster_t >> spark_submit >> check_tasks >> stop_cluster_t >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> start_cluster_t >> spark_submit >> check_tasks >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> check_tasks >> stop_cluster_t >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> check_tasks >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> update_queue
generate_uuid >> load_config_and_validate_run_t >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> update_queue
generate_uuid >> load_config_and_validate_run_t >> start_cluster_t >> update_queue