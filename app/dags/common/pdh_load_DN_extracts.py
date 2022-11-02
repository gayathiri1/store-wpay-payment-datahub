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


default_dag_args = {
    'start_date': datetime.datetime(2020, 5, 10)
}


dag = DAG(
    'pdh_load_DN_extracts',
    schedule_interval=None,
    default_args=default_dag_args
)


def load_config_and_validate_run(**kwargs):
    input_params = kwargs['dag_run'].conf
    task_instance = kwargs['ti']
    pattern = None
    uid = task_instance.xcom_pull("generate_uuid")
    print(f'input param :{input_params}')
    run_config = input_params['conf']
    object_name = input_params['conf']['file_name'].split('/')[0]
    lkp_config = input_params['conf']['file_name'].split('/')[2]
    print(f'file name : {object_name}')
    print(f'lkp_config : {lkp_config}')
    file_name = input_params['conf']['file_name']
    if 'gfs' in object_name:
        config = Variable.get("v_gfs_datasets",deserialize_json=True)
        print(f'config:=>{config}')
        curate_flag = config[lkp_config]['curate_flag']        
        if "delimiter" in config[lkp_config]: 
            print("Delimiter condition satisfied")
            delimiter=config[lkp_config]['delimiter']# Get delimiter from variable 
        else:
            print("Delimiter condition is not satisfied")
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
            print("Delimiter condition satisfied")
            delimiter=config[lkp_config]['delimiter']# Get delimiter from variable
        else:
            print("Delimiter condition is not satisfied")
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
    print(f'config is :{config}')
    flag = pu.PDHUtils.check_delta(dataset, table_name, file_name)
    if flag != 1:
        print(f'uuid1 :=>{uid}')
        kwargs['ti'].xcom_push(key='input_params' + uid, value=config)
        kwargs['ti'].xcom_push(key='payload' + uid, value=input_params)
        dataproc = du.ListDataProcClusters(config['project_name'], 'us-central1')
        dataproc_status = dataproc.list_clusters()
        dataproc_jobs = dataproc.list_jobs()
        if pattern == 'csv' or (pattern == 'txt' and delimiter == '|'):
            return 'load_csv_file'        
        elif dataproc_status is None:
            return 'start_cluster'
        elif dataproc_jobs <= 2:
            return 'spark_submit'
        else:
            print('Re-Updating the Queue')
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
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    print(f'uuid2 :=>{uid}')
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    pu.PDHUtils.load_csv_to_bq(dag,uid,**input_params)
    print('load_csv_file completed')


def load_target_table(**kwargs):
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
        print(f'file_name is : {file_name}')
        source_objects = file_name
    else:
        print('Wrong value for parameter load_type. Should be H or I')
        exit(1)
    dataset = input_params[object_name]['dataset_name']
    table_name = input_params[object_name]['table_name']
    flag = pu.PDHUtils.check_delta(dataset,table_name,file_name)
    if flag != 1:
        if 'curate_sql' not in input_params[object_name]:
            sql = pu.PDHUtils.reflect_bq_schema(dataset,table_name,file_date, source_objects, pdh_load_time, payload_id,uid)
        else:
            sql = pu.PDHUtils.reflect_bq_schema_curated(table_name, file_date, source_objects, pdh_load_time,payload_id, uid)
    else:
        print(f'File {file_name} already loaded')


def start_cluster():
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    spark_start = si.SparkInit(dag, 'start_cluster', config['project_name'],config['project_name'],
                               config['project_name'], '','')
    try:
        t1 = spark_start.start_cluster()
        return t1
    except Exception as e:
        print(f'Exception caught :=> {e}')
        return 'update_queue'


def spark_submit(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    file_name = input_params['run_config']['file_name']
    object_name = file_name.split('/')[2]
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    print(f'object name : {object_name}')
    print(f'Pypath : {config[object_name]}')
    spark_sub= si.SparkInit(dag, 'spark-submit', config['project_name'], config['project_name'],
                            config['project_name'], 'gs://' + config[object_name]['pyPath'],config[object_name]['args'])
    t2 = spark_sub.submit_cluster()
    try:
        t2.execute(dict())
        return 'check_tasks'
    except Exception as e:
        print(f'Exception :=> {e}')
        return 'update_queue'


def check_tasks():
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    tasks = du.ListDataProcClusters.list_task_in_queue()
    dataproc = du.ListDataProcClusters(config['project_name'], 'us-central1')
    dataproc_jobs = dataproc.list_jobs()
    if tasks is None and dataproc_jobs == 0:
        return 'stop_cluster'
    else:
        return 'skip_dataproc'


def stop_cluster():
    config = Variable.get("v_gfs_datasets", deserialize_json=True)
    spark_stop= si.SparkInit(dag, 'stop_cluster', config['project_name'], config['project_name'],
                             config['project_name'], '','')
    t3 = spark_stop.stop_cluster()
    return t3


def load_completed(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    input_params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='input_params' + uid)
    emailTo = input_params['emailTo']
    file_name = input_params['run_config']['file_name']
    pu.PDHUtils.send_email(emailTo,'PDH Prod Load Status',
                           f'File:=>{file_name} loaded successfully')


def skip_dataproc():
    config = Variable.get("v_non_gfs_load_params", deserialize_json=True)
    emailTo = config['emailTo']
    pu.PDHUtils.send_email(emailTo,'spark init skipped','Cluster already in running state or task queue is not empty')


def update_queue(**kwargs):
    task_instance = kwargs['ti']
    uid = task_instance.xcom_pull("generate_uuid")
    params = task_instance.xcom_pull(task_ids='load_config_and_validate_run', key='payload' + uid)
    payload = json.dumps(params)
    converted_payload = payload.encode()
    du.ListDataProcClusters.push_message_to_queue(converted_payload)
    print('Queue update requested')


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


generate_uuid >> load_config_and_validate_run_t >> load_csv_file_t >> load_target_table_t >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> start_cluster() >> spark_submit >> check_tasks >> stop_cluster() >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> start_cluster() >> spark_submit >> check_tasks >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> check_tasks >> stop_cluster() >> load_completed_t
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> check_tasks >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> spark_submit >> update_queue
generate_uuid >> load_config_and_validate_run_t >> skip_dataproc
generate_uuid >> load_config_and_validate_run_t >> update_queue
generate_uuid >> load_config_and_validate_run_t >> start_cluster() >> update_queue
