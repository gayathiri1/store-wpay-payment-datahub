from __future__ import print_function
import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from zlibpdh import pdh_utilities as pu, getBQClient as bq

default_dag_args= {
    'start_date' : datetime.datetime(2021, 1, 4)
}

dag = DAG(
    'pdh_replicate_wdp_prod_to_uat',
    default_args=default_dag_args,
    schedule_interval= "40 22 * * *"
    
)


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
    # key_path = '/home/airflow/gcs/data/wdp_creds.json'
    #
    # credentials = service_account.Credentials.from_service_account_file(
    #     key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    # )

    # client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    client = bq.BqClient.get_bq_client()
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
    EmailTo = Variable.get("wdp_pos", deserialize_json=True)['emailTo']
    logging.info('load into big query completed')
    pu.PDHUtils.send_email(EmailTo, 'WDP Load Status (***UAT***)', 'WDP replication to UAT completed')


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

