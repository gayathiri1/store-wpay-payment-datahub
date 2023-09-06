from airflow import DAG
from datetime import datetime, date, time, timedelta
import logging
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import storage
from google.cloud import bigquery
from airflow.models.variable import Variable
from airflow import models
from datetime import datetime, timedelta
import pytz
import os
import subprocess
from subprocess import call
import re
import ast
import json
from airflow.utils.email import send_email
from airflow.exceptions import AirflowFailException
from zlibpdh import getBQClient as bq
from zlibpdh import pdh_utilities as pu



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime(2099, 12, 1),
    'start_date': datetime(2021,7,19),   # added  for python upgrade
    #'email': emailAlert,
    #'email_on_failure': True,
    #'email_on_retry': True,
    #'retries': 0,
    #'retry_delay': timedelta(minutes=5),
    # 'schedule_interval': 'NONE',
    # 'sla': timedelta(hours=1)
}


logging.info("constructing dag - using airflow as owner")

#path= Variable.get('data_navigator_quickcilver_pos',deserialize_json=True)['templatePath']
#dag = DAG('pdh_replicate_data_navigator_quickcilver_prod_to_uat', catchup=False, default_args=default_args,template_searchpath=path, schedule_interval= "20 23 * * *")   # added  for python upgrade)
#dag = DAG('pdh_replicate_data_navigator_quickcilver_prod_to_uat', catchup=False, default_args=default_args,template_searchpath=['home/us-central1-pdh-composer-ua-12e4c8b7-bucket/dag/'], schedule_interval=None)   # added  for python upgrade)

dag = DAG('pdh_replicate_src_to_target', catchup=False, default_args=default_args, schedule_interval= "30 12,13,14,22,23,01,05 * * *") 

def getConfigDetails(**kwargs):

    logging.info("getConfigDetails started")
    project_id_target = Variable.get('replicate_src_to_target',deserialize_json=True)['target_project']
    Config_file = Variable.get('replicate_src_to_target',deserialize_json=True)['table']


    partition_key= {}
    table_id = []
    encryp_flag = {}
    
    for i in Config_file:
    
        #Traversion through each element in the config_file              
        #To save the table name for furute references
        table_id.append(project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name'])
        if i['encryption'] == 'N':
            encryp_flag[project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']] = 'None'
        else:
            encryp_flag[project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']]=i['encryption']
        if i['partition_key']== 'N':   
            partition_key[project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']] = "None"             
        else:

            partition_key[project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']] = i['partition_key']

    kwargs['ti'].xcom_push(key="table_id",value=table_id)
    kwargs['ti'].xcom_push(key="encryp_flag",value=encryp_flag)
    kwargs['ti'].xcom_push(key="partition_key",value=partition_key)
    kwargs['ti'].xcom_push(key="Config_file",value=Config_file)
    kwargs['ti'].xcom_push(key="project_id_target",value=project_id_target)
    
    return True


def TriggerDML(**kwargs):
    logging.info("Queries to query : {}".format(kwargs.get('templates_dict').get('final_query')))
    encryption = kwargs.get('templates_dict').get('encryp_flag')
    project_id_target = kwargs.get('templates_dict').get('project_id_target')
    Config_file = kwargs.get('templates_dict').get('Config_file')
    #project_id_target = Variable.get('data_navigator_quickcilver_pos',deserialize_json=True)['target_project']
    #Config_file = Variable.get('data_navigator_quickcilver_pos',deserialize_json=True)['table']
    project_id_source = Variable.get('replicate_src_to_target',deserialize_json=True)['source_project']
    table_id = kwargs.get('templates_dict').get('table_id')

    insert_query = []
    delete_query = []
    triggerQueries={}
    error_count = 0
    #project_id_target = ast.literal_eval(project_id_target)
    Config_file = ast.literal_eval(Config_file)
    table_id = ast.literal_eval(table_id)
    encryption = ast.literal_eval(encryption)
    partition_key = ast.literal_eval(kwargs.get('templates_dict').get('partition_key'))
    
    logging.info ("Config_file{},table_id {},project_id_target{}  ".format(Config_file, table_id,project_id_target))
    
    for i in Config_file:
        
        #Checking if it is incremental load or Historical Load
        
        if i['load_type'] == 'I':
             			
            delete_query.append("delete from `"+project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']+'`  where file_date = (select distinct max(file_date) from `'+project_id_source+"."+i['src_dataset']+"."+i['src_table_name']+'` );')		
				
            insert_query.append("insert into `"+project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']+"` select * from `"+project_id_source+"."+i['src_dataset']+"."+i['src_table_name']+'` where file_date > (select distinct max(file_date) from `'+project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']+'` );')		
            
        else:
            
            #if Historical load : we need to determine the start date and end date
            
            if 'start_date' not in i or 'end_date' not in i:
                logging.info("Missing date range")
                return False
            
            delete_query.append("delete from `"+project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']+'`  where file_date between \''+i['start_date']+'\' and \''+i['end_date']+'\'') 
            insert_query.append("insert into `"+project_id_target+"."+i['tgt_dataset']+"."+i['tgt_table_name']+"` select * from `"+project_id_source+"."+i['src_dataset']+"."+i['src_table_name']+'` where file_date between \''+i['start_date']+'\' and \''+i['end_date']+'\'')
			
    combine = list(zip(delete_query,insert_query))
    for index in range(0,len(table_id)):
        triggerQueries[table_id[index]] = combine[index] 
    

    for key, value in triggerQueries.items():
        for val in value:
            logging.info(" Submitted : currQuery {}".format(val))
            if val.find('insert into') !=-1 and encryption.get(key) == 'None':
                result,count = _query_exection(val)
                #if result == 'True'
                    #query_success+=1
                #logging.info("result:{}".format(result))
                #logging.info("count:{}".format(count))				
            elif val.find('insert into') !=-1 and encryption.get(key) != 'None': 
                result,count = _query_exection(val,encryption.get(key))
            else :
                result,count = _query_exection(val)
                if val.find('delete') != -1 and not result:
                    error_count	+=1
                    logging.info("Delete query failed so insert operation isn't performed for this table {}".format(key))					
                    break
                else:
                    delete="Success"
                    #query_success+=1				
            if val.find('insert into')!=-1 and not result and delete=="Success":
                error_count +=1
                logging.info("error count:{}".format(error_count))				
                if partition_key.get(key) == "None":
                    newquery = 'CREATE OR REPLACE TABLE `'+key+'` AS SELECT * FROM `'+key+'` FOR SYSTEM_TIME AS OF   TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)'
                else:
                    newquery = 'CREATE OR REPLACE TABLE `'+key+'` PARTITION BY DATE_TRUNC('+partition_key.get(key)+',MONTH) SELECT * FROM `'+key+'` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)'
                logging.info ("The rollback query is :{}".format(newquery))
                logging.info ("Insert query failed and rollback performed on table {}".format(key))
                result,count = _query_exection(newquery)           
				

    if error_count == 0:
        logging.info("Trigger DML completed successfully")	
        return True
    else:
        raise AirflowException("Trigger DML failed as there is a failure of insert/delete operation")
        return False

def _query_exection(currQuery,encyp_col="None"):
    try:
        if encyp_col=="None" or currQuery.find("delete") != -1 :
            client=bq.BqClient.get_bq_client()
            #client = bigquery.Client()
            query_job = client.query(currQuery, location='US')			
            query_job.result()
            logging.info("query_job.result:{}".format(query_job.result()))			
            if query_job.num_dml_affected_rows is None:
                count_value = 0
            else:
                count_value = query_job.num_dml_affected_rows
            logging.info("Count of DML changes:{}".format(count_value))
            logging.info(" Query finished Successfully")
            return True , count_value
        else : 
            logging.info("Encyption begins here:")
    except Exception as e:
        count_value = 0
        logging.info("Exception:{}".format(e))
        return False , count_value

def executeView(**kwargs):  
    bucket_name = Variable.get("replicate_src_to_target", deserialize_json=True)['bucket_name']   
    query_file =  Variable.get("replicate_src_to_target", deserialize_json=True)['query_file']    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)             
    blob = bucket.blob(query_file)        
    text = blob.download_as_string()
    query_text = text.decode('utf-8')
    
    try:
        client = bigquery.Client()
        query_job = client.query(query_text, location='US')			
        query_job.result()
        logging.info("query_job.result:{}".format(query_job.result()))			
        logging.info(" Query finished Successfully")
        return True
    except Exception as e:
        logging.info("Exception:{}".format(e))
        raise AirflowException("Execute view failed")
        return False 
    
    
    
def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt
    
def Sendemail(**kwargs):  
        
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    subject = "UAT data refresh for Quickcilver Hourly "+loadDateTime
    emailTo = Variable.get("replicate_src_to_target", deserialize_json=True)['emailTo']
    logging.info("subject: {} emailto: {}".format(subject,emailTo))
    pu.PDHUtils.send_email(emailTo, subject,'Quickcilver replication with hourly data completed successfully')
    return True



getConfigDetails = ShortCircuitOperator(
    python_callable=getConfigDetails,
    task_id='getConfigDetails',
    depends_on_past=False,
    provide_context=True,
    dag=dag)
    
    
TriggerDML = ShortCircuitOperator(
    task_id='TriggerDML',
    dag=dag,
    python_callable=TriggerDML,
    provide_context=True,  
    templates_dict={'table_id': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='table_id')}}",'encryp_flag': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='encryp_flag')}}",'partition_key': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='partition_key')}}",
    'Config_file': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='Config_file')}}",
    'project_id_target': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='project_id_target')}}"}
)  

#executeView = BigQueryOperator(
    #task_id='executeView',
    #sql= Variable.get("data_navigator_quickcilver_pos", deserialize_json=True)['sqlFile'],
    #bigquery_conn_id='bigquery_default',
    #use_legacy_sql=False,
    #dag=dag)
    
#executeView = PythonOperator(
#    task_id='executeView',
#    dag=dag,
#    python_callable=executeView,
#    provide_context=True  # must pass this because templates_dict gets passed via context
#)    
     

Sendemail = PythonOperator(
    task_id='Sendemail',
    dag=dag,
    python_callable=Sendemail,
    provide_context=True  # must pass this because templates_dict gets passed via context
)    
 
  
    
#getConfigDetails >> TriggerDML >> executeView>> Sendemail
getConfigDetails >> TriggerDML >> Sendemail