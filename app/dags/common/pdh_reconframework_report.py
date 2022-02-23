from airflow import DAG
from datetime import datetime, date, time, timedelta
import logging
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from google.cloud import bigquery
from airflow.models.variable import Variable
import pytz
import re
from airflow.exceptions import AirflowFailException
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


dag = DAG('pdh_reconframework_report', catchup=False, default_args=default_args, schedule_interval= "30 00 * * *") 

#dag = DAG('pdh_reconframework_report', catchup=False, default_args=default_args, schedule_interval= None) 

def executeView(**kwargs):  
    bucket_name = Variable.get("reconframework_report", deserialize_json=True)['bucket_name']   
    query_file =  Variable.get("reconframework_report", deserialize_json=True)['query_file']    
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
    subject = "Reconcilation framework executed in prod "+loadDateTime
    emailTo = Variable.get("reconframework_report", deserialize_json=True)['emailTo']
    logging.info("subject: {} emailto: {}".format(subject,emailTo))
    pu.PDHUtils.send_email(emailTo, subject,'Reconcilation framework executed successfully in prod')
    return True



    
    


executeView = PythonOperator(
    task_id='executeView',
    dag=dag,
    python_callable=executeView,
    provide_context=True  # must pass this because templates_dict gets passed via context
)    
     

Sendemail = PythonOperator(
    task_id='Sendemail',
    dag=dag,
    python_callable=Sendemail,
    provide_context=True  # must pass this because templates_dict gets passed via context
)    
 
  
    
executeView>> Sendemail