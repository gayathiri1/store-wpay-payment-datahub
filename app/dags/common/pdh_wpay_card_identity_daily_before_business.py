from airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
import ast
import pendulum
import os


local_tz = pendulum.timezone("Australia/Sydney")

#Set project_id here.
project_id = os.environ.get('PROJECT_ID',"gcp-wow-wpay-paydat-dev")
#Based on Project ID set start data here.
if "PROD" in project_id.upper():
    start_date = datetime(2024,5,15, tzinfo=local_tz)
else:
    start_date = datetime(2024,5,12, tzinfo=local_tz)

default_args = {
    'start_date': start_date,
    'retry_delay': timedelta(9000),
    'retries': 0,
    'max_active_runs': 1,   
}


logging.info("constructing dag - using airflow as owner")


dag = DAG('pdh_wpay_card_identity_daily_before_business', catchup=False, default_args=default_args,schedule_interval= "0 5 * * *")


def readexecuteQuery(**kwargs):

    bucket= Variable.get('wpay_card_identity_daily_before_business',deserialize_json=True)['bucket']
    
    query_file=Variable.get('wpay_card_identity_daily_before_business',deserialize_json=True)['files']
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    date_time=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
    environment= Variable.get('wpay_card_identity_daily_before_business',deserialize_json=True)['environment']
    email_to = Variable.get("wpay_card_identity_daily_before_business", deserialize_json=True)['email_to']
    Successfull_execution =[]
    for i in query_file:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)             
        blob = bucket.blob(i['query_file'])        
        text = blob.download_as_string()
        query_text = text.decode('utf-8')
        logging.info("Query fetched from bucket")
        result, rows = processQuery(query_text,i['query_file'],email_to,environment,date_time)
        logging.info("rows {} and result {}".format(rows, result))
        logging.info("Query executed for {}".format(i['query_file']))
        #print("count "+str(count))
        if result==True:
            Successfull_execution.append(i['query_file'])
        logging.info("Length of Successfull_execution  {}".format(len(Successfull_execution)))
        if len(Successfull_execution) > 0:
            subject_success="WPAY Card Identity Daily Before Business Query executed successfully in "+environment+" "+date_time
            body_success ="Below queries were executed successfully in "+environment+":\n\n"+str(Successfull_execution)
            pu.PDHUtils.send_email(email_to,subject_success,body_success)
    
    return True   
	
def processQuery(query,query_file,email,environment,date_time):

    try:  
        #count=0
        rows = " "
        client = bigquery.Client()
        logging.info("executing query")        
        query_job = client.query(query)
        rows = query_job.result()  
        logging.info("WPAY Card Identity Daily Query executed inside processQuery function")
        return True,rows
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))
        logging.info("email_to: {}".format(email))
        complete_exception = str(e)            
        body_fail ="Exception raised while executing WPAY Card Identity Daily Before Business Query "+query_file+":\n\n"+complete_exception
        subject_fail ="Exception raised while executing WPAY Card Identity Daily Before Business query in "+environment+" "+date_time
        logging.info("email body text: {} ".format(body_fail))
        pu.PDHUtils.send_email(email, subject_fail,body_fail)
        return False,rows
        
        

        
def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt       
        

       
    
readexecuteQuery = ShortCircuitOperator(
    task_id='readexecuteQuery',
    dag=dag,
    python_callable=readexecuteQuery,
    provide_context=True  
)  
 
    
readexecuteQuery