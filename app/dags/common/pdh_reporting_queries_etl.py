from airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
import ast
import pendulum


#DATPAY-3521 UTC to Sydney timezone change
local_tz = pendulum.timezone("Australia/Sydney")
default_args = {
    'start_date': datetime(2021,7, 12, tzinfo=local_tz),    
}


logging.info("constructing dag - using airflow as owner")

try:
   control_table = Variable.get("reporting_queries", deserialize_json=True)["control_table"] 
   project_id = control_table.split(".")[0]
   if "PROD" in project_id.upper():
       dag = DAG('pdh_reporting_queries', catchup=False, default_args=default_args,schedule_interval= "45 09,10,15 * * *")
   else:
      dag = DAG('pdh_reporting_queries', catchup=False, default_args=default_args,schedule_interval= "45 22 * * *")
except Exception as e:
    logging.info("Exception in setting DAG schedule:{}".format(e)) 





def readexecuteQuery(**kwargs):

    bucket= Variable.get('reporting_queries',deserialize_json=True)['bucket']
    
    query_file=Variable.get('reporting_queries',deserialize_json=True)['files']
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    date_time=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
    environment= Variable.get('reporting_queries',deserialize_json=True)['environment']
    email_to = Variable.get("reporting_queries", deserialize_json=True)['email_to']
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

        if result==True:
            Successfull_execution.append(i['query_file'])
   
    subject_success="Reporting queries executed successfully in "+environment+" "+date_time
    body_success ="Below queries were executed successfully in "+environment+":\n\n"+str(Successfull_execution)
    pu.PDHUtils.send_email(email_to,subject_success,body_success)
    
    return True   
	
def processQuery(query,query_file,email,environment,date_time):

    try:  
        rows = " "
        client = bigquery.Client()
        logging.info("executing query")        
        query_job = client.query(query)
        rows = query_job.result()  
        logging.info("Query executed inside processQuery function")

        return True,rows
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))
        logging.info("email_to: {}".format(email))
        complete_exception = str(e)            
        body_fail ="Exception raised while executing reporting queries "+query_file+":\n\n"+complete_exception
        subject_fail ="Exception raised while executing reporting queries in "+environment+" "+date_time
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