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

default_args = {
    'start_date': datetime(2021,7, 12),    
}

logging.info("constructing dag - using airflow as owner")


dag = DAG('pdh_billing_engine_etl', catchup=False, default_args=default_args,schedule_interval= "00 23 * * *")


def readexecuteQuery(**kwargs):

    bucket= Variable.get('billing_engine_etl',deserialize_json=True)['bucket']
    
    query_file=Variable.get('billing_engine_etl',deserialize_json=True)['files']

    for i in query_file:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)             
        blob = bucket.blob(i['query_file'])        
        text = blob.download_as_string()
        query_text = text.decode('utf-8')
        logging.info("Query fetched from bucket")
        result, rows = processQuery(query_text)
        logging.info("rows {} and result {}".format(rows, result))
        logging.info("Query executed for {}".format(i['query_file']))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        logging.info("execTimeInAest: {}".format(execTimeInAest))
        email_id = i['email']
        logging.info("email_id: {} ".format(email_id))   
        body ="Billing engine query executed successfully.\n\nQuery file: "+i['query_file']
        logging.info("body: {} ".format(body)) 
        Sendemail(email_id,"Billing engine query executed successfully",body,execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))        
    return True   
	
def processQuery(query):
	
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        rows = " "
        rows = query_job.result()  
        logging.info("Query executed inside processQuery function")
        return True,rows
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        logging.info("execTimeInAest: {}".format(execTimeInAest))
        email_to = Variable.get("billing_engine_etl", deserialize_json=True)['email_to']
        logging.info("email_to: {}".format(email_to))
        complete_exception = str(e)        
        exception_text = Variable.get("billing_engine_etl", deserialize_json=True)['exception_text']        
        if exception_text in complete_exception:
            body= "Exception raised while executing billing query :\n\n"+complete_exception.split(exception_text)[0]
            logging.info("Splitting exception text and formatting email body text: {} ".format(body))
        else:            
            body ="Exception raised while executing billing query :\n\n"+complete_exception
            logging.info("email body text: {} ".format(body))
        Sendemail(email_to,"Exception raised while executing billing query",body,execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        raise AirflowException("Exception raised in ETL query please check")
        return False,rows
        
def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt       
        
def Sendemail(emailid,email_subject, email_body,execTimeInAest):  
        
    subject = email_subject+" "+execTimeInAest
    logging.info("subject: {} emailto: {}".format(subject,emailid))
    pu.PDHUtils.send_email(emailid, subject,email_body)
    return True
        

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
	