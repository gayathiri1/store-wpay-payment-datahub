from airflow import DAG
from google.cloud import storage,pubsub_v1
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
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json,os


#Fix to handle daylight savings
local_tz = pendulum.timezone("Australia/Sydney")

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

default_args = {
    'start_date': start_date,
    'retry_delay': timedelta(9000),
    'retries': 0,
    'max_active_runs': 1,  
}


logging.info("constructing dag - using airflow as owner")
dag_name = "pdh_billing_engine_etl"



try:
    if IS_PROD:
        dag = DAG('pdh_billing_engine_etl', catchup=False, default_args=default_args,schedule_interval= "00 08 * * *")
    else:
        dag = DAG('pdh_billing_engine_etl', catchup=False, default_args=default_args,schedule_interval= "00 16 * * *")
except Exception as e:
    logging.info("Exception in setting DAG schedule:{}".format(e))





# https://stackoverflow.com/a/70397050/482899
#log_prefix = f"[pdh_batch_pipeline][{dag_name}]"
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




def readexecuteQuery(**kwargs):
    logging.info("billing_engine_etl DAG timezone is {}".format(dag.timezone))
    try:
        bucket= Variable.get('billing_engine_etl',deserialize_json=True)['bucket']
        query_file=Variable.get('billing_engine_etl',deserialize_json=True)['files']
    except Exception as e:
        logging.info("Exception raised in readexecuteQuery while reading variables:{}".format(e))

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
        event_message = "Billing engine query executed successfully at" + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
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
        logging.info("Exception Raised :{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        logging.info("execTimeInAest: {}".format(execTimeInAest))
        email_to = Variable.get("billing_engine_etl", deserialize_json=True)['email_to']
        logging.info("email_to: {}".format(email_to))
        complete_exception = str(e)        
        exception_text = Variable.get("billing_engine_etl", deserialize_json=True)['exception_text']        
        if exception_text in complete_exception:
            body= "Exception raised while executing billing query :\n\n"+complete_exception.split(exception_text)[0]
            logging.info("Splitting exception text and formatting email body text: {} ".format(body))
            Sendemail(email_to,"Exception raised while executing billing query",body,execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
            event_message = "Exception raised while executing billing query :\n\n"+complete_exception.split(exception_text)[0]
            event = Event(
               dag_name=dag_name,
               event_status="failure",
               event_message=event_message,
               start_time=exec_time_aest,
               )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        else:            
            body ="Exception raised while executing billing query :\n\n"+complete_exception
            logging.info("email body text: {} ".format(body))
            Sendemail(email_to,"Exception raised while executing billing query",body,execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
            event_message = "Exception raised while executing billing query :\n\n"+complete_exception
            event = Event(
                dag_name=dag_name,
                event_status="failure",
                event_message=event_message,
                start_time=exec_time_aest,)
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        raise AirflowException("Exception raised in ETL query please check")
        return False,rows
        
def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt       
        
def Sendemail(emailid,email_subject, email_body,execTimeInAest):
    try:
        subject = email_subject+" "+execTimeInAest
        logging.info("subject: {} emailto: {}".format(subject,emailid))
        pu.PDHUtils.send_email(emailid, subject,email_body)
    except Exception as e:
        logging.info("Exception raised in Sendemail while sending email:{}".format(e))
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
	