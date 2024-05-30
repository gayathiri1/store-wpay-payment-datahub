from airflow import DAG
from google.cloud import storage,pubsub_v1
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
import pendulum
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json,os


#DATPAY-3521 UTC to Sydney timezone change
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

default_args = {
    'start_date': start_date,
    'max_active_runs': 1,
    'retry_delay': timedelta(9000),
    'retries': 0,  
}

logging.info("constructing dag - using airflow as owner")


dag_name = "pdh_looker_nrt_payout_queries"
try:
    if IS_PROD:
        dag = DAG('pdh_looker_nrt_payout_queries', catchup=False, default_args=default_args,schedule_interval= "59 07-19 * * *")
    else:
        dag = DAG('pdh_looker_nrt_payout_queries', catchup=False, default_args=default_args,schedule_interval= "00 18 * * *")
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

    bucket= Variable.get('looker_nrt_payout_queries',deserialize_json=True)['bucket']
    
    query_file=Variable.get('looker_nrt_payout_queries',deserialize_json=True)['files']
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    date_time=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
    environment= Variable.get('looker_nrt_payout_queries',deserialize_json=True)['environment']
    email_to = Variable.get("looker_nrt_payout_queries", deserialize_json=True)['email_to']
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
        event_message = "looker_nrt_payout_queries were executed successfully in "+environment+": "+str(Successfull_execution) + " at " + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    return True   
	
def processQuery(query,query_file,email,environment,date_time):

    try:  
        #count=0
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
        body_fail ="Exception raised while executing looker_nrt_payout queries "+query_file+":\n\n"+complete_exception
        subject_fail ="Exception raised while executing looker_nrt_payout queries in "+environment+" "+date_time
        logging.info("email body text: {} ".format(body_fail))
        pu.PDHUtils.send_email(email, subject_fail,body_fail)
        event_message = "Exception raised while executing looker_nrt_payout queries "+query_file+":\n\n"+complete_exception
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
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