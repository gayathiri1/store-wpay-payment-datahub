from airflow import DAG
from google.cloud import storage,pubsub_v1
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
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
    'retry_delay': timedelta(9000),
    'retries': 0,
    'max_active_runs': 1,   
}


logging.info("constructing dag - using airflow as owner")
dag_name = "compliance_rules"

try:
    if IS_PROD:
        dag = DAG('compliance_rules', catchup=False, default_args=default_args,schedule_interval= "00 05 * * *")
    else:
        dag = DAG('compliance_rules', catchup=False, default_args=default_args,schedule_interval= "10 22 * * *")        
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

    gs_unique_id= Variable.get('compliance_rules',deserialize_json=True)['gs_unique_id']
    gs_sheet_name= Variable.get('compliance_rules',deserialize_json=True)['gs_sheet_name']
    gs_table_name= Variable.get('compliance_rules',deserialize_json=True)['gs_table_name']
    projectID,datasetID,target_table = gs_table_name.split('.',3)
    logging.info("projectID {} datasetID {} target_table {} ".format(projectID,datasetID,target_table))
    
    pu.PDHUtils.upload_gs_to_bq(gs_unique_id,gs_sheet_name,datasetID+"."+target_table,projectID)
    
    logging.info("{} staging table created successfully".format(gs_table_name))
    
    createquery=Variable.get('compliance_rules',deserialize_json=True)['createquery']
    
    processQuery(createquery)
    
    logging.info("Main table refreshed successfully")
        
    bucket= Variable.get('compliance_rules',deserialize_json=True)['bucket']
    
    query_file=Variable.get('compliance_rules',deserialize_json=True)['files']
    
    environment= Variable.get('compliance_rules',deserialize_json=True)['environment']
    
    for i in query_file:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)             
        blob = bucket.blob(i['query_file'])        
        text = blob.download_as_string()
        query_text = text.decode('utf-8')
        processQuery(query_text)
        logging.info("Query executed for {}".format(i['query_file']))
    return True   
	
def processQuery(query):
	
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        query_job.result()  # Waits for the query to finish
        #Sendemail("Compliance rules airflow ","Compliance rules airflow completed successfully")        
        return True
    except Exception as e:
        logging.info("Exception:{}".format(e))
        #Sendemail("Compliance rules airflow failure","Compliance rules airflow failed, Please check the log")
        event_message = f"Exception raised while executing compliance rules :{str(e)}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        raise AirflowException("extract to staging failed")
        return False
        
def sendEmail(**kwargs):  
        
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    environment= Variable.get('compliance_rules',deserialize_json=True)['environment']
    subject = environment + " "+"Compliance rules airflow "+loadDateTime
    emailTo = Variable.get("compliance_rules", deserialize_json=True)['emailTo']
    logging.info("subject: {} emailto: {}".format(subject,emailTo))
    pu.PDHUtils.send_email(emailTo, subject,"Compliance rules airflow completed successfully in "+environment)    
    logging.info("execTimeInAest: {}".format(execTimeInAest))
    event_message = "Compliance rules query executed successfully at" + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
    event = Event(
        dag_name=dag_name,
        event_status="success",
        event_message=event_message,
        start_time=exec_time_aest,)
    publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
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
    provide_context=True  # must pass this because templates_dict gets passed via context
)  
  
sendEmail = PythonOperator(
    task_id='sendEmail',
    dag=dag,
    python_callable=sendEmail,
    provide_context=True  # must pass this because templates_dict gets passed via context
)    	
    
readexecuteQuery >> sendEmail
	
	
	