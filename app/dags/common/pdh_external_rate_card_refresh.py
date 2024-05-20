from airflow import DAG
from google.cloud import bigquery,pubsub_v1
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
from zlibpdh import pdh_utilities as pu
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
    'retry_delay': timedelta(9000),
    'retries': 0,
    'max_active_runs': 1,  
}


logging.info("constructing dag - using airflow as owner")
dag = DAG('pdh_external_rate_card_refresh', 
          catchup=False, 
          default_args=default_args,
          schedule_interval=None
          )
dag_name = "pdh_external_rate_card_refresh"

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
#logging.info("project_id:{}".format(project_id))

topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
logging.info(f"topic_path => {topic_path}")
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))


def refresh_rate_card_table(**kwargs):

    gs_detail= Variable.get('external_rate_card_det',deserialize_json=True)['gs_detail']
    drop_query= Variable.get('external_rate_card_det',deserialize_json=True)['drop_query']
    
    for i in gs_detail:
        
        projectID,datasetID,target_table = i['gs_stgng_table'].split('.',3)		
        logging.info("projectID {} datasetID {} target_table {} ".format(projectID,datasetID,target_table))    
        pu.PDHUtils.upload_gs_to_bq(i['gs_unique_id'],i['gs_sheet_name'],datasetID+"."+target_table,projectID)            
        logging.info("{} staging table created successfully".format(i['gs_stgng_table']))        
        result_create,rows = processQuery(i['createquery'],projectID)
		
        if result_create ==True:       
            logging.info("{} main table refreshed successfully".format(i['gs_table_name']))
            result_drop,rows = processQuery(drop_query.replace('tbl_name',i['gs_stgng_table']),projectID)		
            if result_drop ==True:
                logging.info("{} staging table dropped successfully".format(i['gs_stgng_table']))
            
    kwargs['ti'].xcom_push(key="projectID", value=projectID) 
 
    return True
    
def processQuery(query,projectID):
	
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        rows = query_job.result()  # Waits for the query to finish        
        return True,rows
    except Exception as e:
        rows=''
        logging.info("Exception:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
        subject = "Exception raised while running external rate card refresh job "+projectID+" "+loadDateTime
        body="Exception raised while running external rate card refresh job in "+projectID+":"+"\n"+str(e)
        emailTo = Variable.get("external_rate_card_det", deserialize_json=True)['emailTo']
        logging.info("subject: {} emailto: {}".format(subject,emailTo))
        pu.PDHUtils.send_email(emailTo, subject,body)        
        #raise AirflowException("extract from staging failed")
        event_message = f"Exception raised while running External rate card refresh job in {projectID}:\n{str(e)}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return False,rows	

        
def sendEmail(**kwargs):  
    projectID = kwargs.get('templates_dict').get('projectID')    
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    subject = "External rate card refresh dag ran successfully "+loadDateTime
    body="External rate card refresh dag ran successfully in "+projectID
    emailTo = Variable.get("external_rate_card_det", deserialize_json=True)['emailTo']
    logging.info("subject: {} emailto: {}".format(subject,emailTo))
    pu.PDHUtils.send_email(emailTo, subject,body)
    event_message = f"External rate card refresh dag ran successfully in {projectID} at {execTimeInAest.strftime('%Y-%m-%d %H:%M:%S')}"
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
    
refresh_rate_card_table = ShortCircuitOperator(
    task_id='refresh_rate_card_table',
    dag=dag,
    python_callable=refresh_rate_card_table,
    provide_context=True  # must pass this because templates_dict gets passed via context
)  
  
sendEmail = PythonOperator(
    task_id='sendEmail',
    dag=dag,
    python_callable=sendEmail,
    provide_context=True,  # must pass this because templates_dict gets passed via context
    templates_dict={ 'projectID': "{{ task_instance.xcom_pull(task_ids='refresh_rate_card_table', key='projectID') }}"})
                      #'unupdated_rules': "{{ task_instance.xcom_pull(task_ids='refresh_rate_card_table', key='unupdated_rules') }}"  
                   #})    	
    
refresh_rate_card_table >> sendEmail
	
	
	
