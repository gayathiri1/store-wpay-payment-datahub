from airflow import DAG
from google.cloud import storage,pubsub_v1
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
import pendulum
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json


#DATPAY-3521 UTC to Sydney timezone change
local_tz = pendulum.timezone("Australia/Sydney")
default_args = {
    'start_date': datetime(2021,11,25, tzinfo=local_tz),    
}

logging.info("constructing dag - using airflow as owner")


dag_name = "pdh_validate_gsheet_store_data"
try:
    control_table = Variable.get("validate_gsheet_store_data", deserialize_json=True)["control_table"]
    project_id = control_table.split(".")[0]
    if "PROD" in project_id.upper():
        dag = DAG('pdh_validate_gsheet_store_data', catchup=False, default_args=default_args,schedule_interval= "45 03 * * *")
    else:
        dag = DAG('pdh_validate_gsheet_store_data', catchup=False, default_args=default_args,schedule_interval= "00 14 * * *")
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

def get_project_id():
    """
    Get GCP project_id from airflow variable, which has been configured in control_table
    """
    control_table = Variable.get("validate_gsheet_store_data", deserialize_json=True)["control_table"]
    project_id = control_table.split(".")[0]
    logger.debug(f"project_id ={project_id}")
    return project_id


publisher = pubsub_v1.PublisherClient()
project_id = get_project_id()
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))





def readexecuteQuery(**kwargs):

    bucket= Variable.get('validate_gsheet_store_data',deserialize_json=True)['bucket']
    
    query_file=Variable.get('validate_gsheet_store_data',deserialize_json=True)['files']
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    date_time=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
    environment= Variable.get('validate_gsheet_store_data',deserialize_json=True)['environment']
    email_to = Variable.get("validate_gsheet_store_data", deserialize_json=True)['email_to']
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
        event_message = "validate_gsheet_store_data queries were executed successfully in "+environment+": "+str(Successfull_execution) + " at " + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    #calling is_gsheet_valid functionality here.
    err_res,err_rows = is_gsheet_valid(email_to,environment,date_time)
    if err_res == True:
        logging.info("**** Entries found in the sap_missing_store_log table. ***")
        store_id_list, error_msg, issue_list = [], [], []
        res_issue_dict = {}
        #loop to get store ids and issue list from log table.
        for qcount in err_rows:
            store_id_list.append(qcount[1])
            error_msg.append(qcount[3])
        issue_list = [(key, value) for i, (key, value) in enumerate(zip(store_id_list, error_msg))]
        res_issue_dict = dict(issue_list)
        #send email with details.
        body_issue ="Issue with the gsheets data for store_ids "+str(store_id_list)+":\n\n"+str(res_issue_dict)
        subject_issue ="Issue with the gsheets data in "+environment+" at "+date_time
        logging.info("email body text: {} ".format(body_issue))
        pu.PDHUtils.send_email(email_to, subject_issue,body_issue)
        event_message = "Issue with the gsheets data for store_ids "+str(store_id_list)+":\n\n"+str(res_issue_dict)
        event = Event(
            dag_name=dag_name,
            event_status="gsheet_issue",
            event_message=event_message,
            start_time=exec_time_aest,
               )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    else:
        logging.info("**** No Entries found in the sap_missing_store_log table. ***")
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
        body_fail ="Exception raised while executing validate_gsheet_store_data queries "+query_file+":\n\n"+complete_exception
        subject_fail ="Exception raised while executing validate_gsheet_store_data queries in "+environment+" "+date_time
        logging.info("email body text: {} ".format(body_fail))
        pu.PDHUtils.send_email(email, subject_fail,body_fail)
        event_message = "Exception raised while executing validate_gsheet_store_data queries "+query_file+":\n\n"+complete_exception
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


def is_gsheet_valid(email,environment,date_time):
    gsheet_status=True
    try:
        rows = " "
        client = bigquery.Client()
        logging.info("executing sap_missing_store_log query")
        query = """SELECT * FROM `{}.pdh_analytics_ds.sap_missing_store_log`\
                   WHERE safe_cast(create_datetime as date) = current_date('Australia/Sydney')\
                   AND create_by IN ('Validate_Store','Validate_Store_Mapping');"""\
                 .format(project_id)
        query_job = client.query(query)
        rows = query_job.result()        
        logging.info("Query executed inside sendAlert function")
        return gsheet_status,rows
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))
        logging.info("email_to: {}".format(email))
        complete_exception = str(e)
        gsheet_status = False           
        body_fail ="Exception raised while executing validate alert query "+query+":\n\n"+complete_exception
        subject_fail ="Exception raised while executing validate_gsheet_store_data queries in "+environment+" at "+date_time
        logging.info("email body text: {} ".format(body_fail))
        pu.PDHUtils.send_email(email, subject_fail,body_fail)
        event_message = "Exception raised while executing validate alert query "+query+":\n\n"+complete_exception
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))        
        return gsheet_status,rows


        

       
    
readexecuteQuery = ShortCircuitOperator(
    task_id='readexecuteQuery',
    dag=dag,
    python_callable=readexecuteQuery,
    provide_context=True  
)  
 
    
readexecuteQuery