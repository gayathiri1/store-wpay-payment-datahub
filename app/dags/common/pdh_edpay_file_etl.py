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
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json,os
import os.path
from google.cloud import pubsub_v1


#DATPAY-3521 UTC to Sydney timezone change
local_tz = pendulum.timezone("Australia/Sydney")
#Set project_id here.
project_id = os.environ.get('PROJECT_ID',"gcp-wow-wpay-paydat-dev")
#Based on Project ID set start data here.
if "PROD" in project_id.upper():
    start_date = datetime(2024,5,10, tzinfo=local_tz)
else:
    start_date = datetime(2021,7, 12, tzinfo=local_tz)

default_args = {
    'start_date': start_date,    
}


logging.info("constructing dag - using airflow as owner")
dag_name = "pdh_edpay_file_etl"

dag = DAG('pdh_edpay_file_etl', catchup=False, default_args=default_args,schedule_interval= "00 15 * * Fri")


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
#get the project_id
project_id=None
if  os.environ.get("GCP_PROJECT", "no project name"):
    project_id = os.environ.get("GCP_PROJECT", "no project name")
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))


def readexecuteQuery(**kwargs):

    bucket= Variable.get('edpay_file_gen_etl',deserialize_json=True)['bucket']
    query_file=Variable.get('edpay_file_gen_etl',deserialize_json=True)['query_file']
    qc_select = Variable.get('edpay_file_gen_etl',deserialize_json=True)['qc_select']
    emailTo = Variable.get("edpay_file_gen_etl", deserialize_json=True)['emailTo']
    merchant= Variable.get("edpay_file_gen_etl", deserialize_json=True)['merchant']
    updated_files =[]
    unupdated_files =[]
    error_count = 0
    updated_query=[]
    unupdated_query=[]
    updated_codes_ctrl_tab=[]

    
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")

    row =''
    rows1=''
    try:  
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)             
        blob = bucket.blob(query_file)        
        text = blob.download_as_string()
        query_text = text.decode('utf-8')
        result, rows = processQuery(query_text,emailTo)

        logging.info("rows {} and result {}".format(rows, result))
        logging.info("Query executed for {}".format(query_file))
        result, rows1 = processQuery(qc_select,emailTo)
        logging.info("Control table record is fetched for file {} and merchant  {}".format(query_file,merchant))
        logging.info("rows1 {} and result {}".format(rows1, result))
        logging.info("row before for loop  {}".format(row))
        for row in rows1:
            updated_codes_ctrl_tab.append(row[2])
            logging.info("updated_codes_ctrl_tab {}".format(updated_codes_ctrl_tab))
            logging.info("row inside for loop {}".format(row))  
            execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
            logging.info("execTimeInAest: {}".format(execTimeInAest))    
            logging.info("row after for loop {}".format(row))  
        if row is None or row == '':
            logging.info("row inside if with None  {}".format(row))
            error_count += 1
            unupdated_files.append(merchant)
            unupdated_query.append(query_file)
            logging.info("unupdated_files {}".format(unupdated_files)) 
            logging.info("unupdated_query {}".format(unupdated_query)) 
        elif row is not None:
            logging.info("row inside if with not None   {}".format(row))
            updated_files.append(merchant)
            updated_query.append(query_file)
            logging.info("updated_query {}".format(updated_query))
            logging.info("updated_files {}".format(updated_files))  
        event_message = "readexecuteQuery step executed successfully" + execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            
    except Exception as e:  
        logging.info("Exception:{}".format(e))
        subject = "Exception raised while executing edpay_file_gen_etl, in task readexecuteQuery"+execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        body= "Exception raised while executing edpay_file_gen_etl, in task readexecuteQuery: \n"+str(e)
        pu.PDHUtils.send_email(emailTo, subject,body)
        error_count += 1
        unupdated_files.append(merchant)
        unupdated_query.append(query_file)
        event_message = "Exception raised while executing edpay_file_gen_etl, in task readexecuteQuery: \n"+str(e)
        event = Event(
               dag_name=dag_name,
               event_status="failure",
               event_message=event_message,
               start_time=exec_time_aest,
               )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return False

            
    kwargs['ti'].xcom_push(key="updated_codes_ctrl_tab", value=updated_codes_ctrl_tab)
    kwargs['ti'].xcom_push(key="updated_query", value=updated_query)
    kwargs['ti'].xcom_push(key="unupdated_query", value=unupdated_query) 
    kwargs['ti'].xcom_push(key="unupdated_files", value=unupdated_files)
    kwargs['ti'].xcom_push(key="updated_files", value=updated_files)
    kwargs['ti'].xcom_push(key="error_count", value=error_count)
    return True   
	
def processQuery(query,emailTo):
	
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        rows = " "
        rows = query_job.result()  
        return True,rows
    except Exception as e:
        logging.info("Exception:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        subject = "Exception raised while executing edpay_file_gen_etl in processQuery"+execTimeInAest.strftime("%Y-%m-%d %H:%M:%S")
        body= "Exception raised while executing edpay_file_gen_etl in processQuery: \n"+str(e)
        pu.PDHUtils.send_email(emailTo, subject,body)
        event_message = "Exception raised while executing edpay_file_gen_etl in processQuery: \n"+str(e)
        event = Event(
                dag_name=dag_name,
                event_status="failure",
                event_message=event_message,
                start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return False,rows
        
def sendEmail(**kwargs):  
    error_count = int(kwargs.get('templates_dict').get('error_count'))
    updated_files = str(kwargs.get('templates_dict').get('updated_files'))
    logging.info("updated_files:{}".format(updated_files))
    unupdated_files = str(kwargs.get('templates_dict').get('unupdated_files'))
    logging.info("unupdated_files:{}".format(unupdated_files))
    updated_codes_ctrl_tab = str(kwargs.get('templates_dict').get('updated_codes_ctrl_tab'))
    logging.info("updated_codes_ctrl_tab:{}".format(updated_codes_ctrl_tab))
    updated_query = str(kwargs.get('templates_dict').get('updated_query'))
    logging.info("updated_query:{}".format(updated_query))
    unupdated_query = str(kwargs.get('templates_dict').get('unupdated_query'))
    logging.info("unupdated_query:{}".format(unupdated_query))
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    subject = "File generation ETL "+loadDateTime
    emailTo = Variable.get("edpay_file_gen_etl", deserialize_json=True)['emailTo']
    if  error_count ==0:
        bodytext = "Control table is updated for merchant codes: "+updated_codes_ctrl_tab+"\n\nQuery files executed successfully: "+updated_query+"\n\nMerchant codes in query file: "+updated_files
        event_message = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files
        event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
                )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8")) 
    else:
        bodytext = "Control table is updated for merchant codes: "+updated_codes_ctrl_tab+"\n\nQuery files executed successfully: "+updated_query+"\n\nMerchant codes in query file: "+updated_files+"\n\nControl table is not updated for merchant codes: "+unupdated_files+"\n\nUnsuccessful query files: "+unupdated_query
        event_message = "Control table is updated for merchant codes: "+updated_codes_ctrl_tab+"\n\nQuery files executed successfully: "+updated_query+"\n\nMerchant codes in query file: "+updated_files+"\n\nControl table is not updated for merchant codes: "+unupdated_files+"\n\nUnsuccessful query files: "+unupdated_query
        event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))    
    logging.info("subject: {} emailto: {} bodytext: {}".format(subject,emailTo,bodytext))
    pu.PDHUtils.send_email(emailTo, subject,bodytext)
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
  
sendEmail = PythonOperator(
    task_id='sendEmail',
    dag=dag,
    python_callable=sendEmail,
    provide_context=True,  
    templates_dict={ 'updated_files': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_files') }}",
                      'unupdated_files': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='unupdated_files') }}" ,
                      'error_count': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='error_count') }}" ,
                      'updated_codes_ctrl_tab': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_codes_ctrl_tab') }}" ,
                      'updated_query': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_query') }}" ,
                      'unupdated_query': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='unupdated_query') }}"                        
                   }
)    	

triggerdag = TriggerDagRunOperator(
    task_id="triggerdag",
    trigger_dag_id="pdh_edpay_file_generation", 
    dag=dag,
)
    
readexecuteQuery >> triggerdag >> sendEmail 