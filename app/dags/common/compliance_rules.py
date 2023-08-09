from airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
from zlibpdh import pdh_utilities as pu

default_args = {
    'start_date': datetime(2021,7, 12),    
}

logging.info("constructing dag - using airflow as owner")


dag = DAG('compliance_rules', catchup=False, default_args=default_args,schedule_interval= "00 05 * * *")


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
	
	
	