from airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
import logging
from zlibpdh import pdh_utilities as pu
import pytz
from zlibpdh import pdh_utilities as pu
import pendulum,os


#DATPAY-3521 UTC to Sydney timezone change
local_tz = pendulum.timezone("Australia/Sydney")
#Set project_id here.
project_id = os.environ.get('PROJECT_ID',"gcp-wow-wpay-paydat-dev")
#Based on Project ID set start data here.
if "PROD" in project_id.upper():
    start_date = datetime(2024,5,13, tzinfo=local_tz)
else:
    start_date = datetime(2022,10, 1, tzinfo=local_tz)

default_args = {
    'start_date': start_date,    
}

logging.info("constructing dag - using airflow as owner")


dag = DAG('pdh_external_table_refresh_wkly', catchup=False, default_args=default_args,schedule_interval= "00 10 * * MON")


def refresh_table(**kwargs):

    gs_detail= Variable.get('external_table_det_wkly',deserialize_json=True)['gs_detail']
    drop_query= Variable.get('external_table_det_wkly',deserialize_json=True)['drop_query']
    
    for i in gs_detail:
        
        projectID,datasetID,target_table = i['gs_stgng_table'].split('.',3)
		
        logging.info("projectID {} datasetID {} target_table {} ".format(projectID,datasetID,target_table))
    
        pu.PDHUtils.upload_gs_to_bq(i['gs_unique_id'],i['gs_sheet_name'],datasetID+"."+target_table,projectID)
            
        logging.info("{} staging table created successfully".format(i['gs_stgng_table']))
        
        result_create,rows = processQuery(i['createquery'],projectID)
		
        if result_create ==True:       
            logging.info("{} main table refrehsed successfully".format(i['gs_table_name']))
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
        subject = "Exception raised while running external Weekly table refresh job "+projectID+" "+loadDateTime
        body="Exception raised while running external Weekly table refresh job in "+projectID+":"+"\n"+str(e)
        emailTo = Variable.get("external_table_det_wkly", deserialize_json=True)['emailTo']
        logging.info("subject: {} emailto: {}".format(subject,emailTo))
        pu.PDHUtils.send_email(emailTo, subject,body)        
        #raise AirflowException("extract from staging failed")
        return False,rows	

        
def sendEmail(**kwargs):  
    projectID = kwargs.get('templates_dict').get('projectID')    
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    subject = "External Weekly tables refresh dag ran successfully "+loadDateTime
    body="External Weekly tables refresh dag ran successfully in "+projectID
    emailTo = Variable.get("external_table_det_wkly", deserialize_json=True)['emailTo']
    logging.info("subject: {} emailto: {}".format(subject,emailTo))
    pu.PDHUtils.send_email(emailTo, subject,body)
    return True   
    
def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt  
    
refresh_table = ShortCircuitOperator(
    task_id='refresh_table',
    dag=dag,
    python_callable=refresh_table,
    provide_context=True  # must pass this because templates_dict gets passed via context
)  
  
sendEmail = PythonOperator(
    task_id='sendEmail',
    dag=dag,
    python_callable=sendEmail,
    provide_context=True,  # must pass this because templates_dict gets passed via context
    templates_dict={ 'projectID': "{{ task_instance.xcom_pull(task_ids='refresh_table', key='projectID') }}"})
                      #'unupdated_rules': "{{ task_instance.xcom_pull(task_ids='refresh_table', key='unupdated_rules') }}"  
                   #})    	
    
refresh_table >> sendEmail
	