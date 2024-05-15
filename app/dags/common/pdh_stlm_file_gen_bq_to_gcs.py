import os
from airflow import DAG,models,AirflowException
from datetime import datetime, date, time, timedelta, tzinfo
import logging
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage, pubsub_v1
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
import datetime as dttime
import pytz
import ast
from zlibpdh import pdh_utilities as pu
from zlibpdh import sub_utilities as su
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import time
import json,os
import pendulum

#Fix to handle daylight savings
local_tz = pendulum.timezone("Australia/Sydney")

#Set project_id here.
project_id = os.environ.get('PROJECT_ID',"gcp-wow-wpay-paydat-dev")
#Based on Project ID set start data here.
if "PROD" in project_id.upper():
    start_date = datetime(2024,5,15, tzinfo=local_tz)
else:
    start_date = datetime(2024,5,12, tzinfo=local_tz)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(9000),
    'retries': 0,  
    'start_date': start_date
}

logging.info("constructing dag - using airflow as owner")

dag_name = "pdh_stlm_file_gen_bq_to_gcs"
dag = DAG(
           'pdh_stlm_file_gen_bq_to_gcs', 
           catchup=False,
           default_args=default_args,
           max_active_runs=1,
           schedule_interval= None )

# https://stackoverflow.com/a/70397050/482899
#log_prefix = f"[pdh_batch_pipeline][{dag_name}]"
log_prefix = "[pdh_batch_pipeline]"+"["+dag_name+"]"
exec_time_aest = get_current_time_str_aest()

gcs_bucket = os.environ.get("GCS_BUCKET")


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        #return f"{log_prefix} {msg}", kwargs
        return log_prefix+" "+msg, kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")


publisher = pubsub_v1.PublisherClient()
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables?
topic_path = publisher.topic_path(project_id, topic_id)
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))


def getConfigDetails(**kwargs):

    try:
        control_table= Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['control_table']
        qs_ctrltab=Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['qs_ctrltab']
        qc_ctrltab=Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['qc_ctrltab']
        config_file = Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['parameter']
    except Exception as e:
        logging.info("Exception raised in getConfigDetails while reading variables:{}".format(e))
    
    merchant =" "
    file_date =" "
    batch_no=" "
    count=0
    rownum=0
    totcount=0
    table_id = []    
    destination_cloud_storage_uri =[]
    output_file_name = []
    email=[]
    merchant_code =[]
    header=[]
    delimiter=[]
    extension=[]
    lck_file=[]
    qufile_date=[]
    trigger_file=[]
    batch_number=[]
    split_etl=[]
    split_merchant=[]
    email_attachment=[]
    merchant_min_count = []
    email_to = Variable.get("stlm_file_gen_bq_to_gcs", deserialize_json=True)['email_to']
    errorcount = 0
    TotalRecordsExported = 0
    

    
    execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
    
    result,rows = query_execution(qs_ctrltab)
    logging.info("select control table query executed successfully")
    result,rowc = query_execution(qc_ctrltab)
    logging.info("select count query executed successfully")
    
    for qcount in rowc:
        totcount = qcount[0] 
        logging.info("total count: {}".format(totcount))
    
    for row in rows:    
        batch_no = row[0]
        merchant = row[2] 
        extract_start_datetime=str(row[5])
        extract_end_datetime=str(row[6])
        file_date=str(row[7])

        logging.info("merchant:{}".format(merchant))
        logging.info("file_date:{}".format(file_date))
        logging.info("Selected rows:{}".format(row))
        rownum += 1
        logging.info("Row Number {}".format(rownum))
        
        for i in config_file:
            
    
            logging.info("i Value {}".format(i))
          
            if merchant == i['merchant_name']:
                count+=1
                logging.info("count {}".format(count))
                bucket_name = Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['bucket']
            
                client2 = storage.Client()
                bucket = client2.get_bucket(bucket_name)             
                blob = bucket.blob(i['query_file'])        
                text = blob.download_as_string()
                stg_query = text.decode('utf-8')
    
                  
                logging.info(" stg_query : {}".format(stg_query)) 

                logging.info("Value of i['date_time_merchant'] {} ".format(i['date_time_merchant']))
                
                logging.info(" len(extract_start_datetime) : {} len(extract_end_datetime) : {} len(file_date):{} ".format(len(extract_start_datetime),len(extract_end_datetime),len(file_date))) 
                
                if len(extract_start_datetime)>19 or len(extract_end_datetime)>19 or len(file_date)>19\
                or extract_end_datetime == None or extract_start_datetime == None \
                or extract_end_datetime == 'None'or extract_start_datetime == 'None'\
                or file_date=='None' or file_date is None:
                    errorcount = errorcount + 1
                    event_message = f"Extraction failed for {merchant}"
                    event = Event(
                        dag_name=dag_name,
                        event_status="failure",
                        event_message=event_message,
                        start_time=exec_time_aest,

                    )
                    publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))                 
                    result = Sendemail(email_to,"Extraction failed for " +merchant,"Please check the datetime format in the control table, Correct format is YYYY-mm-ddTHH:MM:SS \n\nJob name pdh_stlm_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                    break
                    
                

                if i['date_time_merchant']=='Y' and (file_date!='None' or file_date is not None):
                    loadDate = file_date.replace("-","").replace(":","").replace(" ","")
                elif i['date_time_merchant']=='N' and (file_date!='None' or file_date is not None):
                    loadDate,temptime=file_date.split(" ",2)
                    loadDate = loadDate.replace("-","")
                    logging.info("loadDate for file_date {}".format(loadDate)) 
                
                if i['access_google_sheet'] == 'Y': 
                    projectID,datasetID,target_table=i['gs_table_name'].split('.',3)
                    pu.PDHUtils.upload_gs_to_bq(i['gs_unique_id'],i['gs_sheet_name'],datasetID+"."+target_table,projectID)
                    logging.info("{} refreshed successfully".format(i['gs_table_name']))
                
                projectID,datasetID,target_table=i['source_table'].split('.',3);
                target_table= "stgng"+target_table+loadDate
                stag_proj_dataset = Variable.get("stlm_file_gen_bq_to_gcs", deserialize_json=True)['stag_proj_dataset']
                stg_target_table = stag_proj_dataset +"."+target_table
            
                if i['date_filter']=='Y':

                    logging.info("src_date_column {}" .format(i['src_date_column']))
                    stg_query = stg_query.replace('stg',stg_target_table).replace('src',i['source_table']).replace('ctrl',control_table).replace('sr_date_column',i['src_date_column']).replace('mrc',merchant)
        
                else:
        
                    stg_query = stg_query.replace('stg',stg_target_table).replace('src',i['source_table'])         
                    logging.info("query_text - {} ".format(stg_query)) 
        
                result,rows = query_execution(stg_query) 
        
                query_cnt = "select count(*) from " +stg_target_table+';'
                result,rows = query_execution(query_cnt)        
                for row in rows:
                    TotalRecordsExported = row[0]  
                logging.info("Total rows affected: {} ".format(TotalRecordsExported))

                table_id.append(stg_target_table)                
                destination_cloud_storage_uri.append(i['destination_cloud_storage_uri'])
                output_file_name.append(i['output_file_name']+loadDate)
                email.append(i['email'])
                merchant_code.append(i['merchant_name'])
                header.append(i['header'])
                delimiter.append(i['delimiter'])
                extension.append(i['extension'])
                lck_file.append(i['lck_file'])
                trigger_file.append(i['trigger_file'])
                split_etl.append(i['split_etl'])
                split_merchant.append(i['split_merchant'])
                email_attachment.append(i['email_attachment'])
                merchant_min_count.append(i['merchant_min_count'])
                qufile_date.append(file_date)
                batch_number.append(batch_no)                
                
                
                kwargs['ti'].xcom_push(key="email_attachment", value=email_attachment)                
                kwargs['ti'].xcom_push(key="split_merchant", value=split_merchant)                
                kwargs['ti'].xcom_push(key="split_etl", value=split_etl) 
                kwargs['ti'].xcom_push(key="batch_number", value=batch_number) 
                kwargs['ti'].xcom_push(key="trigger_file", value=trigger_file)                
                kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
                kwargs['ti'].xcom_push(key="table_id", value=table_id)                
                kwargs['ti'].xcom_push(key="header", value=header)
                kwargs['ti'].xcom_push(key="delimiter", value=delimiter)
                kwargs['ti'].xcom_push(key="extension", value=extension)
                kwargs['ti'].xcom_push(key="lck_file", value=lck_file)                
                kwargs['ti'].xcom_push(key="trigger_file", value=trigger_file)                  
                kwargs['ti'].xcom_push(key="destination_cloud_storage_uri", value=destination_cloud_storage_uri)
                kwargs['ti'].xcom_push(key="output_file_name", value= output_file_name)
                kwargs['ti'].xcom_push(key="email", value=email)
                kwargs['ti'].xcom_push(key="control_table", value=control_table)
                kwargs['ti'].xcom_push(key="merchant", value=merchant_code)
                kwargs['ti'].xcom_push(key="execTimeInAest", value=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                kwargs['ti'].xcom_push(key="merchant_min_count", value=merchant_min_count)
              
                break    

    logging.info("Task getConfigDetails :=> errorcount : {}".format(errorcount))
    if count >= 1 and count == totcount:
        kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
        kwargs['ti'].xcom_push(key="email_to", value=email_to)        
        return True
    elif totcount == 0 or count == 0:  
        result = Sendemail(email_to,"Extraction failed, No entry in the control table for file generation process","No entry in the control table for file generation process.\n\nJob name - pdh_stlm_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        event_message = "Extraction failed,No entry in the control table for file generation process.No entry in the control table for file generation process."
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,

        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return False
    elif count >= 1 and count != totcount and TotalRecordsExported > 2:  
        result = Sendemail(email_to,"Extraction failed for some merchant codes ","Please check the control table and log.\n\nJob name - pdh_stlm_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        errorcount = errorcount + 1
        event_message = "Extraction failed for some merchant codes"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,

        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        
    kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
    kwargs['ti'].xcom_push(key="email_to", value=email_to)    
    return True   
      
    

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
    
def query_execution(current_query):

    try:
        client = bigquery.Client()
        query_job = client.query(        
        current_query,location='US')
        rows = " "
        rows = query_job.result()  # Waits for the query to finish

        return True,rows
    except Exception as e:
        logging.info("Exception:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        email_to = Variable.get("stlm_file_gen_bq_to_gcs", deserialize_json=True)['email_to']
        result = Sendemail(email_to,"Job Failure, exception raised in file generation process ","Exception:\n"+str(e)+"\n\nJob name - pdh_stlm_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        event_message = "Failed on exception raised during query execution"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        raise AirflowException("Query failure")
        return False,rows    


        
   
def extractToGCS(**kwargs):
    batch_no = kwargs.get('templates_dict').get('batch_number')
    qufile_dt = kwargs.get('templates_dict').get('qufile_date')
    email_id  = kwargs.get('templates_dict').get('email') 
    trigger_file=kwargs.get('templates_dict').get('trigger_file')
    table_id = kwargs.get('templates_dict').get('table_id')    
    header = kwargs.get('templates_dict').get('header')
    delimiter = kwargs.get('templates_dict').get('delimiter')
    extension = kwargs.get('templates_dict').get('extension')
    lck_file = kwargs.get('templates_dict').get('lck_file')
    outputGCSlocation = kwargs.get('templates_dict').get('destination_cloud_storage_uri')

    loadTime = kwargs.get('templates_dict').get('loadDate')
    output_file_name = kwargs.get('templates_dict').get('output_file_name')
    errorcount = int(kwargs.get('templates_dict').get('errorcount'))
    email_to = kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant_code = kwargs.get('templates_dict').get('merchant')
    merchant_min_count = kwargs.get('templates_dict').get('merchant_min_count') 
    
    count = 0
    merchant =[]
    email=[]
    qufile_date=[]
    batch_number=[]
    complete_file_name=[]
    file_path=[]
    
    batch_no=ast.literal_eval(batch_no)
    qufile_dt=ast.literal_eval(qufile_dt)
    email_id=ast.literal_eval(email_id)
    trigger_file = ast.literal_eval(trigger_file)
    table_id = ast.literal_eval(table_id)    
    header = ast.literal_eval(header)
    delimiter = ast.literal_eval(delimiter) 
    extension = ast.literal_eval(extension)
    lck_file = ast.literal_eval(lck_file)    
    output_file_name = ast.literal_eval(output_file_name)
    outputGCSlocation = ast.literal_eval(outputGCSlocation)
    merchant_code = ast.literal_eval(merchant_code)
    merchant_min_count = ast.literal_eval(merchant_min_count)  

    
    logging.info("table_id {}".format(table_id))    
    logging.info("output_file_name {}".format(output_file_name))
    logging.info("outputGCSlocation {}".format(outputGCSlocation))
  
    
    job_config = bigquery.ExtractJobConfig()
    client = bigquery.Client()
    client1=storage.Client()

    for index in range(0,len(table_id)):
        try:
            file_name=''
            count+=1
            logging.info("index{}".format(index))
            logging.info("len(table_id){}".format(len(table_id)))
            logging.info("table_id[index] {}".format(table_id[index]))
            logging.info("table_id {}".format(table_id))            
            absoluteDestinationUri = outputGCSlocation[index]+ "/" + output_file_name[index] + "." + str(extension[index])
            logging.info("absoluteDestinationUri {}".format(absoluteDestinationUri))   
            file_name=output_file_name[index] + "." + extension[index]
            logging.info("file_name {}".format(file_name))           
            complete_file_name.append(file_name)            
            logging.info("complete_file_name {}".format(complete_file_name))
            logging.info("count {}".format(count))
            logging.info("header {}".format(header[index]))
            logging.info("lck_file {}" .format(lck_file[index]))
            if len(merchant_min_count[index].strip()) > 0:
                #value is set in airflow variable.
                logging.info("merchant_min_count {}" .format(merchant_min_count[index]))
            else:
                #default value when variable is not set.
                merchant_min_count[index] = "2"
                logging.info("Default value - merchant_min_count {}" .format(merchant_min_count[index]))
            
            job_config.destination_format = str(extension[index])
            job_config.field_delimiter = str(delimiter[index])

            if header[index] =='N':
                job_config.print_header=False
            else:
                job_config.print_header=True            
    
        
            tableRef = table_id[index]            
            #only generate file when source table has data.            
            result,rowc =query_execution('select * from '+tableRef+';')
            logging.info("Source table: {}".format(tableRef))           
            total_count = rowc.total_rows            
            logging.info("total count: {}".format(total_count))            
            
            if total_count > int(merchant_min_count[index]):
                extract_job = client.extract_table(
                    tableRef,
                    absoluteDestinationUri,
                    location='US',
                    job_config=job_config)  # API request
                extract_job.result()  # Waits for job to complete.
                logging.info("Exported {} to {}".format(table_id[index], absoluteDestinationUri))            

                logging.info("outputGCSlocation[index] {}".format(outputGCSlocation[index])) 
                bucket_name=outputGCSlocation[index].split('/')[2]
                logging.info("bucket_name {}".format(bucket_name))            
                folder_name=outputGCSlocation[index].split('/')[3]
                logging.info("folder_name {} ".format(folder_name))                

                bucket= client1.bucket(bucket_name) 
            
                logging.info("trigger file {}" .format(trigger_file[index]))
            
                if trigger_file[index] is not None:  
                    blob=bucket.blob(folder_name+"/"+trigger_file[index])
                    blob.upload_from_string(" ",content_type = 'text/plain')
                    logging.info("trigger file {} generated" .format(trigger_file[index]))
                
                logging.info("lck_file {}" .format(lck_file[index]))
            
                if lck_file[index]=='Y':
                    time.sleep(10)       #generating lck file for sftp  

                
                    blob=bucket.blob(folder_name+"/"+output_file_name[index]+".lck")
                    blob.upload_from_string(" ",content_type = 'text/plain')

                result,rows = query_execution('drop table '+table_id[index]+';') #dropping table
                logging.info("Dropped table {}" .format(table_id[index]))
                merchant.append(merchant_code[index])
                merchant_min_count.append(merchant_min_count[index])          
                email.append(email_id[index])
                qufile_date.append(qufile_dt[index])
                batch_number.append(batch_no[index])
                file_path.append(outputGCSlocation[index])
                outbound_file = folder_name+'/'+file_name
                event_fname = outputGCSlocation[index]+file_name
                size_in_bytes = bucket.get_blob(outbound_file).size
                destination_bucket_name = bucket_name                
                logging.info("file_size: {}".format(size_in_bytes))
                logging.info("row_count: {}".format(total_count))                   
                event_message = "File generated successfully in task extractToGCS"              
                event = Event(
                    dag_name=dag_name,
                    event_status="success",
                    event_message=event_message,
                    start_time=exec_time_aest,
                    file_name=event_fname,
                    target_name=destination_bucket_name,
                    file_size=int(size_in_bytes),
                    row_count=int(total_count)                    
                    )
                publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            else:
                print(f'Emtpy file will not be generated for =:> {merchant_code[index]}')                  
                event_message = f'Emtpy file will not be generated for =:> {merchant_code[index]}'                            
                event = Event(
                    dag_name=dag_name,
                    event_status="success",
                    event_message=event_message,                    
                    start_time=exec_time_aest                                       
                    )
                publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        except Exception as e:
            logging.info("Exception:{}".format(e))
            errorcount = errorcount + 1
            logging.info("errorcount:{}".format(errorcount))
            result = Sendemail(email_to,"Extraction failed in task extractToGCS ","Exception raised in task extractToGCS \n\nJob name - stlm_file_gen_bq_to_gcs failed.",execTimeInAest)            
            event_message = f"Extraction failed in task extractToGCS for =:> {merchant_code[index]}"
            event = Event(
                dag_name=dag_name,
                event_status="failure",
                event_message=event_message,
                start_time=exec_time_aest                
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))

    kwargs['ti'].xcom_push(key="email", value=email) 
    kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
    kwargs['ti'].xcom_push(key="batch_number", value=batch_number)    
    kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
    kwargs['ti'].xcom_push(key="email_to", value=email_to)
    kwargs['ti'].xcom_push(key="merchant", value=merchant)
    kwargs['ti'].xcom_push(key="complete_file_name", value=complete_file_name) 
    kwargs['ti'].xcom_push(key="file_path", value=file_path)
    kwargs['ti'].xcom_push(key="merchant_min_count", value=merchant_min_count)
    logging.info("Task extractToGCS :=> errorcount : {}".format(errorcount))
    return True    
    
                  
    
def updatecontrolTable(**kwargs):    
    batch_number = kwargs.get('templates_dict').get('batch_number')
    qufile_date = kwargs.get('templates_dict').get('qufile_date')
    control_table = kwargs.get('templates_dict').get('control_table')
    split_etl= kwargs.get('templates_dict').get('split_etl')
    split_merchant=kwargs.get('templates_dict').get('split_merchant')
    complete_file_name=kwargs.get('templates_dict').get('complete_file_name')
    file_path=kwargs.get('templates_dict').get('file_path')
    qu_ctrltab=Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['qu_ctrltab']
    qu_ctrltab_false=Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['qu_ctrltab_false']
    email  = kwargs.get('templates_dict').get('email') 
    error_email=kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant = kwargs.get('templates_dict').get('merchant')
    errorcount=int(kwargs.get('templates_dict').get('errorcount'))
    email_attachment=kwargs.get('templates_dict').get('email_attachment')
    qufile_date = ast.literal_eval(qufile_date)
    merchant = ast.literal_eval(merchant)
    email = ast.literal_eval(email)
    batch_number = ast.literal_eval(batch_number)
    split_etl=ast.literal_eval(split_etl)
    split_merchant=ast.literal_eval(split_merchant)
    email_attachment=ast.literal_eval(email_attachment)
    complete_file_name=ast.literal_eval(complete_file_name)
    file_path=ast.literal_eval(file_path)
    
    environment= Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['environment']
    email_destination_bucket_name = gcs_bucket
    logging.info("email_destination_bucket_name {}".format(email_destination_bucket_name))   

    for index in range(0,len(merchant)):
        logging.info("len(merchant){}".format(len(merchant)))
        logging.info("merchant {}".format(merchant[index]))
        logging.info("email_attachment {}".format(email_attachment[index]))
        qu_ctrl=qu_ctrltab.replace('ctrl',control_table).replace('btc',batch_number[index]).replace('mrc',merchant[index]).replace('fldt',qufile_date[index])
        logging.info("qu_ctrl {}".format(qu_ctrl))
        result,rows =query_execution(qu_ctrl)
        
        if split_etl[index]=='Y':
            logging.info("index {}".format(index)) 
            logging.info("split_etl[index] {}".format(split_etl[index]))            
            qu_ctrl_false=qu_ctrltab_false.replace('ctrl',control_table).replace('mrc',split_merchant[index])
            logging.info("qu_ctrl_false {}".format(qu_ctrl_false))
            result,rows =query_execution(qu_ctrl_false)

        if email_attachment[index] =='Y':  
           logging.info("index {}".format(index))
           logging.info("email[index] {}".format(email[index])) 
           logging.info("email_attachment[index] {}".format(email_attachment[index]))
           logging.info("complete_file_name[index] {}".format(complete_file_name[index]))  
           bucket_name = file_path[index].split('/')[2]
           logging.info("bucket_name {}".format(bucket_name))             
           # email_destination_bucket_name =Variable.get('stlm_file_gen_bq_to_gcs',deserialize_json=True)['base_bucket']
           send_email_attach = su.EmailAttachments('', email[index], "["+environment+"] File generation for "+merchant[index]+" "+execTimeInAest,"File generation for "+merchant[index]+" completed successfully. Please check the attachment",complete_file_name[index],bucket_name,email_destination_bucket_name)           
		   
           logging.info("email {}".format(email))
           logging.info("file_path {}".format(file_path[index]))			
           client2 = storage.Client()
           
           blob_name = (file_path[index].split('/')[3])+'/'+complete_file_name[index]
           logging.info("blob_name {}".format(blob_name))                     
           destination_blob_name=(file_path[index].split('/')[3])+'/'+merchant[index]+'_processed/'+complete_file_name[index] 
           logging.info("destination_blob_name {}".format(destination_blob_name))  
           
            
           destination_bucket_name = file_path[index].split('/')[2]
           logging.info("destination_bucket_name {}".format(destination_bucket_name))                           
           source_bucket = client2.bucket(bucket_name)
           source_blob = source_bucket.blob(blob_name)
           destination_bucket = client2.bucket(destination_bucket_name)
           
           blob_copy = source_bucket.copy_blob(
               source_blob, destination_bucket, destination_blob_name
               )
           source_bucket.delete_blob(blob_name)	
           logging.info("Source file {} deleted successfully".format(complete_file_name[index]))           
           
           # publish event to PubSub
           event_message = f"{environment} Task updatecontrolTable Control table updated for {merchant[index]} {execTimeInAest}"
           event = Event(
               dag_name=dag_name,
               event_status="success",
               event_message=event_message,
               start_time=exec_time_aest)
           publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
		
        else:
           logging.info("email {}".format(email[index]))                   
           result = Sendemail(email[index],"["+environment+"]File generation for "+merchant[index]+" "+execTimeInAest,"File generation for "+merchant[index]+" completed successfully.\n\nJob name pdh_stlm_file_gen_bq_to_gcs",execTimeInAest)
           event_message = f"[{environment}] Task updatecontrolTable Control table updated for {merchant[index]}"
           event = Event(
               dag_name=dag_name,
               event_status="success",
               event_message=event_message,
               start_time=exec_time_aest
               )
           publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))           
    logging.info("Task updatecontrolTable :=> errorcount : {}".format(errorcount))       
    if errorcount == 0:
        event_message = ("updatecontrolTable completed successfully.No error count."
        )
        event = Event(
            dag_name=dag_name,
            event_status="success",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        return True
    if errorcount >=1:
        result = Sendemail(error_email,"pdh_stlm_file_gen_bq_to_gcs failed"," Task updatecontrolTable failed, extraction failed for some merchants.\n\nPlease check the control table and log",execTimeInAest)
        event_message = (
            "Task updateControlTable failed, extraction failed for some merchants.\n\n"
            "Please check the control table and log"
        )
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        raise AirflowException("Job failure")
        return False      
    
    
        
getConfigDetails = ShortCircuitOperator(
    python_callable=getConfigDetails,
    task_id='getConfigDetails',
    depends_on_past=False,
    provide_context=True,
    dag=dag)
        


extractToGCS = PythonOperator(
    task_id='extractToGCS',
    dag=dag,
    python_callable=extractToGCS,
    provide_context=True,  
    templates_dict={'batch_number': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='batch_number')}}",
                    'qufile_date': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='qufile_date')}}",  
                    'email': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='email')}}",
                    'trigger_file': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='trigger_file') }}",
                    'table_id': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='table_id') }}",                    
                    'header': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='header') }}",
                    'delimiter': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='delimiter') }}",    
                    'extension': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='extension') }}",   
                    'lck_file': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='lck_file') }}",                       
                    'loadDate': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='loadDate') }}",
                    'destination_cloud_storage_uri': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='destination_cloud_storage_uri')}}",
                    'output_file_name': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='output_file_name') }}",
                    'errorcount': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='errorcount') }}",
                    'email_to': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to') }}",
                    'execTimeInAest': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='execTimeInAest')}}",
                    'merchant': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='merchant')}}",
                    'merchant_min_count': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='merchant_min_count')}}"                    
                   }
)
                 
    
updatecontrolTable = PythonOperator(
    task_id='updatecontrolTable',
    dag=dag,
    python_callable=updatecontrolTable,
    provide_context=True,  
    templates_dict={'batch_number': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='batch_number')}}",
                    'qufile_date': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='qufile_date')}}",
                    'control_table': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='control_table')}}",
                    'email': "{{task_instance.xcom_pull(task_ids='extractToGCS',key='email')}}",
                    'email_to': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to')}}",
                    'execTimeInAest': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='execTimeInAest')}}",
                    'merchant': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='merchant')}}",
                    'errorcount': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='errorcount')}}",
                    'split_etl': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='split_etl')}}",
                    'split_merchant': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='split_merchant')}}",
                    'email_attachment': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_attachment')}}",
                    'complete_file_name': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='complete_file_name')}}",
                    'file_path': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='file_path')}}"                    
                   })
                   


getConfigDetails >> extractToGCS >> updatecontrolTable