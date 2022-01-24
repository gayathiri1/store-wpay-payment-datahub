from airflow import DAG,models,AirflowException
from datetime import datetime, date, time, timedelta, tzinfo
import logging
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
#from airflow.contrib.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
import datetime as dttime
import pytz
import ast
from zlibpdh import pdh_utilities as pu
import time




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime(2099, 12, 1),
    'start_date': datetime(2021,7, 12),    # added  for python upgrade to 3
    #'email': emailAlert,
    #'email_on_failure': True,
    #'email_on_retry': True,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    #'schedule_interval': "50 12 * * *",
    # 'sla': timedelta(hours=1)
}

logging.info("constructing dag - using airflow as owner")


dag = DAG('pdh_file_gen_bq_to_gcs', catchup=False, default_args=default_args,schedule_interval= None)
#template_searchpath=path)


def getConfigDetails(**kwargs):

    control_table= Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['control_table']
    qs_ctrltab=Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['qs_ctrltab']
    qc_ctrltab=Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['qc_ctrltab']
    config_file = Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['parameter']
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
    #failed_code=[]
    header=[]
    qufile_date=[]
    trigger_file=[]
    batch_number=[]
    email_to = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)['email_to']
    errorcount = 0
    

    
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
                bucket_name = Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['bucket']
            
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(bucket_name)             
                blob = bucket.blob(i['query_file'])        
                text = blob.download_as_string()
                stg_query = text.decode('utf-8')
    
                  
                logging.info(" stg_query : {}".format(stg_query)) 

                logging.info("Value of i['date_time_merchant'] {} ".format(i['date_time_merchant']))
                
                logging.info(" len(extract_start_datetime) : {} len(extract_end_datetime) : {} len(file_date):{} ".format(len(extract_start_datetime),len(extract_end_datetime),len(file_date))) 
                
                if len(extract_start_datetime)>19 or len(extract_end_datetime)>19 or len(file_date)>19 or extract_end_datetime == None or extract_start_datetime == None or extract_end_datetime == 'None' or extract_start_datetime == 'None'or file_date=='None' or file_date is None:
                    errorcount = errorcount + 1
                    result = Sendemail(email_to,"Extraction failed for " +merchant,"Please check the datetime format in the control table, Correct format is YYYY-mm-ddTHH:MM:SS \n\nJob name pdh_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                    break
                    
                
                #if i['date_time_merchant']=='Y' and (file_date=='None' or file_date is None):
                   # loadDate = execTimeInAest.strftime("%Y%m%d%H%M%S")
                   # logging.info("load for Date date_time_merchant- Y {} ".format(loadDate))
               # elif i['date_time_merchant']=='N' and (file_date is None or file_date=='None'):
                    #loadDate = execTimeInAest.strftime("%Y%m%d")
                    #logging.info("file_date is null so the loadDate is  {} ".format(loadDate))
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
                stag_proj_dataset = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)['stag_proj_dataset']
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
                trigger_file.append(i['trigger_file'])
                qufile_date.append(file_date)
                batch_number.append(batch_no)
                kwargs['ti'].xcom_push(key="batch_number", value=batch_number) 
                kwargs['ti'].xcom_push(key="trigger_file", value=trigger_file)                
                kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
                kwargs['ti'].xcom_push(key="table_id", value=table_id)
                kwargs['ti'].xcom_push(key="header", value=header)
                kwargs['ti'].xcom_push(key="destination_cloud_storage_uri", value=destination_cloud_storage_uri)
                kwargs['ti'].xcom_push(key="output_file_name", value= output_file_name)
                kwargs['ti'].xcom_push(key="email", value=email)
                kwargs['ti'].xcom_push(key="control_table", value=control_table)
                kwargs['ti'].xcom_push(key="merchant", value=merchant_code)
                kwargs['ti'].xcom_push(key="execTimeInAest", value=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))

                break

            #return True
    if count >= 1 and count == totcount :
        kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
        kwargs['ti'].xcom_push(key="email_to", value=email_to)
        return True
    elif totcount == 0 or count == 0 :  
        result = Sendemail(email_to,"Extraction failed for fee file generation","Please check the control table and log.\n\nJob name - pdh_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        return False
    elif count >= 1 and count != totcount :  
        result = Sendemail(email_to,"Extraction failed for some merchant codes ","Please check the control table and log.\n\nJob name - pdh_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        errorcount = errorcount + 1
        #logging.info("errorcount: {} ".format(errorcount))
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
        #for row in rows:
            #records = row[1]
        #logging.info("records {}".format(records))
        return True,rows
    except Exception as e:
        logging.info("Exception:{}".format(e))        
        raise AirflowException("Query failure")
        return False,rows    


        
   
def extractToGCS(**kwargs):
    batch_no = kwargs.get('templates_dict').get('batch_number')
    qufile_dt = kwargs.get('templates_dict').get('qufile_date')
    email_id  = kwargs.get('templates_dict').get('email') 
    trigger_file=kwargs.get('templates_dict').get('trigger_file')
    table_id = kwargs.get('templates_dict').get('table_id')
    header = kwargs.get('templates_dict').get('header')
    outputGCSlocation = kwargs.get('templates_dict').get('destination_cloud_storage_uri')
    #project = kwargs.get('templates_dict').get('projectId')
    #datasetId = kwargs.get('templates_dict').get('datasetID')
    loadTime = kwargs.get('templates_dict').get('loadDate')
    output_file_name = kwargs.get('templates_dict').get('output_file_name')
    errorcount = int(kwargs.get('templates_dict').get('errorcount'))
    email_to = kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant_code = kwargs.get('templates_dict').get('merchant')
    count = 0
    merchant =[]
    email=[]
    qufile_date=[]
    batch_number=[]
    
    batch_no=ast.literal_eval(batch_no)
    qufile_dt=ast.literal_eval(qufile_dt)
    email_id=ast.literal_eval(email_id)
    trigger_file = ast.literal_eval(trigger_file)
    table_id = ast.literal_eval(table_id)
    header = ast.literal_eval(header)
    output_file_name = ast.literal_eval(output_file_name)
    outputGCSlocation = ast.literal_eval(outputGCSlocation)
    merchant_code = ast.literal_eval(merchant_code)

    
    logging.info("table_id {}".format(table_id))
    logging.info("output_file_name {}".format(output_file_name))
    logging.info("outputGCSlocation {}".format(outputGCSlocation))
    extension = "csv"
    #delimiter = ","

    #job_config = bigquery.job.ExtractJobConfig(print_header=False)
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = extension
    #job_config.field_delimiter = delimiter
    #job_config.destination_format = "csv"
    #job_config.field_delimiter = ","

    

    #try:     

    client = bigquery.Client()
    client1=storage.Client()

    for index in range(0,len(table_id)):
        try:
            count+=1
            logging.info("index{}".format(index))
            logging.info("len(table_id){}".format(len(table_id)))
            logging.info("table_id[index] {}".format(table_id[index]))
            logging.info("table_id {}".format(table_id))
            absoluteDestinationUri = outputGCSlocation[index]+ "/" + output_file_name[index] + "." + extension
            logging.info("absoluteDestinationUri {}".format(absoluteDestinationUri))
            logging.info("count {}".format(count))
        

            if header[index] =='N':
                job_config.print_header=False
                job_config.field_delimiter = "|"         

            else: 
             #bucket= client1.bucket(bucket_name)
                job_config.print_header=True
                job_config.field_delimiter = ","
             
    
        
            tableRef = table_id[index]
            extract_job = client.extract_table(
            tableRef,
            absoluteDestinationUri,
            location='US',
            job_config=job_config)  # API request
            extract_job.result()  # Waits for job to complete.
            logging.info("Exported {} to {}".format(table_id[index], absoluteDestinationUri))
        
            temp1,temp2,bucket_name,folder_name,temp3=outputGCSlocation[index].split('/',5)
            logging.info("temp1 {} temp2 {} bucket_name {} folder_name {} temp3 {}".format(temp1,temp2,bucket_name,folder_name,temp3))
            bucket= client1.bucket(bucket_name) 
        
            logging.info("trigger file {}" .format(trigger_file[index]))
        
            if trigger_file[index] is not None:  
                logging.info("trigger file {} generated" .format(trigger_file[index]))
                blob=bucket.blob(folder_name+"/"+trigger_file[index])
                blob.upload_from_string(" ",content_type = 'text/plain')
             

        
            time.sleep(10)       #generating lck file for sftp     
            #destination_cloud_storage_uri="gs://pdh_uat_outgoing/live_group/"
            #temp1,temp2,bucket_name,folder_name,temp3=outputGCSlocation[index].split('/',5)
            #logging.info("temp1 {} temp2 {} bucket_name {} folder_name {} temp3 {}".format(temp1,temp2,bucket_name,folder_name,temp3))

            #client1=storage.Client(); 
            #bucket= client1.bucket(bucket_name)
        
            blob=bucket.blob(folder_name+"/"+output_file_name[index]+".lck")
            blob.upload_from_string(" ",content_type = 'text/plain')
        
            result,rows = query_execution('drop table '+table_id[index]+';') #dropping table
            logging.info("Dropped table {}" .format(table_id[index]))
            merchant.append(merchant_code[index])
            email.append(email_id[index])
            qufile_date.append(qufile_dt[index])
            batch_number.append(batch_no[index])
        except Exception as e:
            logging.info("Exception:{}".format(e))
            errorcount = errorcount + 1
            logging.info("errorcount:{}".format(errorcount))
            result = Sendemail(email_to,"Extraction failed in task extractToGCS ","Exception raised in task extractToGCS \n\nJob name - file_gen_bq_to_gcs failed.",execTimeInAest)
        #raise AirflowException("loading to GCS failed")
    kwargs['ti'].xcom_push(key="email", value=email) 
    kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
    kwargs['ti'].xcom_push(key="batch_number", value=batch_number)    
    kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
    kwargs['ti'].xcom_push(key="email_to", value=email_to)
    kwargs['ti'].xcom_push(key="merchant", value=merchant)
    return True    
   # except Exception as e:
        #logging.info("Exception:{}".format(e))
        #errorcount = errorcount + 1
        #logging.info("errorcount:{}".format(errorcount))
        #result = Sendemail(email_to,"Extraction failed in task extractToGCS ","Exception raised in task extractToGCS "+e,execTimeInAest)
        #raise AirflowException("loading to GCS failed")
        #return True
            
                  
    
def updatecontrolTable(**kwargs):
    batch_number = kwargs.get('templates_dict').get('batch_number')
    qufile_date = kwargs.get('templates_dict').get('qufile_date')
    control_table = kwargs.get('templates_dict').get('control_table')
    qu_ctrltab=Variable.get('file_gen_bq_to_gcs',deserialize_json=True)['qu_ctrltab']
    email  = kwargs.get('templates_dict').get('email') 
    error_email=kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant = kwargs.get('templates_dict').get('merchant')
    errorcount=int(kwargs.get('templates_dict').get('errorcount'))
    qufile_date = ast.literal_eval(qufile_date)
    merchant = ast.literal_eval(merchant)
    email = ast.literal_eval(email)
    batch_number = ast.literal_eval(batch_number)
    

    for index in range(0,len(merchant)):
        logging.info("len(merchant){}".format(len(merchant)))
        logging.info("merchant {}".format(merchant[index]))
        qu_ctrl=qu_ctrltab.replace('ctrl',control_table).replace('btc',batch_number[index]).replace('mrc',merchant[index]).replace('fldt',qufile_date[index])
        logging.info("qu_ctrl {}".format(qu_ctrl))
        result,rows =query_execution(qu_ctrl)
        result = Sendemail(email[index],"File generation for "+merchant[index],"File generation for "+merchant[index]+" completed successfully.\n\nJob name pdh_file_gen_bq_to_gcs",execTimeInAest)
    if errorcount == 0:
        return True
    if errorcount >=1:
        result = Sendemail(error_email,"pdh_file_gen_bq_to_gcs failed"," Task updatecontrolTable failed, extraction failed for some merchants.\n\nPlease check the control table and log",execTimeInAest)
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
    provide_context=True,  # must pass this because templates_dict gets passed via context
    templates_dict={'batch_number': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='batch_number')}}",
                    'qufile_date': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='qufile_date')}}",  
                    'email': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='email')}}",
                    'trigger_file': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='trigger_file') }}",
                    'table_id': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='table_id') }}",
                    'header': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='header') }}",  
                    'loadDate': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='loadDate') }}",
                    'destination_cloud_storage_uri': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='destination_cloud_storage_uri')}}",
                    'output_file_name': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='output_file_name') }}",
                    'errorcount': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='errorcount') }}",
                    'email_to': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to') }}",
                    'execTimeInAest': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='execTimeInAest')}}",
                    'merchant': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='merchant')}}"
                   }
)
                 
    
updatecontrolTable = PythonOperator(
    task_id='updatecontrolTable',
    dag=dag,
    python_callable=updatecontrolTable,
    provide_context=True,  # must pass this because templates_dict gets passed via context
    templates_dict={'batch_number': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='batch_number')}}",
                    'qufile_date': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='qufile_date')}}",
                   'control_table': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='control_table')}}",
                    'email': "{{task_instance.xcom_pull(task_ids='extractToGCS',key='email')}}",
                    'email_to': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to') }}",
                    'execTimeInAest': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='execTimeInAest')}}",
                   'merchant': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='merchant')}}",
                   'errorcount': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='errorcount') }}"})
                   


getConfigDetails >> extractToGCS >> updatecontrolTable