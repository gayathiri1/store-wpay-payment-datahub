import os
from airflow import DAG,models,AirflowException
from datetime import datetime, date, time, timedelta, tzinfo
import logging
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
from google.cloud import bigquery
# from airflow.models import Variable
from datetime import datetime, timedelta
import datetime as dttime
import pytz
import ast
import numpy as np
from zlibpdh import pdh_utilities as pu
from zlibpdh import sub_utilities as su
import time
import pendulum

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
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(9000),
    'retries': 0,
    'max_active_runs': 1,
    'start_date': start_date
}

logging.info("constructing dag - using airflow as owner")

dag = DAG('pdh_generic_file_gen_bq_to_gcs', catchup=False, default_args=default_args,schedule_interval= None)

gcs_bucket = os.environ.get("GCS_BUCKET")

def getConfigDetails(**kwargs):

    bq_data = kwargs['ti'].xcom_pull(task_ids='getMerchantData')
    count=0
    get_config_opt=[]
    table_id = []
    final_file_name=[]
    batch_no=" "
    batch_number=[]
    merchant_code =[]
    qufile_date=[]
    dest_cloud_storage_uri = []
    email_final = []
    header_final = []
    delimiter_final = []
    extension_final = []
    lck_file_final = []
    trigger_file_final = []
    split_etl_final = []
    split_merchant_final = []
    email_attachment_final = []
    base_bucket_final = []
    qu_ctrltab_final = []
    qu_ctrltab_false_final = []
    environment_final =[]
    
    for i in range(len(bq_data)):
        
        dag_var = np.array(bq_data[i])
        generic_data= dag_var[:10]
        merchant_data= dag_var[10:]
        
        #0 to 9
        bucket,control_table,qs_ctrltab,qu_ctrltab,qu_ctrltab_false,qc_ctrltab,email_to,stag_proj_dataset,base_bucket,environment = generic_data
        
        #10
        date_filter,date_time_merchant,destination_cloud_storage_uri,email,access_google_sheet,gs_sheet_name,gs_table_name,gs_unique_id,header,merchant_name,output_file_name,query_file,source_table,src_date_column,trigger_file,delimiter,extension,lck_file,split_etl,split_merchant,email_attachment = merchant_data   
        
        merchant =" "
        file_date =" "
        rownum=0
        totcount=0
        destination_cloud_storage_uri = dag_var[12:13]
        output_file_name = dag_var[20:21]
        email=dag_var[13:14]
        header=dag_var[18:19]
        delimiter=dag_var[25:26]
        extension=dag_var[26:27]
        lck_file=dag_var[27:28]
        trigger_file=dag_var[24:25]
        src_date_column=dag_var[23:24]
        split_etl=dag_var[28:29]
        split_merchant=dag_var[29:30]
        email_attachment=dag_var[30:]
        errorcount = 0
        base_bucket = dag_var[8:9]
        qu_ctrltab = dag_var[3:4]
        qu_ctrltab_false = dag_var[4:5]
        environment = dag_var[9:10]

        # https://woolworthsdigital.atlassian.net/browse/DATPAY-3739
        # Assign base_bucket from env var
        base_bucket = gcs_bucket
            
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        
        result,rows = query_execution(qs_ctrltab,email_to)
        logging.info("select control table query executed successfully")
        result,rowc = query_execution(qc_ctrltab,email_to)
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
            
            logging.info("merchant:{}".format(merchant)) #file_gen_details
            logging.info("merchant_name:{}".format(merchant_name)) #Variables

            if merchant == merchant_name:
                count+=1
                logging.info("count {}".format(count))
                bucket_name = bucket
                client2 = storage.Client()
                bucket_storage = client2.get_bucket(bucket_name)              
                blob = bucket_storage.blob(query_file)
                text = blob.download_as_string()
                stg_query = text.decode('utf-8')
            
                logging.info("Value of 'date_time_merchant' {} ".format(date_time_merchant))
                logging.info("Destination_cloud_storage_uri {} ".format(destination_cloud_storage_uri))
                logging.info(" len(extract_start_datetime) : {} len(extract_end_datetime) : {} len(file_date):{} ".format(len(extract_start_datetime),len(extract_end_datetime),len(file_date))) 
                
                if len(extract_start_datetime)>19 or len(extract_end_datetime)>19 or len(file_date)>19 or extract_end_datetime == None or extract_start_datetime == None or extract_end_datetime == 'None' or extract_start_datetime == 'None'or file_date=='None' or file_date is None:
                    errorcount = errorcount + 1
                    result = Sendemail(email_to,"Extraction failed for " +merchant,"Please check the datetime format in the control table, Correct format is YYYY-mm-ddTHH:MM:SS \n\nJob name pdh_generic_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                    break
                
                logging.info("date_time_merchant {}".format(date_time_merchant))  
                logging.info("src_date_column {}".format(src_date_column))
                
                if (extract_end_datetime!='None' or extract_end_datetime is not None):
                    tranDate,trantime=extract_end_datetime.split(" ",2)
                    #tranDate = tranDate.replace("-","")
                    logging.info("tran_date {}".format(tranDate))
                
                if date_time_merchant=='Y' and (file_date!='None' or file_date is not None):
                    loadDate = file_date.replace("-","").replace(":","").replace(" ","")
                    logging.info("loadDate for file_date {}".format(loadDate))
                elif (date_time_merchant=='N' and (file_date!='None' or file_date is not None)) and (src_date_column=='Y' and (extract_end_datetime!='None' or extract_end_datetime is not None)):
                    loadDate,temptime=extract_end_datetime.split(" ",2)
                    loadDate = loadDate.replace("-","")
                    logging.info("loadDate for file_date {}".format(loadDate))
                else:
                    loadDate,temptime=file_date.split(" ",2)
                    loadDate = loadDate.replace("-","")
                    logging.info("loadDate for file_date {}".format(loadDate))
                
                if access_google_sheet == 'Y': 
                    projectID,datasetID,target_table=gs_table_name.split('.',3)
                    pu.PDHUtils.upload_gs_to_bq(gs_unique_id,gs_sheet_name,datasetID+"."+target_table,projectID)
                    logging.info("{} refreshed successfully".format(gs_table_name))
                
                projectID,datasetID,target_table=source_table.split('.',3);
                target_table= "stgng"+target_table+loadDate
                stg_target_table = stag_proj_dataset +"."+target_table
                logging.info("stg_target_table: {} ".format(stg_target_table))
                
                if date_filter=='Y':
                    logging.info("src_date_column {}" .format(src_date_column))
                    stg_query = stg_query.replace('stg',stg_target_table).replace('src',source_table).replace('ctrl',control_table).replace('src_date_column',src_date_column).replace('mrc',merchant)
                else:
                    stg_query = stg_query.replace('stg',stg_target_table).replace('src',source_table).replace('fldt',tranDate)
                    logging.info("query_text - {} ".format(stg_query)) 
            
                result,rows = query_execution(stg_query,email_to) 
            
                query_cnt = "select count(*) from " +stg_target_table+';'
                result,rows = query_execution(query_cnt,email_to)  
                
                for row in rows:
                    TotalRecordsExported = row[0]  
                logging.info("Total rows affected: {} ".format(TotalRecordsExported))
            
                output_file_name_str = str(output_file_name)
                file_name_wdate = "".join([output_file_name_str,loadDate]).replace("]","").replace("[","").replace("'","")
                
                logging.info("output_file_name: {} ".format(file_name_wdate))
                logging.info("loadDate: {} ".format(loadDate))
            
                table_id.append(stg_target_table) #Y
                dest_cloud_storage_uri = np.append(dest_cloud_storage_uri,destination_cloud_storage_uri).tolist()
                #destination_cloud_storage_uri = np.append(destination_cloud_storage_uri,destination_cloud_storage_uri).tolist()
                #logging.info("Destination_cloud_storage_uri {} ".format(dest_cloud_storage_uri))
                final_file_name = np.append(final_file_name,file_name_wdate).tolist() #Y
                email_final = np.append(email_final,email).tolist()
                merchant_code.append(merchant_name) #Y
                header_final = np.append(header_final,header).tolist()
                delimiter_final = np.append(delimiter_final,delimiter).tolist()
                extension_final = np.append(extension_final,extension).tolist()
                lck_file_final = np.append(lck_file_final,lck_file).tolist()
                trigger_file_final = np.append(trigger_file_final,trigger_file).tolist()
                split_etl_final = np.append(split_etl_final,split_etl).tolist()
                split_merchant_final = np.append(split_merchant_final,split_merchant).tolist()
                email_attachment_final = np.append(email_attachment_final,email_attachment).tolist()
                qufile_date.append(file_date) #Y
                batch_number.append(batch_no) #Y
                base_bucket_final = np.append(base_bucket_final,base_bucket).tolist()
                qu_ctrltab_final = np.append(qu_ctrltab_final,qu_ctrltab).tolist()
                qu_ctrltab_false_final = np.append(qu_ctrltab_false_final,qu_ctrltab_false).tolist()
                environment_final = np.append(environment_final,environment).tolist()
                
                
                kwargs['ti'].xcom_push(key="email_attachment", value=email_attachment_final)                
                kwargs['ti'].xcom_push(key="split_merchant", value=split_merchant_final)                
                kwargs['ti'].xcom_push(key="split_etl", value=split_etl_final) 
                kwargs['ti'].xcom_push(key="batch_number", value=batch_number) 
                kwargs['ti'].xcom_push(key="trigger_file", value=trigger_file_final)                
                kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
                kwargs['ti'].xcom_push(key="table_id", value=table_id)
                kwargs['ti'].xcom_push(key="header", value=header_final)
                kwargs['ti'].xcom_push(key="delimiter", value=delimiter_final)
                kwargs['ti'].xcom_push(key="extension", value=extension_final)
                kwargs['ti'].xcom_push(key="lck_file", value=lck_file_final)                              
                kwargs['ti'].xcom_push(key="destination_cloud_storage_uri", value=dest_cloud_storage_uri)
                kwargs['ti'].xcom_push(key="output_file_name", value= final_file_name)
                kwargs['ti'].xcom_push(key="email", value=email_final)
                kwargs['ti'].xcom_push(key="control_table", value=control_table)
                kwargs['ti'].xcom_push(key="merchant", value=merchant_code)
                kwargs['ti'].xcom_push(key="base_bucket", value=base_bucket_final)
                kwargs['ti'].xcom_push(key="qu_ctrltab", value=qu_ctrltab_final)
                kwargs['ti'].xcom_push(key="qu_ctrltab_false", value=qu_ctrltab_false_final)
                kwargs['ti'].xcom_push(key="environment", value=environment_final)
                kwargs['ti'].xcom_push(key="execTimeInAest", value=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                
    #break
            
        
    if count >= 1 and count == totcount :
        kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
        kwargs['ti'].xcom_push(key="email_to", value=email_to)
        return True
    elif totcount == 0 or count == 0 :  
        result = Sendemail(email_to,"Extraction failed, No entry in the control table for file generation process","No entry in the control table for file generation process.\n\nJob name - pdh_generic_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        return False
    elif count >= 1 and count != totcount :  
        result = Sendemail(email_to,"Extraction failed for some merchant codes ","Please check the control table and log.\n\nJob name - pdh_generic_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        errorcount = errorcount + 1
    
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
    
def query_execution(current_query,error_email):

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
        result = Sendemail(error_email,"Job Failure, exception raised in file generation process ","Exception:\n"+str(e)+"\n\nJob name - pdh_generic_file_gen_bq_to_gcs",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
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
    environment=kwargs.get('templates_dict').get('environment')

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
    environment = ast.literal_eval(environment)
  
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
            
            job_config.destination_format = str(extension[index])
            job_config.field_delimiter = str(delimiter[index])

            if header[index] =='N':
                job_config.print_header=False
            else: 
                job_config.print_header=True

            tableRef = table_id[index]
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
        
            result,rows = query_execution('drop table '+table_id[index]+';',email_to) #dropping table
            logging.info("Dropped table {}" .format(table_id[index]))
            merchant.append(merchant_code[index])
            email.append(email_id[index])
            qufile_date.append(qufile_dt[index])
            batch_number.append(batch_no[index])
            file_path.append(outputGCSlocation[index])
        except Exception as e:
            logging.info("Exception:{}".format(e))
            errorcount = errorcount + 1
            logging.info("errorcount:{}".format(errorcount))
            result = Sendemail(email_to[index],"Extraction failed in task extractToGCS ","Exception raised in task extractToGCS \n\nJob name - pfd_file_gen_bq_to_gcs failed.",execTimeInAest)

    kwargs['ti'].xcom_push(key="email", value=email) 
    kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
    kwargs['ti'].xcom_push(key="batch_number", value=batch_number)    
    kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
    kwargs['ti'].xcom_push(key="email_to", value=email_to)
    kwargs['ti'].xcom_push(key="merchant", value=merchant)
    kwargs['ti'].xcom_push(key="complete_file_name", value=complete_file_name) 
    kwargs['ti'].xcom_push(key="file_path", value=file_path)
    return True    
    

def updatecontrolTable(**kwargs):
    batch_number = kwargs.get('templates_dict').get('batch_number')
    qufile_date = kwargs.get('templates_dict').get('qufile_date')
    control_table = kwargs.get('templates_dict').get('control_table')
    split_etl= kwargs.get('templates_dict').get('split_etl')
    split_merchant=kwargs.get('templates_dict').get('split_merchant')
    complete_file_name=kwargs.get('templates_dict').get('complete_file_name')
    file_path=kwargs.get('templates_dict').get('file_path')
    qu_ctrltab=kwargs.get('templates_dict').get('qu_ctrltab')
    qu_ctrltab_false=kwargs.get('templates_dict').get('qu_ctrltab_false')
    email=kwargs.get('templates_dict').get('email') 
    error_email=kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant = kwargs.get('templates_dict').get('merchant')
    errorcount=int(kwargs.get('templates_dict').get('errorcount'))
    email_attachment=kwargs.get('templates_dict').get('email_attachment')
    environment=kwargs.get('templates_dict').get('environment')
    base_bucket=kwargs.get('templates_dict').get('base_bucket')
    
    qufile_date = ast.literal_eval(qufile_date)
    merchant = ast.literal_eval(merchant)
    email = ast.literal_eval(email)
    batch_number = ast.literal_eval(batch_number)
    split_etl=ast.literal_eval(split_etl)
    split_merchant=ast.literal_eval(split_merchant)
    email_attachment=ast.literal_eval(email_attachment)
    complete_file_name=ast.literal_eval(complete_file_name)
    file_path=ast.literal_eval(file_path)
    qu_ctrltab=ast.literal_eval(qu_ctrltab)
    qu_ctrltab_false=ast.literal_eval(qu_ctrltab_false)
    environment=ast.literal_eval(environment)
    base_bucket=ast.literal_eval(base_bucket)
    

    for index in range(0,len(merchant)):
        
        environment_str = str(environment[index])
        merchant_str = str(merchant[index])
        complete_file_name_str = str(complete_file_name[index])
        
        logging.info("len(merchant){}".format(len(merchant)))
        logging.info("merchant {}".format(merchant[index]))
        qu_ctrltab_str = str(qu_ctrltab[index])
        qu_ctrl=qu_ctrltab_str.replace('ctrl',control_table).replace("]","").replace("[","").replace('btc',batch_number[index]).replace('mrc',merchant[index]).replace('fldt',qufile_date[index])
        logging.info("qu_ctrl {}".format(qu_ctrl))
        result,rows =query_execution(qu_ctrl,error_email)
 
        if split_etl[index]=='Y':
            logging.info("index {}".format(index)) 
            logging.info("split_etl[index] {}".format(split_etl[index]))
            qu_ctrltab_false_str = str(qu_ctrltab_false[index])
            qu_ctrl_false=qu_ctrltab_false_str.replace('ctrl',control_table).replace('mrc',split_merchant[index]).replace("]","").replace("[","")
            logging.info("qu_ctrl_false {}".format(qu_ctrl_false))
            result,rows =query_execution(qu_ctrl_false,error_email)
         
        if email_attachment[index] == "Y":  
           logging.info("index {}".format(index))
           logging.info("email_attachment[index] {}".format(email_attachment[index]))  
           logging.info("complete_file_name[index] {}".format(complete_file_name[index]))  
           bucket_name = file_path[index].split('/')[2]
           logging.info("bucket_name {}".format(bucket_name))             
           email_destination_bucket_name =base_bucket[index]
           logging.info("email_destination_bucket_name {}".format(email_destination_bucket_name))   
           send_email_attach = su.EmailAttachments('', email[index], "["+environment_str+"] File generation for "+merchant_str+" "+execTimeInAest,"File generation for "+merchant_str+" completed successfully. Please check the attachment",complete_file_name_str,bucket_name,email_destination_bucket_name)           
		   
           logging.info("email {}".format(email[index]))
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
		
        else:
           logging.info("email {}".format(email[index]))    
                       
           result = Sendemail(email[index],"["+environment_str+"]File generation for "+merchant_str+" "+execTimeInAest,"File generation for "+merchant_str+" completed successfully.\n\nJob name pdh_generic_file_gen_bq_to_gcs",execTimeInAest)


       
    if errorcount == 0:
        return True
    if errorcount >=1:
        result = Sendemail(error_email,"pdh_generic_file_gen_bq_to_gcs failed"," Task updatecontrolTable failed, extraction failed for some merchants.\n\nPlease check the control table and log",execTimeInAest)
        raise AirflowException("Job failure")
        return False
        
    
getMerchantData = BigQueryGetDataOperator(
    task_id='getMerchantData',
    dataset_id= 'pdh_analytics_ds',
    table_id= 'file_gen_bq_to_gcs',
    dag=dag,
    max_results='500'
)
 
        
getConfigDetails = ShortCircuitOperator(
    python_callable=getConfigDetails,
    task_id='getConfigDetails',
    depends_on_past=False,
    provide_context=True,
    dag=dag,
    templates_dict={'return_value': "{{ task_instance.xcom_pull(task_ids='getMerchantData', key='return_value')}}" }
    )
        


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
                    'environment': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='environment')}}"
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
                    'file_path': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='file_path')}}",
                    'base_bucket': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='base_bucket')}}",
                    'qu_ctrltab': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='qu_ctrltab')}}",
                    'qu_ctrltab_false': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='qu_ctrltab_false')}}",
                    'environment': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='environment')}}"
                   }
)
                   

getMerchantData >> getConfigDetails >> extractToGCS >> updatecontrolTable