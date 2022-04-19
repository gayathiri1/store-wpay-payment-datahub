from airflow import DAG,AirflowException
from datetime import datetime, time
import logging
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
import pytz
import ast
from google.oauth2 import service_account
from zlibpdh import pdh_utilities as pu
import time



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,7, 12)
}

logging.info("constructing dag - using airflow as owner")


dag = DAG('pdh_edpay_file_generation', catchup=False, default_args=default_args,schedule_interval= None)



def getConfigDetails(**kwargs):

    control_table= Variable.get('edpay_file_generation',deserialize_json=True)['control_table']
    qs_ctrltab=Variable.get('edpay_file_generation',deserialize_json=True)['qs_ctrltab']
    qc_ctrltab=Variable.get('edpay_file_generation',deserialize_json=True)['qc_ctrltab']
    config_file = Variable.get('edpay_file_generation',deserialize_json=True)['parameter']
    merchant =" "
    file_date =" "
    batch_no=" "
    count=0
    rownum=0
    totcount=0
    table_id = []
    destination_cloud_storage_uri =[]
    destination_folder = []
    edp_destination_cloud_storage_uri =[]
    output_file_name = []
    email=[]
    merchant_code =[]
    header=[]
    delimiter=[]
    extension=[]
    lck_file=[]
    qufile_date=[]
    batch_number=[]
    email_to = Variable.get("edpay_file_generation", deserialize_json=True)['email_to']
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
                bucket_name = Variable.get('edpay_file_generation',deserialize_json=True)['bucket']
            
                client2 = storage.Client()
                bucket = client2.get_bucket(bucket_name)             
                blob = bucket.blob(i['query_file'])        
                text = blob.download_as_string()
                stg_query = text.decode('utf-8')
                      
                logging.info(" stg_query : {}".format(stg_query)) 

                logging.info("Value of i['date_time_merchant'] {} ".format(i['date_time_merchant']))
                
                logging.info(" len(extract_start_datetime) : {} len(extract_end_datetime) : {} len(file_date):{} ".format(len(extract_start_datetime),len(extract_end_datetime),len(file_date))) 
                
                if len(extract_start_datetime)>19 or len(extract_end_datetime)>19 or len(file_date)>19 or extract_end_datetime == None or extract_start_datetime == None or extract_end_datetime == 'None' or extract_start_datetime == 'None' or file_date=='None' or file_date is None:
                    errorcount = errorcount + 1
                    result = Sendemail(email_to,"Extraction failed for " +merchant,"Please check the datetime format in the control table, Correct format is YYYY-mm-ddTHH:MM:SS \n\nJob name pdh_edpay_file_generation",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                    break
                    
                if (file_date!='None' or file_date is not None):
                    loadDate = file_date.replace("-","").replace(":","").replace(" ","")
                              
                projectID,datasetID,target_table=i['source_table'].split('.',3);
                target_table= "stgng"+target_table+loadDate
                stag_proj_dataset = Variable.get("edpay_file_generation", deserialize_json=True)['stag_proj_dataset']
                stg_target_table = stag_proj_dataset +"."+target_table
            
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
                destination_folder.append(i['destination_folder'])
                edp_destination_cloud_storage_uri.append(i['edp_destination_cloud_storage_uri'])
                output_file_name.append(i['output_file_name']+loadDate)
                email.append(i['email'])
                merchant_code.append(i['merchant_name'])
                header.append(i['header'])
                delimiter.append(i['delimiter'])
                extension.append(i['extension'])
                lck_file.append(i['lck_file'])
                qufile_date.append(file_date)
                batch_number.append(batch_no)
                                
                kwargs['ti'].xcom_push(key="batch_number", value=batch_number)                
                kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
                kwargs['ti'].xcom_push(key="table_id", value=table_id)
                kwargs['ti'].xcom_push(key="header", value=header)
                kwargs['ti'].xcom_push(key="delimiter", value=delimiter)
                kwargs['ti'].xcom_push(key="extension", value=extension)
                kwargs['ti'].xcom_push(key="lck_file", value=lck_file)                                 
                kwargs['ti'].xcom_push(key="destination_cloud_storage_uri", value=destination_cloud_storage_uri)
                kwargs['ti'].xcom_push(key="destination_folder", value=destination_folder)
                kwargs['ti'].xcom_push(key="output_file_name", value= output_file_name)
                kwargs['ti'].xcom_push(key="email", value=email)
                kwargs['ti'].xcom_push(key="control_table", value=control_table)
                kwargs['ti'].xcom_push(key="merchant", value=merchant_code)
                kwargs['ti'].xcom_push(key="execTimeInAest", value=execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
                kwargs['ti'].xcom_push(key="edp_destination_cloud_storage_uri", value=edp_destination_cloud_storage_uri)

                break

    if count >= 1 and count == totcount :
        kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
        kwargs['ti'].xcom_push(key="email_to", value=email_to)
        return True
    elif totcount == 0 or count == 0 :  
        result = Sendemail(email_to,"Extraction failed, No entry in the control table for file generation process","No entry in the control table for file generation process.\n\nJob name - pdh_edpay_file_generation",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        return False
    elif count >= 1 and count != totcount :  
        result = Sendemail(email_to,"Extraction failed for some merchant codes ","Please check the control table and log.\n\nJob name - pdh_edpay_file_generation",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
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
    
def query_execution(current_query):

    try:
        client = bigquery.Client()
        query_job = client.query(        
        current_query,location='US')
        rows = " "
        rows = query_job.result()  

        return True,rows
        
    except Exception as e:
        logging.info("Exception:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(),"UTC","Australia/NSW")
        email_to = Variable.get("edpay_file_generation", deserialize_json=True)['email_to']
        result = Sendemail(email_to,"Job Failure, exception raised in file generation process ","Exception:\n"+str(e)+"\n\nJob name - pdh_edpay_file_generation",execTimeInAest.strftime("%Y-%m-%d %H:%M:%S"))
        raise AirflowException("Query failure")
        return False,rows    

def extractToGCS(**kwargs):
    batch_no = kwargs.get('templates_dict').get('batch_number')
    qufile_dt = kwargs.get('templates_dict').get('qufile_date')
    email_id  = kwargs.get('templates_dict').get('email') 
    table_id = kwargs.get('templates_dict').get('table_id')
    header = kwargs.get('templates_dict').get('header')
    delimiter = kwargs.get('templates_dict').get('delimiter')
    extension = kwargs.get('templates_dict').get('extension')
    lck_file = kwargs.get('templates_dict').get('lck_file')
    outputGCSlocation = kwargs.get('templates_dict').get('destination_cloud_storage_uri')
    destFolder = kwargs.get('templates_dict').get('destination_folder')
    output_file_name = kwargs.get('templates_dict').get('output_file_name')
    errorcount = int(kwargs.get('templates_dict').get('errorcount'))
    email_to = kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant_code = kwargs.get('templates_dict').get('merchant')
    edpDestGCSlocation = kwargs.get('templates_dict').get('edp_destination_cloud_storage_uri')
    
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
    table_id = ast.literal_eval(table_id)
    header = ast.literal_eval(header)
    delimiter = ast.literal_eval(delimiter) 
    extension = ast.literal_eval(extension)
    lck_file = ast.literal_eval(lck_file)    
    output_file_name = ast.literal_eval(output_file_name)
    outputGCSlocation = ast.literal_eval(outputGCSlocation)
    destFolder = ast.literal_eval(destFolder)
    merchant_code = ast.literal_eval(merchant_code)
    edpDestGCSlocation = ast.literal_eval(edpDestGCSlocation)
        
    logging.info("table_id {}".format(table_id))
    logging.info("output_file_name {}".format(output_file_name))
    logging.info("outputGCSlocation {}".format(outputGCSlocation))
    logging.info("destFolder {}".format(destFolder))
    logging.info("edpDestGCSlocation {}".format(edpDestGCSlocation))
  
    job_config = bigquery.ExtractJobConfig()
    client = bigquery.Client()
    client1= storage.Client()

    for index in range(0,len(table_id)):
        try:
            file_name=''
            count+=1
            logging.info("index{}".format(index))
            logging.info("len(table_id){}".format(len(table_id)))
            logging.info("table_id[index] {}".format(table_id[index]))
            logging.info("table_id {}".format(table_id))
            absoluteDestinationUri = outputGCSlocation[index] + output_file_name[index] + "." + str(extension[index])
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
            job_config=job_config)  
            extract_job.result()  
            logging.info("Exported {} to {}".format(table_id[index], absoluteDestinationUri))
                  
            logging.info("outputGCSlocation[index] {}".format(outputGCSlocation[index])) 
            bucket_name=outputGCSlocation[index].split('/')[2]
            logging.info("bucket_name {}".format(bucket_name))  
            
            folder_name=destFolder[index]
            logging.info("folder_name {} ".format(folder_name))  
                       
            bucket= client1.bucket(bucket_name) 
            
            logging.info("edpDestGCSlocation[index] {}".format(edpDestGCSlocation[index])) 
            edp_bucket_name=edpDestGCSlocation[index].split('/')[2]
            logging.info("edp_bucket_name {}".format(edp_bucket_name))            

            source_object=absoluteDestinationUri.split('/')[3]
            logging.info("source_object {}".format(source_object)) 
     
            src_obj_pdh=bucket_name+"/"+file_name
            logging.info("src_obj_pdh {}".format(src_obj_pdh)) 
            
            dest_obj_pdh=bucket_name+"/"+folder_name
            logging.info("dest_obj_pdh {}".format(dest_obj_pdh)) 
            
            copy_cmd=" gs://"+src_obj_pdh+" gs://"+dest_obj_pdh
            logging.info("copy_cmd {}".format(copy_cmd))
            
            logging.info("lck_file {}" .format(lck_file[index]))
            if lck_file[index]=='Y':
                time.sleep(10)         
                blob=bucket.blob(destFolder[index]+output_file_name[index]+".lck")
                logging.info("lck_file {}" .format(bucket.blob(destFolder[index]+output_file_name[index]+".lck")))
                blob.upload_from_string(" ",content_type = 'text/plain')
        
            result,rows = query_execution('drop table '+table_id[index]+';') #dropping table
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
            result = Sendemail(email_to,"Extraction failed in task extractToGCS ","Exception raised in task extractToGCS \n\nJob name - edpay_file_generation failed.",execTimeInAest)

    kwargs['ti'].xcom_push(key="email", value=email) 
    kwargs['ti'].xcom_push(key="qufile_date", value=qufile_date)
    kwargs['ti'].xcom_push(key="batch_number", value=batch_number)    
    kwargs['ti'].xcom_push(key="errorcount", value=errorcount)
    kwargs['ti'].xcom_push(key="email_to", value=email_to)
    kwargs['ti'].xcom_push(key="merchant", value=merchant)
    kwargs['ti'].xcom_push(key="complete_file_name", value=complete_file_name) 
    kwargs['ti'].xcom_push(key="file_path", value=file_path)
    kwargs['ti'].xcom_push(key="bucket_name", value=bucket_name)
    kwargs['ti'].xcom_push(key="edp_bucket_name", value=edp_bucket_name)
    kwargs['ti'].xcom_push(key="source_object", value=source_object)
    kwargs['ti'].xcom_push(key="src_obj_pdh", value=src_obj_pdh)
    kwargs['ti'].xcom_push(key="dest_obj_pdh", value=dest_obj_pdh)
    kwargs['ti'].xcom_push(key="copy_cmd", value=copy_cmd)
   
    return True                  
    
def updatecontrolTable(**kwargs):
    batch_number = kwargs.get('templates_dict').get('batch_number')
    qufile_date = kwargs.get('templates_dict').get('qufile_date')
    control_table = kwargs.get('templates_dict').get('control_table')
    complete_file_name=kwargs.get('templates_dict').get('complete_file_name')
    file_path=kwargs.get('templates_dict').get('file_path')
    qu_ctrltab=Variable.get('edpay_file_generation',deserialize_json=True)['qu_ctrltab']
    email  = kwargs.get('templates_dict').get('email') 
    error_email=kwargs.get('templates_dict').get('email_to')
    execTimeInAest = kwargs.get('templates_dict').get('execTimeInAest')
    merchant = kwargs.get('templates_dict').get('merchant')
    errorcount=int(kwargs.get('templates_dict').get('errorcount'))
    qufile_date = ast.literal_eval(qufile_date)
    merchant = ast.literal_eval(merchant)
    email = ast.literal_eval(email)
    batch_number = ast.literal_eval(batch_number)
    complete_file_name=ast.literal_eval(complete_file_name)
    file_path=ast.literal_eval(file_path)
    
    environment= Variable.get('edpay_file_generation',deserialize_json=True)['environment']

    for index in range(0,len(merchant)):
        logging.info("len(merchant){}".format(len(merchant)))
        logging.info("merchant {}".format(merchant[index]))
        qu_ctrl=qu_ctrltab.replace('ctrl',control_table).replace('btc',batch_number[index]).replace('mrc',merchant[index]).replace('fldt',qufile_date[index])
        logging.info("qu_ctrl {}".format(qu_ctrl))
        result,rows =query_execution(qu_ctrl)
            
        logging.info("email {}".format(email[index]))                   
        result = Sendemail(email[index],"["+environment+"]File generation for "+merchant[index]+" "+execTimeInAest,"File generation for "+merchant[index]+" completed successfully.\n\nJob name pdh_edpay_file_generation",execTimeInAest)

    if errorcount == 0:
        return True
    if errorcount >=1:
        result = Sendemail(error_email,"pdh_edpay_file_generation failed"," Task updatecontrolTable failed, extraction failed for some merchants.\n\nPlease check the control table and log",execTimeInAest)
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
                    'table_id': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='table_id') }}",
                    'header': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='header') }}",
                    'delimiter': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='delimiter') }}",    
                    'extension': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='extension') }}",   
                    'lck_file': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='lck_file') }}",                       
                    'destination_cloud_storage_uri': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='destination_cloud_storage_uri')}}",
                    'destination_folder': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='destination_folder')}}",
                    'edp_destination_cloud_storage_uri': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='edp_destination_cloud_storage_uri')}}",
                    'output_file_name': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='output_file_name') }}",
                    'errorcount': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='errorcount') }}",
                    'email_to': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to') }}",
                    'execTimeInAest': "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='execTimeInAest')}}",
                    'merchant': "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='merchant')}}"
                   }
)

movePDHToEDP = GCSToGCSOperator(
    task_id='movePDHToEDP',
    source_bucket="{{task_instance.xcom_pull(task_ids='extractToGCS',key='bucket_name')}}",
    source_object="{{task_instance.xcom_pull(task_ids='extractToGCS',key='source_object')}}",
    destination_bucket="{{task_instance.xcom_pull(task_ids='extractToGCS',key='edp_bucket_name')}}",
    google_cloud_storage_conn_id='rewards_gcs',
    dag=dag
)
 
move_cmd = """ gsutil mv  {{task_instance.xcom_pull(task_ids='extractToGCS',key='copy_cmd')}} """ 

movePDHToPDH = BashOperator(
    task_id='movePDHToPDH',
    bash_command =move_cmd,
    dag=dag
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
                   'complete_file_name': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='complete_file_name')}}",
                   'file_path': "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='file_path')}}"                   
                   })
                   

getConfigDetails >> extractToGCS >> movePDHToEDP >> movePDHToPDH >> updatecontrolTable