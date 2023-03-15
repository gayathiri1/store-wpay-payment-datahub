####################################################################
#Purpose: Re-usable utilities for payment data hub                 #
#Version: v1.0                                                     #
#Created Date: 30/03/2021                                          #
#Modifed Date: 05/07/2022                                          #
#Author: Rupesh Dubey                                              #
#######################START########################################
######Comment to test Ignore file issue for pdh_utilities.py######
from __future__ import print_function
import pickle
import os.path
import base64
import pandas as pd
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account                       
from google.auth.transport.requests import Request
from google.cloud import bigquery
from jinja2 import Template
from zlibpdh import curate_sql as cs
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from google.oauth2.credentials import Credentials


class PDHUtils:
    SCOPES = ['https:/www.googleapis.com/auth/gmail.send']
    sender = 'payments_datahub@woolworths.com.au'
    bq_client = bigquery.Client()

    @staticmethod
    def get_email_service():
        """
                    send the gmail notification from the service account credentials
                """
        creds = None
        if os.path.exists('/home/airflow/gcs/data/tokenProd.pickle'):
            with open('/home/airflow/gcs/data/tokenProd.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    '/home/airflow/gcs/data/ProdCredential.json', cls.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('/home/airflow/gcs/data/tokenProd.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('gmail', 'v1', credentials=creds)
        return service

    @classmethod
    def send_email(cls,to, subject, message_text):
        service = cls.get_email_service()
        message = MIMEText(message_text)
        message['to'] = to
        message['from'] = cls.sender
        message['subject'] = subject
        message = {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}
        try:
            message = (service.users().messages().send(userId=cls.sender, body=message)
                       .execute())
            return f"Notification sent successfully.Message Id: {message['id']}"
        except Exception as e:
            return f"Notification failed due to error: {e}"

    @classmethod
    def send_email_attachment(cls, message):
        service = cls.get_email_service()
        message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        try:
            message = (service.users().messages().send(userId=cls.sender, body=message)
                       .execute())
            print('Message Id: %s' % message['id'])
            return message
        except Exception as e:
            print(f'An error occurred:{e}')

    @classmethod
    def load_csv_to_bq(cls,dag,uid,**kwargs):
        source_objects = ''
        bucket = kwargs['run_config']['bucket']
        file_name = kwargs['run_config']['file_name']
        delimiter=kwargs['delimiter_used']
        print(f'delimiter :=>{delimiter}')
        object_name = file_name.split('/')[2]
        load_type = kwargs[object_name]['load_type']

        if load_type == 'H':
            truncateSql= 'truncate table ' + kwargs[object_name]['dataset_name'] + \
                         '.' +kwargs[object_name]['table_name']
            truncate_job = cls.bq_client.query(truncateSql)
            for res in truncate_job:
                print(res)

            path = file_name.split('/')
            path[2] = '*'
            source_objects = ['/'.join(path)][0]

        elif load_type == 'I':
            print(f'file_name is : {file_name}')
            print('project Name : {}'.format(kwargs['project_name']))
            print('table Name : {}'.format(kwargs[object_name]['table_name']))
            print(f'bucket : {bucket}')
            source_objects = file_name

        else:
            print('Wrong value for parameter load_type. Should be H or I')
            exit(1)
            
        #Logic to handle MMI Feed Format.
        if (kwargs[object_name]['table_name']).lower() == 'mmi_store_data_feed'\
            or (kwargs[object_name]['table_name']).lower() == 'dn_wpaygfssst'\
             or (kwargs[object_name]['table_name']).lower() == 'eftpos_tx_100_01':            
            t1 = GoogleCloudStorageToBigQueryOperator(
                task_id='load_csv_to_bq',
                bucket=bucket,
                source_objects=[source_objects],
                destination_project_dataset_table=kwargs['project_name'] + ':'
                                              + 'pdh_staging_ds' + '.'
                                              + kwargs[object_name]['table_name'] + uid,
                create_disposition="CREATE_IF_NEEDED",
                schema_object=kwargs[object_name]['schema'],
                schema_fields=None,
                skip_leading_rows=0,                
                field_delimiter=delimiter,
                autodetect=False,
                dag=dag
            )
            try:
                t1.execute(dict())
            except Exception as e:
                print(f'Exception as {e}')
        #BAU logic.
        else:
            t1 = GoogleCloudStorageToBigQueryOperator(
                task_id='load_csv_to_bq',
                bucket=bucket,
                source_objects=[source_objects],
                destination_project_dataset_table=kwargs['project_name'] + ':'
                                                  + 'pdh_staging_ds' + '.'
                                                  + kwargs[object_name]['table_name'] + uid,
                create_disposition="CREATE_IF_NEEDED",
                schema_object=kwargs[object_name]['schema'],
                schema_fields=None,
                skip_leading_rows=1,
                field_delimiter=delimiter,                             
                autodetect=False,
                dag=dag
            )
            try:
                t1.execute(dict())
            except Exception as e:
                print(f'Exception as {e}')

    @classmethod
    def reflect_bq_schema_curated(cls,table_name,file_date, file_name, pdh_load_time, payload_id,uid):
        sqlTemplate = Template(cs.CurateSqls.get_curation_sql(table_name))
        sqlFinal = sqlTemplate.render(staging_table='pdh_staging_ds'+'.'+table_name+uid,
                                      file_date=file_date,
                                      file_name=file_name,
                                      pdh_load_time=pdh_load_time,
                                      payload_id=payload_id
                                      )
        print(f'sqlFinal :=> {sqlFinal}')
        query_job = cls.bq_client.query(sqlFinal)
        for row in query_job:
            print(row)

        dropSql = 'drop table pdh_staging_ds.' + table_name + uid
        query_job = cls.bq_client.query(dropSql)
        for row in query_job:
            print(row)


    @classmethod
    def reflect_bq_schema(cls,dataset_name,table_name,file_date, file_name, pdh_load_time, payload_id,uid):
        sql = 'select \'SELECT SAFE.PARSE_DATE(\\\'%Y%m%d\\\',\\\'{0}\\\') as file_date,\\\'{1}\\\' as file_name, ' \
              'SAFE.PARSE_TIMESTAMP(\\\'%Y-%m-%d %H:%M:%S\\\',\\\'{2}\\\') as pdh_load_time,\\\'{3}\\\' as payload_id,\'|| ' \
              '(select string_agg(' \
              'case data_type' \
              ' when \'NUMERIC\' then \'SAFE_CAST(\' || column_name || \' as NUMERIC) as \' ||  column_name' \
              ' when \'TIME\' then \'SAFE.PARSE_TIME(\\\'%H:%M:%S\\\',\' || column_name || \') as \' || column_name' \
              ' when \'DATE\' then \'SAFE.PARSE_DATE(\\\'%d/%m/%Y\\\',\' || column_name || \') as \' || column_name' \
              ' when \'TIMESTAMP\' then \'SAFE.PARSE_TIMESTAMP(\\\'%Y/%m/%d %H:%M:%S\\\',\' || column_name || \') as \' || column_name' \
              ' else column_name end, "," order by ordinal_position) as column_name' \
              ' from  ' + dataset_name + '.INFORMATION_SCHEMA.COLUMNS where table_name=\'' + table_name + '\' ' \
              ' and ordinal_position not in (1,2,3,4))'
        sqlFinal = sql.format(file_date, file_name, pdh_load_time, payload_id)
        query_job = cls.bq_client.query(sqlFinal)
        for row in query_job:
            insertSql = 'Insert into ' + dataset_name + '.' + table_name + ' ' + \
                        str(row[0]) + ' from pdh_staging_ds.' + table_name + uid
            print(insertSql)
            insert_job = cls.bq_client.query(insertSql)
            for res in insert_job:
                print(res)

        dropSql = 'drop table pdh_staging_ds.' + table_name + uid
        query_job = cls.bq_client.query(dropSql)
        for row in query_job:
            print(row)

    @classmethod
    def check_delta(cls,dataset, table_name, file_name):
        sql = f'select distinct 1 as flag from {dataset}.{table_name} where file_name = \'{file_name}\''
        print(sql)
        res = cls.bq_client.query(sql)
        for row in res:
            return row['flag']

    @staticmethod
    def curate_csv_files(src_file,trgt_file,curate_flag):
        print('gs://'+src_file)
        header = int(curate_flag[1:]) if curate_flag[1:] else 0
        if 'GFS_PDH_TXNACT' in src_file:
            df = pd.read_csv('gs://' + src_file, dtype=str, encoding='ISO-8859-1', header=header)
            df.drop(df.tail(1).index, inplace=True)
            df.to_csv('gs://'+trgt_file,index=False)        
        elif 'APM_TRAN' in src_file:
            df = pd.read_csv('gs://' + src_file, sep='|',dtype=str, encoding='ISO-8859-1', header=header+1)            
            df.to_csv('gs://'+trgt_file, sep='|', index=False)
        elif 'MMI_STORE_FEED' in src_file:
            df = pd.read_csv('gs://' + src_file, sep='|', dtype=str, skiprows=1, encoding='ISO-8859-1', header=None)            
            df.drop(df.tail(1).index, inplace=True)
            df.to_csv('gs://'+trgt_file,sep='|', index=False, header=None)
        elif 'eftpos_tx_100_01' in src_file:
            df = pd.read_csv('gs://' + src_file, sep=',', dtype=str, skiprows=1, encoding='ISO-8859-1', header=None)            
            df.drop(df.tail(1).index, inplace=True)
            df.to_csv('gs://'+trgt_file,sep=',', index=False,header=None)
        elif 'dn_wpaygfssst' in src_file:
            df = pd.read_csv('gs://' + src_file, sep=',', dtype=str, skiprows=1, encoding='ISO-8859-1', header=None)            
            df.drop(df.tail(1).index, inplace=True)
            df.to_csv('gs://'+trgt_file,sep=',', index=False, header=None)
        else:
            df = pd.read_csv('gs://'+src_file,dtype=str,encoding='unicode_escape',header=header)
            df.drop(df.tail(1).index, inplace=True)
            df.to_csv('gs://'+trgt_file,index=False)

    @classmethod
    def upload_gs_to_bq(cls,SPREADSHEET_ID,RANGE_NAME,table_id,project_name):
        SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets.readonly']
        secret_file = os.path.join(os.getcwd(), '/home/airflow/gcs/data/gcp-wow-wpay-paydathub-prod-07bbd8b2b461.json')
        
        #Check for UAT SA.        
        if 'uat' in project_name.lower() or 'dev' in project_name.lower():
            secret_file = os.path.join(os.getcwd(), '/home/airflow/gcs/data/gcp-wow-wpay-paydathub-uat-a343797a8cbe.json')
        
        creds = service_account.Credentials.from_service_account_file(secret_file, scopes=SCOPES)        
        service = build('sheets', 'v4', credentials=creds)
        
        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                    range=RANGE_NAME).execute()
        values = result.get('values', [])
        df = pd.DataFrame(values)
        df.columns = df.iloc[0]
        df = df.reindex(df.index.drop(0))
        df.columns = map(str.lower, df.columns)
        df.columns = df.columns.str.replace(' ', '_')
        df.to_gbq(table_id, project_name, if_exists='replace')

    @classmethod
    def is_csv_file_empty(cls, file_path):
        #check if csv file is empty, only for pdh_dev_incoming/gfs/landing/EFTPOS for now
        print("Checking if csv file is empty, file name: " + file_path)
        try:
            if ('dn_wpaygfssst' in file_path or 'mmi_store_data_feed' in file_path or 'eftpos_tx_100_01' in file_path):
                df = pd.read_csv(file_path, nrows=10, header=None)
            else:
                df = pd.read_csv(file_path, nrows=10)
        except pd.errors.EmptyDataError as e:
            return True

        if df.empty:
            print('%s is empty.' % file_path)
            return True
        else:
            print('%s is not empty.' % file_path)
            return False