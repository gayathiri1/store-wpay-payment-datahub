from airflow import DAG
import pickle
import os.path
import logging
import base64
from airflow.models import Variable
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from datetime import datetime, timedelta
from pytz import timezone
from dateutil import parser

offsetDays = Variable.get("sapHybris", deserialize_json=True)['offsetDays']
today_aest = datetime.now().astimezone(timezone('Australia/Sydney'))
hybris_file_date = today_aest.date() - timedelta(int(offsetDays))
# set the GCS bucket name
AIRFLOW_GCS_BUCKET= None
if  os.environ.get("GCS_BUCKET", "no bucket name"):
    AIRFLOW_GCS_BUCKET = os.environ.get("GCS_BUCKET", "no bucket name")

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


default_dag_args={
    'start_date' : datetime(2021, 8, 12)
}


dag = DAG(
    'pdh_extract_email_attachments_SapHybris',
    schedule_interval=None,
    default_args=default_dag_args
)


def get_gmail_service():
    """
        get the gmail credentials for the service account
    """
    creds = None
    if os.path.exists('/home/airflow/gcs/data/token.pickle'):
        logging.info('**********TOKEN EXISTS***********')
        with open('/home/airflow/gcs/data/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    else:
        logging.info('**********TOKEN DOES NOT EXIST***********')
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        logging.info('**********CREDS NOT VALID***********')
        if creds and creds.expired and creds.refresh_token:
            logging.info('**********CREDS EXPIRED***********')
            creds.refresh(Request())
        else:
            logging.info('**********CREDS NOT EXPIRED***********')
            flow = InstalledAppFlow.from_client_secrets_file(
                '/home/airflow/gcs/data/MerFeeEmailKey.json', SCOPES)
            logging.info('got the flow')
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('/home/airflow/gcs/data/token.pickle', 'wb') as token:
            logging.info('Save the credentials for the next run')
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    logging.info('returning the service')
    return service


def get_email_list():
    logging.info('**********GETTING EMAIL LIST***********')
    try:
        service = get_gmail_service()
        search_string = Variable.get("sapHybris", deserialize_json=True)['search_string']
        logging.info(f'Search string is : {search_string}')
        results = service.users().messages().list(userId='me',q=search_string,maxResults=10).execute()
        for msg in results['messages']:
            logging.info(f'msg is : {msg}')
            file_dt = None
            emai_mssg = service.users().messages().get(userId='me',id=msg['id']).execute()
            logging.info(f'emai_mssg is : {emai_mssg}')
            for hdr in emai_mssg['payload']['headers']:
                if hdr['name'] == 'Date':
                    file_dt = parser.parse(hdr['value'][0:31].strip(' ')).date()
                    if file_dt == hybris_file_date:
                        logging.info('**********MATCH FOUND***********')
                        download_attachment(msg['id'],service)
                    else:
                        logging.info(f'No match found for the date {hybris_file_date}')
                        pass
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))


def download_attachment(id, service):
    logging.info(f'Email id is : {id}')
    try:            
        results = service.users().messages().get(userId='me', id=id).execute()
        for part in results['payload']['parts']:
            logging.info(f'part is : {part}')
            newvar = part['body']
            logging.info(f'newvar is : {newvar}')
            if 'attachmentId' in newvar:    
                logging.info(f'attachmentId is : {newvar["attachmentId"]}')
                att_id = newvar['attachmentId']
                att = service.users().messages().attachments().get(userId='me', messageId=id, id=att_id).execute()
                logging.info(f'att is : {att}')
                data = att['data']
                logging.info(f'data is : {data}')
                file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                # path = part['filename'].replace(' ','').replace(':','').replace('-','')
                path = part['filename'].split(':')[0].replace('-', '')[:-10] + '_' + part['filename'].split(':')[0].replace('-', '')[-10:-2] + '.csv'
                logging.info(f'path is : {path}')
                with open('/home/airflow/gcs/data/'+ path, 'wb') as f:
                    f.write(file_data)
            else:
                logging.info(f'No attachment found in the email with id {id}')
    except Exception as e:
        logging.info("Exception Raised:{}".format(e))

cleanup_dags_folder = BashOperator(
    task_id='task_cleanup_dags_folder',
    bash_command='gsutil rm gs://{AIRFLOW_GCS_BUCKET}/data/QCItem* 2> /dev/null || true',
    dag=dag,
    )


task_copy_email_attachment = PythonOperator(
    task_id='task_copy_email_attachment',
    python_callable=get_email_list,
    dag=dag,
    )


move_sapHybris_file = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='copy_sapHybris_file',
    source_bucket={AIRFLOW_GCS_BUCKET},
    source_object='data/*.csv',
    destination_bucket='pdh_prod_incoming',
    destination_object='others/landing/saphybris/',
    move_object=False,
    dag=dag,
)


cleanup_dags_folder >> task_copy_email_attachment >> move_sapHybris_file