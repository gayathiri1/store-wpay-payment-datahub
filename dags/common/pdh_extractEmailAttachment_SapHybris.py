from airflow import DAG
import pickle
import os.path
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
today = today_aest.date() - timedelta(int(offsetDays))

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
        with open('/home/airflow/gcs/data/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/home/airflow/gcs/data/MerFeeEmailKey.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('/home/airflow/gcs/data/token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    return service


def get_email_list():
    service = get_gmail_service()
    search_string = Variable.get("sapHybris", deserialize_json=True)['search_string']
    results = service.users().messages().list(userId='me',q=search_string,maxResults=10).execute()
    for i in results['messages']:
        file_dt = None
        mssg = service.users().messages().get(userId='me',id=i['id']).execute()
        for hdr in mssg['payload']['headers']:
            if hdr['name'] == 'Date':
                file_dt = parser.parse(hdr['value'][0:31].strip(' ')).date()
                if file_dt == today:
                    print('**********MATCH FOUND***********')
                    download_attachment(i['id'],service)
                else:
                    pass


def download_attachment(id, service):
    results = service.users().messages().get(userId='me', id=id).execute()
    for part in results['payload']['parts']:
        print(part)
        newvar = part['body']
        if 'attachmentId' in newvar:
            att_id = newvar['attachmentId']
            att = service.users().messages().attachments().get(userId='me', messageId=id, id=att_id).execute()
            data = att['data']
            file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
            # path = part['filename'].replace(' ','').replace(':','').replace('-','')
            path = part['filename'].split(':')[0].replace('-', '')[:-10] + '_' + part['filename'].split(':')[0].replace('-', '')[-10:-2] + '.csv'
            print(f'path is : {path}')
            with open('/home/airflow/gcs/data/'+ path, 'wb') as f:
                f.write(file_data)


cleanup_dags_folder = BashOperator(
    task_id='task_cleanup_dags_folder',
    bash_command='gsutil rm gs://us-central1-pdh-composer-pr-e2c7bc55-bucket/data/QCItem* 2> /dev/null || true',
    dag=dag,
    )


task_copy_email_attachment = PythonOperator(
    task_id='task_copy_email_attachment',
    python_callable=get_email_list,
    dag=dag,
    )


move_sapHybris_file = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='copy_sapHybris_file',
    source_bucket='us-central1-pdh-composer-pr-e2c7bc55-bucket',
    source_object='data/*.csv',
    destination_bucket='pdh_prod_incoming',
    destination_object='others/landing/saphybris/',
    move_object=False,
    dag=dag,
)


cleanup_dags_folder >> task_copy_email_attachment >> move_sapHybris_file