from airflow import DAG
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pendulum
import pytz

#Bugfix DATPAY-3505 to handle daylight savings
local_tz = pendulum.timezone("Australia/Sydney")

default_args = {
    'start_date': datetime(2021,7,12, tzinfo=local_tz),    
}

logging.info("constructing dag - using airflow as owner")

dag_name = "pdh_file_gen_etl"

try:
   control_table = Variable.get("file_gen_etl", deserialize_json=True)["control_table"] 
   project_id = control_table.split(".")[0]
   if "PROD" in project_id.upper():
       dag = DAG('pdh_file_gen_etl', catchup=False, default_args=default_args,schedule_interval="30 03,04,05,06,09,11 * * *")
   else:
      dag = DAG('pdh_file_gen_etl', catchup=False, default_args=default_args,schedule_interval="30 15,16,17 * * *")
except Exception as e:
    logging.info("Exception in setting DAG schedule:{}".format(e)) 



def readexecuteQuery(**kwargs):
    bucket = Variable.get('file_gen_etl', deserialize_json=True)['bucket']

    query_file = Variable.get('file_gen_etl', deserialize_json=True)['files']
    qc_select = Variable.get('file_gen_etl', deserialize_json=True)['qc_select']
    emailTo = Variable.get("file_gen_etl", deserialize_json=True)['emailTo']
    updated_files = []
    unupdated_files = []
    error_count = 0
    updated_query = []
    unupdated_query = []
    updated_codes_ctrl_tab = []

    for i in query_file:
        execTimeInAest = convertTimeZone(datetime.now(), "UTC", "Australia/NSW")
        current_time = execTimeInAest.strftime("%H:%M:%S")
        if current_time >= i['start_time'] and current_time <= i['end_time']:
            row = ''
            rows1 = ''
            try:
                if i['access_google_sheet'] == 'Y':
                    projectID, datasetID, target_table = i['gs_table_name'].split('.', 3)
                    pu.PDHUtils.upload_gs_to_bq(i['gs_unique_id'], i['gs_sheet_name'], datasetID + "." + target_table,
                                                projectID)
                    logging.info("{} refreshed successfully".format(i['gs_table_name']))
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(bucket)
                blob = bucket.blob(i['query_file'])
                text = blob.download_as_string()
                query_text = text.decode('utf-8')
                result, rows = processQuery(query_text, emailTo)
                # if result==False:
                # unupdated_files.append(i['merchant'])
                # unupdated_query.append(i['query_file'])
                logging.info("rows {} and result {}".format(rows, result))
                logging.info("Query executed for {}".format(i['query_file']))
                qc_query = qc_select.replace('mrc', i['merchant'])
                logging.info("qc_query {}".format(qc_query))
                result, rows1 = processQuery(qc_query, emailTo)
                logging.info("Control table record is fetched for file {} and merchant  {}".format(i['query_file'],
                                                                                                   i['merchant']))
                logging.info("rows1 {} and result {}".format(rows1, result))
                logging.info("row before for loop  {}".format(row))
                for row in rows1:
                    updated_codes_ctrl_tab.append(row[2])
                    logging.info("updated_codes_ctrl_tab {}".format(updated_codes_ctrl_tab))
                    logging.info("row inside for loop {}".format(row))

                logging.info("row after for loop {}".format(row))
                if row is None or row == '':
                    logging.info("row inside if with None  {}".format(row))
                    error_count += 1
                    unupdated_files.append(i['merchant'])
                    unupdated_query.append(i['query_file'])
                    logging.info("unupdated_files {}".format(unupdated_files))
                    logging.info("unupdated_query {}".format(unupdated_query))
                elif row is not None:
                    logging.info("row inside if with not None   {}".format(row))
                    updated_files.append(i['merchant'])
                    updated_query.append(i['query_file'])
                    logging.info("updated_query {}".format(updated_query))
                    logging.info("updated_files {}".format(updated_files))

            except Exception as e:
                logging.info("Exception:{}".format(e))
                subject = "Exception raised while executing file extraction etl, in task readexecuteQuery" + execTimeInAest.strftime(
                    "%Y-%m-%d %H:%M:%S")
                body = "Exception raised while executing file extraction etl, in task readexecuteQuery: \n" + str(e)
                pu.PDHUtils.send_email(emailTo, subject, body)
                error_count += 1
                unupdated_files.append(i['merchant'])
                unupdated_query.append(i['query_file'])

    kwargs['ti'].xcom_push(key="updated_codes_ctrl_tab", value=updated_codes_ctrl_tab)
    kwargs['ti'].xcom_push(key="updated_query", value=updated_query)
    kwargs['ti'].xcom_push(key="unupdated_query", value=unupdated_query)
    kwargs['ti'].xcom_push(key="unupdated_files", value=unupdated_files)
    kwargs['ti'].xcom_push(key="updated_files", value=updated_files)
    kwargs['ti'].xcom_push(key="error_count", value=error_count)
    return True


def processQuery(query, emailTo):
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        rows = " "
        rows = query_job.result()
        return True, rows
    except Exception as e:
        logging.info("Exception:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(), "UTC", "Australia/NSW")
        subject = "Exception raised while executing file extraction etl in processQuery" + execTimeInAest.strftime(
            "%Y-%m-%d %H:%M:%S")
        body = "Exception raised while executing file extraction etl in processQuery: \n" + str(e)
        pu.PDHUtils.send_email(emailTo, subject, body)
        return False, rows


def sendEmail(**kwargs):
    error_count = int(kwargs.get('templates_dict').get('error_count'))
    updated_files = str(kwargs.get('templates_dict').get('updated_files'))
    # updated_files=ast.literal_eval(updated_files)
    logging.info("updated_files:{}".format(updated_files))
    unupdated_files = str(kwargs.get('templates_dict').get('unupdated_files'))
    logging.info("unupdated_files:{}".format(unupdated_files))
    # unupdated_files = ast.literal_eval(unupdated_files)
    updated_codes_ctrl_tab = str(kwargs.get('templates_dict').get('updated_codes_ctrl_tab'))
    logging.info("updated_codes_ctrl_tab:{}".format(updated_codes_ctrl_tab))
    updated_query = str(kwargs.get('templates_dict').get('updated_query'))
    logging.info("updated_query:{}".format(updated_query))
    unupdated_query = str(kwargs.get('templates_dict').get('unupdated_query'))
    logging.info("unupdated_query:{}".format(unupdated_query))
    execTimeInAest = convertTimeZone(datetime.now(), "UTC", "Australia/NSW")
    loadDateTime = execTimeInAest.strftime("%Y-%m-%d %H:%M:%S.%f")
    subject = "File generation ETL " + loadDateTime
    emailTo = Variable.get("file_gen_etl", deserialize_json=True)['emailTo']
    if error_count == 0:
        bodytext = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files
    else:
        bodytext = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files + "\n\nControl table is not updated for merchant codes: " + unupdated_files + "\n\nUnsuccessful query files: " + unupdated_query
    logging.info("subject: {} emailto: {} bodytext: {}".format(subject, emailTo, bodytext))
    pu.PDHUtils.send_email(emailTo, subject, bodytext)
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
    templates_dict={'updated_files': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_files') }}",
                    'unupdated_files': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='unupdated_files') }}",
                    'error_count': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='error_count') }}",
                    'updated_codes_ctrl_tab': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_codes_ctrl_tab') }}",
                    'updated_query': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='updated_query') }}",
                    'unupdated_query': "{{ task_instance.xcom_pull(task_ids='readexecuteQuery', key='unupdated_query') }}"
                    }
)

triggerdag = TriggerDagRunOperator(
    task_id="triggerdag",
    trigger_dag_id="pdh_file_gen_bq_to_gcs",
    dag=dag,
)

readexecuteQuery >> triggerdag >> sendEmail