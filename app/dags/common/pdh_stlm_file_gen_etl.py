from airflow import DAG
from google.cloud import storage, pubsub_v1
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import logging
from zlibpdh import pdh_utilities as pu
import pendulum
import pytz
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from dataclasses import asdict
import json

#Bugfix DATPAY-3505 to handle daylight savings
local_tz = pendulum.timezone("Australia/Sydney")

default_args = {
    'start_date': datetime(2021,7,12, tzinfo=local_tz),    
}

logging.info("constructing dag - using airflow as owner")

dag_name = "pdh_stlm_file_gen_etl"
dag = DAG(
          'pdh_stlm_file_gen_etl',
          catchup=False, 
          default_args=default_args,
          max_active_runs=1, 
          schedule_interval="00 04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23 * * *")

# https://stackoverflow.com/a/70397050/482899
#log_prefix = f"[pdh_batch_pipeline][{dag_name}]"
log_prefix = "[pdh_batch_pipeline]"+"["+dag_name+"]"
exec_time_aest = get_current_time_str_aest()


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        #return f"{log_prefix} {msg}", kwargs
        return log_prefix+" "+msg, kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")


def get_project_id():
    """
    Get GCP project_id from airflow variable, which has been configured in control_table
    """
    control_table = Variable.get("stlm_file_gen_etl", deserialize_json=True)["control_table"]
    project_id = control_table.split(".")[0]
    logger.debug(f"project_id ={project_id}")
    return project_id


publisher = pubsub_v1.PublisherClient()
project_id = get_project_id()
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables
topic_path = publisher.topic_path(project_id, topic_id)
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))


def readexecuteQuery(**kwargs):
    try:
        bucket = Variable.get('stlm_file_gen_etl', deserialize_json=True)['bucket']
        query_file = Variable.get('stlm_file_gen_etl', deserialize_json=True)['files']
        qc_select = Variable.get('stlm_file_gen_etl', deserialize_json=True)['qc_select']
        emailTo = Variable.get("stlm_file_gen_etl", deserialize_json=True)['emailTo']
        emailException = Variable.get("stlm_file_gen_etl", deserialize_json=True)['emailException']
    except Exception as e:
        logging.info("Exception raised in readexecuteQuery while reading variables:{}".format(e))

    updated_files = []
    unupdated_files = []
    error_count = 0
    updated_query = []
    unupdated_query = []
    updated_codes_ctrl_tab = []

    for i in query_file:
        execTimeInAest = convertTimeZone(datetime.now(), "UTC", "Australia/NSW")
        current_time = execTimeInAest.strftime("%H:%M:%S")
               

        if (current_time >= i['start_time'] and current_time <= i['end_time']) \
        and (i['run_flag']).upper() == 'Y':
            logging.info(">>>>> Following merchants will be picked up settlement process...")        
            logging.info("{} start_time:".format(i['start_time']))
            logging.info("{} end_time:".format(i['end_time']))
            logging.info("{} run_flag:".format(i['run_flag']))
            logging.info("{} merchant:".format(i['merchant']))                       
            row = ''
            rows1 = ''
            try:
                if (i['access_google_sheet']).upper() == 'Y':
                    projectID, datasetID, target_table = i['gs_table_name'].split('.', 3)
                    pu.PDHUtils.upload_gs_to_bq(i['gs_unique_id'], i['gs_sheet_name'], datasetID + "." + target_table,
                                                projectID)
                    logging.info("{} refreshed successfully".format(i['gs_table_name']))
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(bucket)
                blob = bucket.blob(i['query_file'])
                text = blob.download_as_string()
                query_text = text.decode('utf-8')
                result, rows = processQuery(query_text, emailException)
                # if result==False:
                # unupdated_files.append(i['merchant'])
                # unupdated_query.append(i['query_file'])
                if rows is not None or rows != '':
                    logging.info("rows {} and result {}".format(rows, result))
                logging.info("Query executed for {}".format(i['query_file']))
                qc_query = qc_select.replace('mrc', i['merchant'])
                logging.info("qc_query {}".format(qc_query))
                result, rows1 = processQuery(qc_query, emailTo)
                logging.info("Control table record is fetched for file {} and merchant  {}".format(i['query_file'],
                                                                                                   i['merchant']))
                if rows1 is not None or rows1 != '':
                    logging.info("rows1 {} and result {}".format(rows1, result))
                
                if row is not None or row != '':
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
                pu.PDHUtils.send_email(emailException, subject, body)
                error_count += 1
                unupdated_files.append(i['merchant'])
                unupdated_query.append(i['query_file'])
                event_message = f"Exception raised while executing file extraction etl, in task readexecuteQuery: {str(e)}" 
                event = Event(
                    dag_name=dag_name,
                    event_status="failure",
                    event_message=event_message,
                    start_time=exec_time_aest,
                    )
                publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        else:
            logging.info(">>>>> Following merchants will not be picked up settlement process...")        
            logging.info("{} start_time:".format(i['start_time']))
            logging.info("{} end_time:".format(i['end_time']))
            logging.info("{} run_flag:".format(i['run_flag']))
            logging.info("{} merchant:".format(i['merchant']))

    kwargs['ti'].xcom_push(key="updated_codes_ctrl_tab", value=updated_codes_ctrl_tab)
    kwargs['ti'].xcom_push(key="updated_query", value=updated_query)
    kwargs['ti'].xcom_push(key="unupdated_query", value=unupdated_query)
    kwargs['ti'].xcom_push(key="unupdated_files", value=unupdated_files)
    kwargs['ti'].xcom_push(key="updated_files", value=updated_files)
    kwargs['ti'].xcom_push(key="error_count", value=error_count)
    return True


def processQuery(query, emailException):
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        rows = " "
        rows = query_job.result()
        return True, rows
    except Exception as e:
        logging.info("Exception raised while executing file extraction etl in processQuerys:{}".format(e))
        execTimeInAest = convertTimeZone(datetime.now(), "UTC", "Australia/NSW")
        subject = "Exception raised while executing file extraction etl in processQuery" + execTimeInAest.strftime(
            "%Y-%m-%d %H:%M:%S")
        body = "Exception raised while executing file extraction etl in processQuery: \n" + str(e)
        pu.PDHUtils.send_email(emailException, subject, body)
        event_message = f"Exception raised while executing file extraction etl in processQuery {str(e)}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
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
    try:
        subject = "File generation ETL " + loadDateTime
        emailTo = Variable.get("stlm_file_gen_etl", deserialize_json=True)['emailTo']
        emailException = Variable.get("stlm_file_gen_etl", deserialize_json=True)['emailException']
        #Check for Settlment Cycle
        is_settlement_cycle = settlement_cycle_check()
        if error_count == 0:
            bodytext = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files
            event_message = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))    
        elif error_count > 0 and is_settlement_cycle == False:
            bodytext = "Division not in Settlement Cycle - Settlments ETLs will be skipped."
            event_message = "Division not in Settlement Cycle - Settlments ETLs will be skipped."
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,)
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))              
        else:
            bodytext = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files + "\n\nControl table is not updated for merchant codes: " + unupdated_files + "\n\nSkipped(not in STLM cycle) or Unsuccessful query files: " + unupdated_query
            event_message = "Control table is updated for merchant codes: " + updated_codes_ctrl_tab + "\n\nQuery files executed successfully: " + updated_query + "\n\nMerchant codes in query file: " + updated_files + "\n\nControl table is not updated for merchant codes: " + unupdated_files + "\n\nSkipped(not in STLM cycle) or Unsuccessful query files: " + unupdated_query
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))        
        logging.info("subject: {} emailto: {} bodytext: {}".format(subject, emailTo, bodytext))
        pu.PDHUtils.send_email(emailTo, subject, bodytext)
    except Exception as e:
        logging.info("Exception raised while executing file extraction etl in sendEmail:{}".format(e))
        event_message = f"Exception raised while executing file extraction etl in sendEmail:{e}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,)
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        bodytext = f"Exception raised while executing file extraction etl in sendEmail:{str(e)}"
        pu.PDHUtils.send_email(emailException, subject, bodytext)
    return True


def convertTimeZone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt

#Function to check for Settlment cycle.
def settlement_cycle_check():    
    is_settlement_cycle = True    
    try:
        client = bigquery.Client()
        query_string_syd_hour = """select extract(hour from current_time("Australia/Sydney"));"""
        query_job_syd_hr = client.query(query_string_syd_hour)
        #Store syd_hour value in variable
        for row in query_job_syd_hr:
            syd_hour = int(row[0])
        logging.info("syd_hour is :=> {}".format(syd_hour))
        query_string_main = """select count(*)
        from `pdh_ref_ds.ref_merchant_settlement_window`
        where is_active = "Y"
        and settlement_cycle = {0};""".format(syd_hour)
        query_job_main = client.query(query_string_main)    
        for row in query_job_main:
            qc_count_main = row[0]        
        if qc_count_main == 0:
            is_settlement_cycle = False
        
    except Exception as e:
        logging.info("Exception:{}".format(e))                
        event_message = f"Exception raised while executing file extraction etl in settlement_cycle_check {str(e)}"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
            )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
    return is_settlement_cycle


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
    trigger_dag_id="pdh_stlm_file_gen_bq_to_gcs",
    dag=dag,
)

readexecuteQuery >> triggerdag >> sendEmail