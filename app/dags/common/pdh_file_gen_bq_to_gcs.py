import os
import ast
import json
import logging
import time
from dataclasses import asdict
from datetime import datetime
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery, pubsub_v1, storage
from pdh_logging.event import Event
from pdh_logging.utils import get_current_time_str_aest
from zlibpdh import pdh_utilities as pu
from zlibpdh import sub_utilities as su

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 12),
}

dag_name = "pdh_file_gen_bq_to_gcs"
dag = DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
)
gcs_bucket = os.environ.get("GCS_BUCKET")

# https://stackoverflow.com/a/70397050/482899
log_prefix = f"[pdh_batch_pipeline][{dag_name}]"


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{log_prefix} {msg}", kwargs


logger = CustomAdapter(logging.getLogger(__name__), {})
logger.info(f"constructing dag {dag_name} - using airflow as owner")


def get_project_id():
    """
    Get GCP project_id from airflow variable, which has been configured in control_table
    """
    control_table = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["control_table"]
    project_id = control_table.split(".")[0]
    # logger.debug(f"{project_id=}")
    return project_id


publisher = pubsub_v1.PublisherClient()
project_id = get_project_id()
topic_id = "T_batch_pipeline_outbound_events"  # TODO: airflow variables?
topic_path = publisher.topic_path(project_id, topic_id)
# msg = {"dag_name": dag_name}
# publisher.publish(topic_path, data=json.dumps(msg).encode("utf-8"))


def send_email(email_to, email_subject, email_body, exec_time_aest):
    subject = email_subject + " " + exec_time_aest
    # logger.debug(f"{subject=} {email_to=}")
    pu.PDHUtils.send_email(email_to, subject, email_body)
    return True


def query_execution(current_query):
    try:
        client = bigquery.Client()
        query_job = client.query(current_query, location="US")
        rows = " "
        rows = query_job.result()  # Waits for the query to finish

        return True, rows
    except Exception as e:
        logger.exception(f"Exception: {e}")
        exec_time_aest = get_current_time_str_aest()
        email_to = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["email_to"]
        event_message = "Failed on exception raised during query execution."
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        send_email(
            email_to=email_to,
            email_subject=event_message,
            email_body=f"Exception:\n {e} \n\nJob name - {dag_name}",
            exec_time_aest=exec_time_aest,
        )
        raise AirflowException("Query failure")
        return False, rows


def getConfigDetails(**kwargs):
    control_table = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["control_table"]
    qs_ctrltab = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["qs_ctrltab"]
    qc_ctrltab = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["qc_ctrltab"]
    config_file = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["parameter"]
    merchant = " "
    file_date = " "
    batch_no = " "
    count = 0
    row_num = 0
    total_count = 0
    table_id = []
    destination_cloud_storage_uri = []
    output_file_name = []
    email = []
    merchant_code = []
    header = []
    delimiter = []
    extension = []
    lck_file = []
    qufile_date = []
    trigger_file = []
    batch_number = []
    split_etl = []
    split_merchant = []
    email_attachment = []
    email_to = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["email_to"]
    error_count = 0

    exec_time_aest = get_current_time_str_aest()

    result, rows = query_execution(qs_ctrltab)
    logger.debug("select control table query executed successfully")
    result, rowc = query_execution(qc_ctrltab)
    logger.debug("select count query executed successfully")

    for qcount in rowc:
        total_count = qcount[0]
        # logger.debug(f"{total_count=}")

    for row in rows:
        batch_no = row[0]
        merchant = row[2]
        extract_start_datetime = str(row[5])
        extract_end_datetime = str(row[6])
        file_date = str(row[7])

        # logger.debug(f"{merchant=}")
        # logger.debug(f"{file_date=}")
        logger.debug(f"Selected rows: {row}")
        row_num += 1
        # logger.debug(f"{row_num=}")

        for i in config_file:
            # logger.debug(f"{i=}")

            if merchant == i["merchant_name"]:
                count += 1
                # logger.debug(f"{count=}")
                bucket_name = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["bucket"]

                client2 = storage.Client()
                bucket = client2.get_bucket(bucket_name)
                blob = bucket.blob(i["query_file"])
                text = blob.download_as_string()
                stg_query = text.decode("utf-8")

                # logger.debug(f"{stg_query=}")

                # logger.debug(f"{i['date_time_merchant']=}")

                # logger.debug(
                #     f"{len(extract_start_datetime)=}"
                #     f"{len(extract_end_datetime)=}"
                #     f"{len(file_date)=}"
                # )

                if (
                    len(extract_start_datetime) > 19
                    or len(extract_end_datetime) > 19
                    or len(file_date) > 19
                    or extract_end_datetime is None
                    or extract_start_datetime is None
                    or extract_end_datetime == "None"
                    or extract_start_datetime == "None"
                    or file_date == "None"
                    or file_date is None
                ):
                    error_count = error_count + 1
                    event_message = f"Extraction failed for {merchant}"
                    event = Event(
                        dag_name=dag_name,
                        event_status="failure",
                        event_message=event_message,
                        start_time=exec_time_aest,
                    )
                    publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
                    result = send_email(
                        email_to=email_to,
                        email_subject=event_message,
                        email_body=(
                            "Please check the datetime format in the control table, "
                            "Correct format is YYYY-mm-ddTHH:MM:SS \n\n"
                            f"Job name {dag_name}",
                        ),
                        exec_time_aest=exec_time_aest,
                    )
                    break

                if i["date_time_merchant"] == "Y" and (
                    file_date != "None" or file_date is not None
                ):
                    loadDate = file_date.replace("-", "").replace(":", "").replace(" ", "")
                elif i["date_time_merchant"] == "N" and (
                    file_date != "None" or file_date is not None
                ):
                    loadDate, temptime = file_date.split(" ", 2)
                    loadDate = loadDate.replace("-", "")
                    # logger.debug(f"{loadDate=}")

                if i["access_google_sheet"] == "Y":
                    projectID, datasetID, target_table = i["gs_table_name"].split(".", 3)
                    pu.PDHUtils.upload_gs_to_bq(
                        i["gs_unique_id"],
                        i["gs_sheet_name"],
                        datasetID + "." + target_table,
                        projectID,
                    )
                    logger.debug(f"{i['gs_table_name']} refreshed successfully")

                projectID, datasetID, target_table = i["source_table"].split(".", 3)
                target_table = "stgng" + target_table + loadDate
                stag_proj_dataset = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)[
                    "stag_proj_dataset"
                ]
                stg_target_table = stag_proj_dataset + "." + target_table

                if i["date_filter"] == "Y":
                    # logger.debug(f"{i['src_date_column']=}")
                    stg_query = (
                        stg_query.replace("stg", stg_target_table)
                        .replace("src", i["source_table"])
                        .replace("ctrl", control_table)
                        .replace("sr_date_column", i["src_date_column"])
                        .replace("mrc", merchant)
                    )

                else:
                    stg_query = stg_query.replace("stg", stg_target_table).replace(
                        "src", i["source_table"]
                    )
                    logger.debug(f"query_text - {stg_query}")

                result, rows = query_execution(stg_query)

                query_cnt = "select count(*) from " + stg_target_table + ";"
                result, rows = query_execution(query_cnt)
                for row in rows:
                    total_records_exported = row[0]
                # logger.debug(f"{total_records_exported=}")

                table_id.append(stg_target_table)
                destination_cloud_storage_uri.append(i["destination_cloud_storage_uri"])
                output_file_name.append(i["output_file_name"] + loadDate)
                email.append(i["email"])
                merchant_code.append(i["merchant_name"])
                header.append(i["header"])
                delimiter.append(i["delimiter"])
                extension.append(i["extension"])
                lck_file.append(i["lck_file"])
                trigger_file.append(i["trigger_file"])
                split_etl.append(i["split_etl"])
                split_merchant.append(i["split_merchant"])
                email_attachment.append(i["email_attachment"])
                qufile_date.append(file_date)
                batch_number.append(batch_no)

                kwargs["ti"].xcom_push(key="email_attachment", value=email_attachment)
                kwargs["ti"].xcom_push(key="split_merchant", value=split_merchant)
                kwargs["ti"].xcom_push(key="split_etl", value=split_etl)
                kwargs["ti"].xcom_push(key="batch_number", value=batch_number)
                kwargs["ti"].xcom_push(key="trigger_file", value=trigger_file)
                kwargs["ti"].xcom_push(key="qufile_date", value=qufile_date)
                kwargs["ti"].xcom_push(key="table_id", value=table_id)
                kwargs["ti"].xcom_push(key="header", value=header)
                kwargs["ti"].xcom_push(key="delimiter", value=delimiter)
                kwargs["ti"].xcom_push(key="extension", value=extension)
                kwargs["ti"].xcom_push(key="lck_file", value=lck_file)
                kwargs["ti"].xcom_push(key="trigger_file", value=trigger_file)
                kwargs["ti"].xcom_push(
                    key="destination_cloud_storage_uri",
                    value=destination_cloud_storage_uri,
                )
                kwargs["ti"].xcom_push(key="output_file_name", value=output_file_name)
                kwargs["ti"].xcom_push(key="email", value=email)
                kwargs["ti"].xcom_push(key="control_table", value=control_table)
                kwargs["ti"].xcom_push(key="merchant", value=merchant_code)
                kwargs["ti"].xcom_push(
                    key="exec_time_aest",
                    value=exec_time_aest,
                )

                break

    if count >= 1 and count == total_count:
        kwargs["ti"].xcom_push(key="error_count", value=error_count)
        kwargs["ti"].xcom_push(key="email_to", value=email_to)
        return True
    elif total_count == 0 or count == 0:
        event_message = (
            "Extraction failed. No entry in the control table for file generation process"
        )
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        send_email(
            email_to=email_to,
            email_subject=event_message,
            email_body=(
                "No entry in the control table for file generation process.\n\n"
                f"Job name - {dag_name}"
            ),
            exec_time_aest=exec_time_aest,
        )
        return False
    elif count >= 1 and count != total_count:
        event_message = "Extraction failed for some merchant codes"
        event = Event(
            dag_name=dag_name,
            event_status="failure",
            event_message=event_message,
            start_time=exec_time_aest,
        )
        publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
        send_email(
            email_to=email_to,
            email_subject=event_message,
            email_body=f"Please check the control table and log.\n\nJob name - {dag_name}",
            exec_time_aest=exec_time_aest,
        )
        error_count = error_count + 1

        kwargs["ti"].xcom_push(key="error_count", value=error_count)
        kwargs["ti"].xcom_push(key="email_to", value=email_to)
        return True


def extractToGCS(**kwargs):
    batch_no = kwargs.get("templates_dict").get("batch_number")
    qufile_dt = kwargs.get("templates_dict").get("qufile_date")
    email_id = kwargs.get("templates_dict").get("email")
    trigger_file = kwargs.get("templates_dict").get("trigger_file")
    table_id = kwargs.get("templates_dict").get("table_id")
    header = kwargs.get("templates_dict").get("header")
    delimiter = kwargs.get("templates_dict").get("delimiter")
    extension = kwargs.get("templates_dict").get("extension")
    lck_file = kwargs.get("templates_dict").get("lck_file")
    outputGCSlocation = kwargs.get("templates_dict").get("destination_cloud_storage_uri")

    # loadTime = kwargs.get("templates_dict").get("loadDate")
    output_file_name = kwargs.get("templates_dict").get("output_file_name")
    error_count = int(kwargs.get("templates_dict").get("error_count"))
    email_to = kwargs.get("templates_dict").get("email_to")
    exec_time_aest = kwargs.get("templates_dict").get("exec_time_aest")
    merchant_code = kwargs.get("templates_dict").get("merchant")

    count = 0
    merchant = []
    email = []
    qufile_date = []
    batch_number = []
    complete_file_name = []
    file_path = []

    batch_no = ast.literal_eval(batch_no)
    qufile_dt = ast.literal_eval(qufile_dt)
    email_id = ast.literal_eval(email_id)
    trigger_file = ast.literal_eval(trigger_file)
    table_id = ast.literal_eval(table_id)
    header = ast.literal_eval(header)
    delimiter = ast.literal_eval(delimiter)
    extension = ast.literal_eval(extension)
    lck_file = ast.literal_eval(lck_file)
    output_file_name = ast.literal_eval(output_file_name)
    outputGCSlocation = ast.literal_eval(outputGCSlocation)
    merchant_code = ast.literal_eval(merchant_code)

    # logger.debug(f"{table_id=}")
    # logger.debug(f"{output_file_name=}")
    # logger.debug(f"{outputGCSlocation=}")

    job_config = bigquery.ExtractJobConfig()
    client = bigquery.Client()
    client1 = storage.Client()

    for index in range(0, len(table_id)):
        try:
            file_name = ""
            count += 1
            # logger.debug(f"{index=}")
            # logger.debug(f"{len(table_id)=}")
            # logger.debug(f"{table_id[index]=}")
            # logger.debug(f"{table_id=}")
            absoluteDestinationUri = (
                outputGCSlocation[index]
                + "/"
                + output_file_name[index]
                + "."
                + str(extension[index])
            )
            # logger.debug(f"{absoluteDestinationUri=}")
            file_name = output_file_name[index] + "." + extension[index]
            # logger.debug(f"{file_name=}")
            complete_file_name.append(file_name)
            # logger.debug(f"{complete_file_name=}")
            # logger.debug(f"{count=}")
            # logger.debug(f"{header[index]=}")
            # logger.debug(f"{lck_file[index]=}")

            job_config.destination_format = str(extension[index])
            job_config.field_delimiter = str(delimiter[index])

            if header[index] == "N":
                job_config.print_header = False
            else:
                job_config.print_header = True

            extract_job = client.extract_table(
                source=table_id[index],
                destination_uris=absoluteDestinationUri,
                location="US",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.
            logger.info(f"Exported {table_id[index]} to {absoluteDestinationUri}")

            # logger.debug(f"{outputGCSlocation[index]=}")
            bucket_name = outputGCSlocation[index].split("/")[2]
            # logger.debug(f"{bucket_name=}")
            folder_name = outputGCSlocation[index].split("/")[3]
            # logger.debug(f"{folder_name=}")

            bucket = client1.bucket(bucket_name)

            # logger.debug(f"{trigger_file[index]=}")

            if trigger_file[index] is not None:
                blob = bucket.blob(folder_name + "/" + trigger_file[index])
                blob.upload_from_string(" ", content_type="text/plain")
                logger.info(f"trigger file {trigger_file[index]} generated")

            # logger.debug(f"{lck_file[index]=}")

            if lck_file[index] == "Y":
                time.sleep(10)  # generating lck file for sftp

                blob = bucket.blob(folder_name + "/" + output_file_name[index] + ".lck")
                blob.upload_from_string(" ", content_type="text/plain")

            result, rows = query_execution("drop table " + table_id[index] + ";")  # dropping table
            logger.debug(f"Dropped table {table_id[index]}")
            merchant.append(merchant_code[index])
            email.append(email_id[index])
            qufile_date.append(qufile_dt[index])
            batch_number.append(batch_no[index])
            file_path.append(outputGCSlocation[index])
        except Exception as e:
            logging.exception(f"Exception: {e}")
            error_count = error_count + 1
            # logging.exception(f"{error_count=}")
            event_message = "Extraction failed in task extractToGCS"
            event = Event(
                dag_name=dag_name,
                event_status="failure",
                event_message=event_message,
                start_time=exec_time_aest,
                file_name=absoluteDestinationUri,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            send_email(
                email_to=email_to,
                email_subject=event_message,
                email_body=f"Exception raised in task extractToGCS \n\nJob name - {dag_name} failed.",
                exec_time_aest=exec_time_aest,
            )

    kwargs["ti"].xcom_push(key="email", value=email)
    kwargs["ti"].xcom_push(key="qufile_date", value=qufile_date)
    kwargs["ti"].xcom_push(key="batch_number", value=batch_number)
    kwargs["ti"].xcom_push(key="error_count", value=error_count)
    kwargs["ti"].xcom_push(key="email_to", value=email_to)
    kwargs["ti"].xcom_push(key="merchant", value=merchant)
    kwargs["ti"].xcom_push(key="complete_file_name", value=complete_file_name)
    kwargs["ti"].xcom_push(key="file_path", value=file_path)
    return True


def updateControlTable(**kwargs):
    batch_number = kwargs.get("templates_dict").get("batch_number")
    qufile_date = kwargs.get("templates_dict").get("qufile_date")
    control_table = kwargs.get("templates_dict").get("control_table")
    split_etl = kwargs.get("templates_dict").get("split_etl")
    split_merchant = kwargs.get("templates_dict").get("split_merchant")
    complete_file_name = kwargs.get("templates_dict").get("complete_file_name")
    file_path = kwargs.get("templates_dict").get("file_path")
    qu_ctrltab = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["qu_ctrltab"]
    qu_ctrltab_false = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["qu_ctrltab_false"]
    email = kwargs.get("templates_dict").get("email")
    error_email = kwargs.get("templates_dict").get("email_to")
    exec_time_aest = kwargs.get("templates_dict").get("exec_time_aest")
    merchant = kwargs.get("templates_dict").get("merchant")
    error_count = int(kwargs.get("templates_dict").get("error_count"))
    email_attachment = kwargs.get("templates_dict").get("email_attachment")
    qufile_date = ast.literal_eval(qufile_date)
    merchant = ast.literal_eval(merchant)
    email = ast.literal_eval(email)
    batch_number = ast.literal_eval(batch_number)
    split_etl = ast.literal_eval(split_etl)
    split_merchant = ast.literal_eval(split_merchant)
    email_attachment = ast.literal_eval(email_attachment)
    complete_file_name = ast.literal_eval(complete_file_name)
    file_path = ast.literal_eval(file_path)

    environment = Variable.get("file_gen_bq_to_gcs", deserialize_json=True)["environment"]
    email_destination_bucket_name = gcs_bucket
    logger.debug(f"email_destination_bucket_name = {email_destination_bucket_name}")

    for index in range(0, len(merchant)):
        # logger.debug(f"{len(merchant)=}")
        # logger.debug(f"{merchant[index]=}")
        qu_ctrl = (
            qu_ctrltab.replace("ctrl", control_table)
            .replace("btc", batch_number[index])
            .replace("mrc", merchant[index])
            .replace("fldt", qufile_date[index])
        )
        # logger.debug(f"{qu_ctrl=}")
        result, rows = query_execution(qu_ctrl)

        logger.debug(f"{file_path[index]}")
        bucket_name = file_path[index].split("/")[2]
        # logger.debug(f"{bucket_name=}")

        if split_etl[index] == "Y":
            # logger.debug(f"{index=}")
            # logger.debug(f"{split_etl[index]=}")
            qu_ctrl_false = qu_ctrltab_false.replace("ctrl", control_table).replace(
                "mrc", split_merchant[index]
            )
            # logger.debug(f"{qu_ctrl_false=}")
            result, rows = query_execution(qu_ctrl_false)

        if email_attachment[index] == "Y":
            # logger.debug(f"{index=}")
            # logger.debug(f"{email_attachment[index]=}")
            # logger.debug(f"{complete_file_name[index]=}")

            # logger.debug(f"{file_path[index]=}")
            client2 = storage.Client()

            blob_name = (file_path[index].split("/")[3]) + "/" + complete_file_name[index]
            # logger.debug(f"{blob_name=}")
            destination_blob_name = (
                (file_path[index].split("/")[3])
                + "/"
                + merchant[index]
                + "_processed/"
                + complete_file_name[index]
            )
            # logger.debug(f"{destination_blob_name=}")

            destination_bucket_name = file_path[index].split("/")[2]
            # logger.debug(f"{destination_bucket_name=}")
            source_bucket = client2.bucket(bucket_name)
            source_blob = source_bucket.blob(blob_name)
            destination_bucket = client2.bucket(destination_bucket_name)
            file_name = f"gs://{destination_bucket_name}/{destination_blob_name}"
            # logger.debug(f"{file_name=}")

            blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name
            )
            # logger.info(f"Uploaded file successfully {destination_blob_name=} {blob_copy=}")

            # publish event to PubSub
            email_subject = (
                f"[{environment}] File generation for {merchant[index]} {exec_time_aest}"
            )
            event_message = email_subject
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
                file_name=file_name,
                target_name=destination_bucket_name,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            # send email with attachment
            # logger.debug(f"{email=}")
            # logger.debug(f"{email_subject=}")
            # email_destination_bucket_name = Variable.get(
            #     "file_gen_bq_to_gcs", deserialize_json=True
            # )["base_bucket"]
            su.EmailAttachments(
                sender="",
                to=email[index],
                subject=email_subject,
                message_text=f"File generation for {merchant[index]} completed successfully. Please check the attachment",
                file=complete_file_name[index],
                source_bucket=bucket_name,
                destination_bucket=email_destination_bucket_name,
            )

            source_bucket.delete_blob(blob_name)
            logger.debug(f"Source file {complete_file_name[index]} deleted successfully")

        else:
            # without email attachment
            # logger.debug(f"{email[index]=}")
            file_name = file_path[index] + complete_file_name[index]
            # logger.debug(f"{file_name=}")
            event_message = f"[{environment}] File generation for {merchant[index]}"
            event = Event(
                dag_name=dag_name,
                event_status="success",
                event_message=event_message,
                start_time=exec_time_aest,
                file_name=file_name,
                target_name=bucket_name,
            )
            publisher.publish(topic_path, data=json.dumps(asdict(event)).encode("utf-8"))
            send_email(
                email_to=email[index],
                email_subject=event_message,
                email_body=f"File generation for {merchant[index]} completed successfully.\n\nJob name {dag_name}",
                exec_time_aest=exec_time_aest,
            )

    if error_count == 0:
        return True
    if error_count >= 1:
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
        send_email(
            email_to=error_email,
            email_subject=f"{dag_name} failed",
            email_body=event_message,
            exec_time_aest=exec_time_aest,
        )
        raise AirflowException("Job failure")
        return False


getConfigDetails = ShortCircuitOperator(
    python_callable=getConfigDetails,
    task_id="getConfigDetails",
    depends_on_past=False,
    provide_context=True,
    dag=dag,
)


extractToGCS = PythonOperator(
    task_id="extractToGCS",
    dag=dag,
    python_callable=extractToGCS,
    provide_context=True,
    templates_dict={
        "batch_number": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='batch_number')}}",
        "qufile_date": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='qufile_date')}}",
        "email": "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='email')}}",
        "trigger_file": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='trigger_file') }}",
        "table_id": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='table_id') }}",
        "header": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='header') }}",
        "delimiter": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='delimiter') }}",
        "extension": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='extension') }}",
        "lck_file": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='lck_file') }}",
        "loadDate": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='loadDate') }}",
        "destination_cloud_storage_uri": "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='destination_cloud_storage_uri')}}",
        "output_file_name": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='output_file_name') }}",
        "error_count": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='error_count') }}",
        "email_to": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to') }}",
        "exec_time_aest": "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='exec_time_aest')}}",
        "merchant": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='merchant')}}",
    },
)


updateControlTable = PythonOperator(
    task_id="updateControlTable",
    dag=dag,
    python_callable=updateControlTable,
    provide_context=True,
    templates_dict={
        "batch_number": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='batch_number')}}",
        "qufile_date": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='qufile_date')}}",
        "control_table": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='control_table')}}",
        "email": "{{task_instance.xcom_pull(task_ids='extractToGCS',key='email')}}",
        "email_to": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_to')}}",
        "exec_time_aest": "{{task_instance.xcom_pull(task_ids='getConfigDetails',key='exec_time_aest')}}",
        "merchant": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='merchant')}}",
        "error_count": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='error_count')}}",
        "split_etl": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='split_etl')}}",
        "split_merchant": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='split_merchant')}}",
        "email_attachment": "{{ task_instance.xcom_pull(task_ids='getConfigDetails', key='email_attachment')}}",
        "complete_file_name": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='complete_file_name')}}",
        "file_path": "{{ task_instance.xcom_pull(task_ids='extractToGCS', key='file_path')}}",
    },
)

# pylint: disable=pointless-statement
getConfigDetails >> extractToGCS >> updateControlTable
