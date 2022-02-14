from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import mimetypes
import os
import time
from zlibpdh.pdh_utilities import PDHUtils
from google.cloud import storage


class EmailAttachments(PDHUtils):
    def __init__(self,sender, to, subject, message_text, file,source_bucket,destination_bucket):
        self.sender = sender
        self.to = to
        self.subject = subject
        self.message_text = message_text
        self.file = file
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        message = MIMEMultipart()
        message['to'] = self.to
        message['from'] = self.sender
        message['subject'] = self.subject

        msg = MIMEText(self.message_text)
        message.attach(msg)

        content_type, encoding = mimetypes.guess_type(file)

        if content_type is None or encoding is not None:
            content_type = 'application/octet-stream'
        main_type, sub_type = content_type.split('/', 1)
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(self.source_bucket)
        source_blob = source_bucket.blob('email_attachment/' + file)
        destination_bucket = storage_client.get_bucket(self.destination_bucket)
        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, 'data/' + file)
        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                new_blob.name,
                destination_bucket.name,
            )
        )
        time.sleep(5)

        if main_type == 'text':
            print(f'file name is : {file}')
            if os.path.exists('/home/airflow/gcs/data/' + file):
                fp = open('/home/airflow/gcs/data/' + file, 'rb')
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(fp.read())
                filename = os.path.basename(file)
                msg.add_header('Content-Disposition', 'attachment', filename=filename)
                encoders.encode_base64(msg)
                fp.close()
            else:
                print('File does not exists')
        else:
            print(f'file name is : {file}')
            fp = open('/home/airflow/gcs/data/' + file, 'rb')
            msg = MIMEBase(main_type, sub_type)
            msg.set_payload(fp.read())
            filename = os.path.basename(file)
            msg.add_header('Content-Disposition', 'attachment', filename=filename)
            encoders.encode_base64(msg)
            fp.close()

        #message.attach(msg)
        #super().send_email_attachment(message)
        message.attach(msg)
        res = super().send_email_attachment(message)
        new_blob.delete()
        