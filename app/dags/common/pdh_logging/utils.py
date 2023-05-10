from datetime import datetime
from typing import Optional
import pytz
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage


def convert_timezone(dt, tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = dt.astimezone(tz2)
    return dt


def get_current_time_str_aest() -> str:
    """
    Return current datetime in string format "%Y-%m-%d %H:%M:%S"
    at Australia/NSW timezone
    """
    time_aest = convert_timezone(datetime.now(), "UTC", "Australia/NSW")
    return time_aest.strftime("%Y-%m-%d %H:%M:%S.%f")


def strptime(datetime_string: str) -> datetime:
    return datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S.%f")


def get_blob_size(gcs_uri: str) -> Optional[int]:
    """
    Returns the file size in bytes of a Google Cloud Storage blob.
    """
    if not gcs_uri:
        return None
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI {gcs_uri}")
    try:
        bucket_name, blob_name = gcs_uri.split("gs://")[1].split("/", 1)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.reload()
        return blob.size
    except (ValueError, IndexError) as e:
        print(f"An error occurred: {e}")
        return None
    except GoogleAPIError as e:
        print(f"A Google API error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
