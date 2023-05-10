import uuid
from dataclasses import dataclass
from typing import Optional
from pdh_logging.utils import get_blob_size, get_current_time_str_aest, strptime


@dataclass
class Event:
    dag_name: str
    event_status: str
    event_message: str
    start_time: str
    end_time: Optional[str] = None
    load_time: Optional[int] = None  # duration in milliseconds
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    # TODO: how to get the row count of CSV without downloaded
    row_count: Optional[int] = None
    curation_type: str = "file_extract"
    target: str = "gcs_bucket"
    target_name: Optional[str] = None  # bucket name
    target_system: Optional[str] = None
    biz_type: str = "outbound"
    pdh_audit_uuid: Optional[str] = None

    def __post_init__(self):
        self.pdh_audit_uuid = str(uuid.uuid4())

        # extract target_system from file_name
        file_name = self.file_name
        if file_name and "_" in file_name:
            if "/" in file_name:
                file_name = file_name.split("/")[-1]
                if self.target_name is None:
                    self.target_name = file_name.split("/")[2]
            self.target_system = file_name.split("_")[0]

        self.file_size = get_blob_size(self.file_name)

        self.end_time = get_current_time_str_aest()

        # duration in milliseconds
        if self.start_time and self.end_time:
            start_time = strptime(self.start_time)
            end_time = strptime(self.end_time)
            self.load_time = int((end_time - start_time).total_seconds() * 1000)
