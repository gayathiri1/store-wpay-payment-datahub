from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator
from zlibpdh.composer_constants import ComposerConstants as cc
import uuid


class SparkInit:

    def __init__(self,dag,task_id,project_id,bucket,subnet,pypath,args):
        self.dag = dag
        self.task_id = task_id
        self.project_id = project_id
        self.bucket = bucket
        self.subnet = subnet
        self.pypath = pypath
        self.args = args

    def start_cluster(self):
        start_cluster = DataprocClusterCreateOperator(
            dag=self.dag,
            task_id=self.task_id,
            cluster_name=cc.CLUSTER,
            project_id=self.project_id,
            num_workers=cc.WORKERNUM,
            master_machine_type=cc.MASTER,
            worker_machine_type=cc.WORKER,
            worker_disk_size=cc.WDISK,
            master_disk_size=cc.MDISK,
            image_version=cc.IMAGE,
            init_actions_uris=['gs://' + cc.init_actions_uris + '/spark-job-copy/pip-install.sh'],
            metadata={'PIP_PACKAGES':'google-cloud google-cloud-bigquery google-cloud-storage Jinja2'},
            region=cc.REGION,
            zone=cc.ZONE,
            storage_bucket=self.bucket,
            labels={'product': 'pdh-composer'},
            service_account_scopes=['https://www.googleapis.com/auth/cloud-platform'],
            subnetwork_uri=self.subnet,
            retries=1,
        )
        return start_cluster

    def submit_cluster(self):
        submit_pyspark = DataProcPySparkOperator(
            dag=self.dag,
            task_id=self.task_id,
            job_name=self.task_id + str(uuid.uuid4()),
            main=self.pypath,
            arguments=self.args,
            cluster_name=cc.CLUSTER,
            region=cc.REGION,
            # https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.23.1
            dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.1.jar'#'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar'#'gs://spark-lib/bigquery/spark-bigquery-latest.jar' 
            
        )
        return submit_pyspark

    def stop_cluster(self):
        stop_cluster = DataprocClusterDeleteOperator(
        dag= self.dag,
        task_id= self.task_id,
        cluster_name=cc.CLUSTER,
        project_id=self.project_id,
        region=cc.REGION,
        )
        return stop_cluster