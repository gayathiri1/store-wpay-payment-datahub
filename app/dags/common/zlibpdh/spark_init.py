#from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from zlibpdh.composer_constants import ComposerConstants as cc
import uuid


class SparkInit:

    def __init__(self,dag,task_id,project_id,bucket,subnet,pypath,args,file_name):
        self.dag = dag
        self.task_id = task_id
        self.project_id = project_id
        self.bucket = bucket
        self.subnet = subnet
        self.pypath = pypath
        self.args = args
        self.file_name = file_name

    def start_cluster(self):
        CLUSTER_CONFIG = { ## Added for Composer 2.6.3 Upgrade
                    "master_config": {
                            "num_instances": 1,
                            "machine_type_uri": cc.MASTER,
                            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": cc.MDISK},
                            },
                    "worker_config": {
                            "num_instances": cc.WORKERNUM,
                            "machine_type_uri": cc.WORKER,
                            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": cc.WDISK},
                            },
                    "gce_cluster_config": {
                            "subnetwork_uri": cc.SUBNETURI,
                            "service_account_scopes": ['https://www.googleapis.com/auth/cloud-platform'],
                            "metadata": {'PIP_PACKAGES':'google-cloud google-cloud-bigquery google-cloud-storage Jinja2'},
                            },  
                    "software_config": {
                            "image_version": cc.IMAGE,
                            },
                         }
        
        start_cluster = DataprocCreateClusterOperator(
                dag=self.dag,
                task_id=self.task_id,
                cluster_name=cc.CLUSTER,
                project_id=self.project_id,
                cluster_config=CLUSTER_CONFIG,
                region=cc.REGION,
            )

        return start_cluster

    def submit_cluster(self):
    
        PYSPARK_JOB = {
                    "reference": {"project_id": self.project_id,
                                  "job_id": self.file_name + "-" + str(uuid.uuid4())},
                    "placement": {"cluster_name": cc.CLUSTER},
                    "pyspark_job": {"main_python_file_uri": self.pypath,
                                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar"],
                                    "args": self.args},
                      }
                    
        submit_pyspark = DataprocSubmitJobOperator(
                            task_id=self.task_id, 
                            job=PYSPARK_JOB, 
                            region=cc.REGION, 
                            project_id=self.project_id
                        )
                    
        return submit_pyspark

    def stop_cluster(self):
        stop_cluster = DataprocDeleteClusterOperator(
        dag= self.dag,
        task_id= self.task_id,
        cluster_name=cc.CLUSTER,
        project_id=self.project_id,
        region=cc.REGION,
        )
        return stop_cluster