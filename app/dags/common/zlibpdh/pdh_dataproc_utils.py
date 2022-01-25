from google.cloud import dataproc_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import datetime
from zlibpdh.composer_constants import ComposerConstants as cc


class ListDataProcClusters:
    def __init__(self, project, region):
        self.project = project
        self.region = region

    def list_clusters(self):
        """List the details of clusters in the region."""
        if self.region == "global":
            dataproc_cluster_client = dataproc_v1.ClusterControllerClient()
        else:
            dataproc_cluster_client = dataproc_v1.ClusterControllerClient(
                client_options={"api_endpoint": f"{self.region}-dataproc.googleapis.com:443"}
            )
        for cluster in dataproc_cluster_client.list_clusters(
            request={"project_id": self.project, "region": self.region}
        ):
            return (
                (
                    "{} - {}".format(
                        cluster.cluster_name,
                        cluster.status.state.name,
                    )
                )
            )

    def list_jobs(self):
        """List the details of jobs in the cluster."""
        active_jobs_count = 0
        job_client = dataproc_v1.JobControllerClient(
            client_options={'api_endpoint': '{}-dataproc.googleapis.com:443'.format(self.region)}
        )
        for jobs in job_client.list_jobs(request={"project_id": self.project, "region": self.region,"filter":"status.state = ACTIVE"}):
            print(f'jobs => {jobs}')
            active_jobs_count += 1
        print(f'Active job count :=>{active_jobs_count}')
        return active_jobs_count

    @staticmethod
    def push_message_to_queue(payload):
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(cc.PROJECT, cc.LOCATION, cc.QUEUE)
        task = {
            'http_request': {
                'http_method': tasks_v2.HttpMethod.POST,
                'url': cc.URL,
                'oidc_token': {"service_account_email": cc.OIDC_TOKEN},
            }
        }
        task["http_request"]["headers"] = {"Content-type": "application/json"}
        task['http_request']['body'] = payload
        d = datetime.datetime.utcnow() + datetime.timedelta(seconds=120)

        # Create Timestamp protobuf.
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(d)

        # Add the timestamp to the tasks.
        task["schedule_time"] = timestamp
        response = client.create_task(parent=parent, task=task)
        print(response)

    @staticmethod
    def list_task_in_queue():
        task = None
        client = tasks_v2.CloudTasksClient()
        parent = f"projects/{cc.PROJECT}/locations/{cc.LOCATION}/queues/{cc.QUEUE}"

        response = client.list_tasks(request={"parent": parent})

        for element in response:
            print(f'Element : {element}')
            if element is not None:
                task = element
                break
            else:
                task = None
        return task
