#from enum import Enum


class ComposerConstants():
    CLUSTER = 'pdh-spark-composer'
    PROJECT = 'gcp-wow-wpay-paydathub-prod'
    # SUBNET = 'gcp-wow-snet-wlxpymts-dev-dataflow' fetch it from Airflow Variable
    MASTER = 'n1-standard-4'
    WORKER = 'n1-standard-4'
    WDISK = 300
    MDISK = 300
    IMAGE = '1.4.77-debian10'
    REGION = "us-central1"
    ZONE = "us-central1-c"
    WORKERNUM = 2
    HISTORICAL_LOAD = 1
    INCREMENTAL_LOAD = 2
    init_actions_uris = 'gcp-wow-wpay-paydathub-prod'
    QUEUE = 'pdh-composer-queue'
    LOCATION = 'us-central1'
    URL = 'https://us-central1-gcp-wow-wpay-paydathub-prod.cloudfunctions.net/pdh-trigger-composer'
    OIDC_TOKEN = 'gcp-wow-wpay-paydathub-prod@appspot.gserviceaccount.com'
