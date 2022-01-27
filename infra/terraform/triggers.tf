resource "google_cloudbuild_trigger" "cicd-pr-trigger" {
    name = "pdh-cicd-pr-trigger"
    github {
        owner = var.github_repo_owner
        name   = var.github_repo_name
        pull_request {
            branch          = var.github_repo_branch
        }
    }
    # ignored_files = [
    #     "README.md",
    #     "data/**"
    # ]
    ignored_files = [
        "README.md",
    ]

    substitutions = {
        _GCS_BUCKET = var.composer_gcs_bucket
        _COMPOSER_NAME = var.composer_environment
        _COMPOSER_LOC = var.composer_location
        _TF_BACKEND_BUCKET = var.terraform_backend_bucket
        _TF_BACKEND_PREFIX = var.terraform_backend_prefix
        _ETL_BUCKET = var.etl_bucket
        _ENV_NAME = var.environment
        # _LOGS_BUCKET = var.cloudBuild_logs
    }
    # service_account = "projects/${var.composer_project_id}/serviceAccounts/${var.cloudBuild_serviceAcct}"
    # service_account = google_service_account.cloudbuild_serv_acct.name
    filename = var.pull_req_cloudbuild_yaml
    tags = [ 
        "env = ${var.environment}"
     ]
}

resource "google_cloudbuild_trigger" "cicd-mr-trigger" {
    name = "pdh-cicd-mr-trigger"
    github {
        owner = var.github_repo_owner
        name   = var.github_repo_name
        push {
            branch          = var.github_repo_branch
        }
    }
    # ignored_files = [
    #     "README.md",
    #     "data/**"
    # ]
    ignored_files = [
        "README.md",
    ]

    substitutions = {
        _GCS_BUCKET = var.composer_gcs_bucket
        _COMPOSER_NAME = var.composer_environment
        _COMPOSER_LOC = var.composer_location
        # _LOGS_BUCKET = var.cloudBuild_logs
        _ETL_BUCKET = var.etl_bucket
        _ENV_NAME = var.environment
    }
    # service_account = "projects/${var.composer_project_id}/serviceAccounts/${var.cloudBuild_serviceAcct}"
    # service_account = google_service_account.cloudbuild_serv_acct.name
    filename = var.merge_req_cloudbuild_yaml
    tags = [ 
        "env = ${var.environment}"
     ]
}