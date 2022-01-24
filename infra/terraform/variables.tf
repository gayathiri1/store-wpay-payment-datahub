#************GitHub Variables************
variable github_repo_name {
    type = string
    description = "Name of the Github Repo"
    default = "xxxx-pls-change"
}

variable github_repo_owner {
    type = string
    description = "Github Repo Owner"
    default = "xxxx-pls-change"
}

variable github_repo_branch {
    type = string
    description = "Github Repo banch which need to be monitored by trigger."
    default = "xxxx-pls-change"
}

variable pull_req_cloudbuild_yaml {
    type = string
    description = "Pull request cloud build yaml file"
    default = "xxxx-pls-change"
}

variable merge_req_cloudbuild_yaml {
    type = string
    description = "Push(After Merge of the pull request) request cloud build yaml file"
    default = "xxxx-pls-change"
}

#************App Backend Variables************
variable terraform_backend_bucket {
    type = string
    description = "terraform app backend bucket"
    default = "xxxx-pls-change"
}

variable terraform_backend_prefix {
    type = string
    description = "terraform app backend prefix"
    default = "xxxx-pls-change"
}

#************Composer Variables************
variable composer_gcs_bucket {
    type = string
    description = "composer gcs bucket name"
    default = "xxxx-pls-change"
}

variable composer_environment {
    type = string
    description = "composer name"
    default = "xxxx-pls-change"
}

variable composer_location {
    type = string
    description = "composer name"
    default = "xxxx-sydney-pls-change"
}

variable etl_bucket {
    type = string
    description = "etl bucket name"
    default = "xxxx-pls-change"
}


# variable cloudBuild_serviceAcct {
#     type = string
#     description = "CloudBuild ServiceAccount which has composer access"
#     default = "serviceAcct-pls-change"
# }

# variable composer_project_id {
#     type = string
#     description = "Project Id"
#     default = "projectId-pls-change"
# }

# variable cloudBuild_logs {
#     type = string
#     description = "Cloud Build logs bucket for using the service account"
#     default = "logsBucket-pls-change"
# }