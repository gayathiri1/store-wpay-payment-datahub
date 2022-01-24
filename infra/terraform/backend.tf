terraform {
  backend "gcs" {
    bucket  = "gcp-wow-wpay-paydat-dev"
    prefix  = "pdh-cicd/infra"
  }
}

# terraform {
#   backend "gcs" {}
# }

# data "terraform_remote_state" "state" {
#   backend = "gcs"
#   config {
#     bucket     = var.composer_project_id
#     prefix = "pdh-cicd/infra"
#   }
# }