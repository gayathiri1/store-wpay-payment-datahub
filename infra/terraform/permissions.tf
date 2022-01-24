
# data "google_project" "project" {
# }

# resource "google_project_iam_member" "cloudbuild-secret-access" {
#   role   = "roles/secretmanager.secretAccessor"
# #   member = "serviceAccount:${google_service_account.cloudbuild_serv_acct.email}"
#   member ="serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
#   project = var.composer_project_id
# }
