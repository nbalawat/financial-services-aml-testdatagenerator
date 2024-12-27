# Create the main service account for RAG application
resource "google_service_account" "rag_service_account" {
  account_id   = "rag-service-account"
  display_name = "RAG Application Service Account"
  description  = "Service account for RAG application components"
  project      = var.project_id
}

# Create a separate service account for invoking Cloud Run services
resource "google_service_account" "cloud_run_invoker" {
  account_id   = "cloud-run-invoker"
  display_name = "Cloud Run Invoker Service Account"
  description  = "Service account for invoking Cloud Run services"
  project      = var.project_id
}

# Create service account key
resource "google_service_account_key" "rag_sa_key" {
  service_account_id = google_service_account.rag_service_account.name
}

# Grant necessary roles to the RAG service account
resource "google_project_iam_member" "rag_service_account_roles" {
  for_each = toset([
    "roles/storage.objectViewer",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/cloudsql.client",
    "roles/aiplatform.user",
    "roles/secretmanager.secretAccessor",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/artifactregistry.reader"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.rag_service_account.email}"
}

# Grant Cloud Run invoker role to the invoker service account
resource "google_cloud_run_service_iam_member" "rag_services_invoker" {
  for_each = {
    document_processor = google_cloud_run_service.document_processor.name
    query_service      = google_cloud_run_service.query_service.name
  }
  
  location = var.region
  project  = var.project_id
  service  = each.value
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cloud_run_invoker.email}"
}

# Store service account keys in Secret Manager
resource "google_secret_manager_secret" "sa_key_secret" {
  secret_id = "rag-sa-key"
  project   = var.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "sa_key_version" {
  secret      = google_secret_manager_secret.sa_key_secret.id
  secret_data = base64decode(google_service_account_key.rag_sa_key.private_key)
}

# Store invoker service account key
resource "google_service_account_key" "invoker_sa_key" {
  service_account_id = google_service_account.cloud_run_invoker.name
}

resource "google_secret_manager_secret" "invoker_key_secret" {
  secret_id = "cloud-run-invoker-key"
  project   = var.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "invoker_key_version" {
  secret      = google_secret_manager_secret.invoker_key_secret.id
  secret_data = base64decode(google_service_account_key.invoker_sa_key.private_key)
}

# Output service account details
output "service_account_email" {
  description = "The email address of the service account"
  value       = google_service_account.rag_service_account.email
}

output "service_account_key_secret" {
  description = "The Secret Manager path to the service account key"
  value       = google_secret_manager_secret.sa_key_secret.name
  sensitive   = true
}

output "invoker_service_account_email" {
  description = "The email address of the Cloud Run invoker service account"
  value       = google_service_account.cloud_run_invoker.email
}

output "invoker_key_secret" {
  description = "The Secret Manager path to the invoker service account key"
  value       = google_secret_manager_secret.invoker_key_secret.name
  sensitive   = true
}
