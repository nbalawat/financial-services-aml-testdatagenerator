output "bucket_name" {
  description = "The name of the created Cloud Storage bucket"
  value       = google_storage_bucket.document_bucket.name
}

output "pubsub_topic" {
  description = "The name of the Pub/Sub topic"
  value       = google_pubsub_topic.document_uploaded.name
}

output "pubsub_subscription" {
  description = "The name of the Pub/Sub subscription"
  value       = google_pubsub_subscription.document_processor.name
}

output "database_connection" {
  description = "The Cloud SQL database connection name"
  value       = google_sql_database_instance.rag_db.connection_name
  sensitive   = true
}

output "document_processor_url" {
  description = "The URL of the document processor service"
  value       = google_cloud_run_service.document_processor.status[0].url
}

output "query_service_url" {
  description = "The URL of the query service"
  value       = google_cloud_run_service.query_service.status[0].url
}

# Service account email is defined in service_accounts.tf
