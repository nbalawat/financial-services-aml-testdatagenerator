terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.0"
    }
  }
  backend "gcs" {
    bucket = "agentic-experiments-446019-tfstate"
    prefix = "rag-application"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "aiplatform.googleapis.com",
    "secretmanager.googleapis.com"
  ])
  
  project = var.project_id
  service = each.key
  
  disable_dependent_services = true
  disable_on_destroy        = false
}

# Grant necessary roles to service account (defined in service_accounts.tf)
resource "google_project_iam_member" "service_account_roles" {
  for_each = toset([
    "roles/storage.objectViewer",
    "roles/pubsub.publisher",
    "roles/cloudsql.client",
    "roles/aiplatform.user"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.rag_service_account.email}"
}

# Create Cloud Storage bucket
resource "google_storage_bucket" "document_bucket" {
  name     = "${var.project_id}-rag-documents"
  location = var.region
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30  # days
    }
    action {
      type = "Delete"
    }
  }
}

# Create Pub/Sub topic and subscription
resource "google_pubsub_topic" "document_uploaded" {
  name = "document-uploaded"
  
  message_retention_duration = "86600s"  # 24 hours
}

resource "google_pubsub_subscription" "document_processor" {
  name  = "document-processor-sub"
  topic = google_pubsub_topic.document_uploaded.name
  
  ack_deadline_seconds = 60
  
  expiration_policy {
    ttl = "2592000s"  # 30 days
  }
  
  retry_policy {
    minimum_backoff = "10s"
  }
}

# Create Cloud SQL instance
resource "google_sql_database_instance" "rag_db" {
  name             = "rag-db-instance"
  database_version = "POSTGRES_14"
  region           = var.region
  project          = var.project_id
  deletion_protection = false  # Set to false to allow Terraform to manage the instance lifecycle
  
  settings {
    tier = var.db_instance_tier
    
    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "all"
        value = "0.0.0.0/0"  # Note: This is not recommended for production
      }
    }
    
    backup_configuration {
      enabled = true
      point_in_time_recovery_enabled = true
    }
  }
}

resource "google_sql_database" "rag_database" {
  name     = var.db_name
  instance = google_sql_database_instance.rag_db.name
}

resource "google_sql_user" "rag_user" {
  name     = var.db_user
  instance = google_sql_database_instance.rag_db.name
  password = var.db_password
}

# Create Cloud Run services
resource "google_cloud_run_service" "document_processor" {
  name     = "document-processor"
  location = var.region
  
  template {
    spec {
      service_account_name = google_service_account.rag_service_account.email
      
      containers {
        image = var.document_processor_image
        
        env {
          name  = "DB_CONNECTION"
          value = "postgresql+pg8000://${var.db_user}:${var.db_password}@${google_sql_database_instance.rag_db.connection_name}/${var.db_name}"
        }
        
        env {
          name  = "BUCKET_NAME"
          value = google_storage_bucket.document_bucket.name
        }
        
        resources {
          limits = {
            cpu    = "1000m"
            memory = "2Gi"
          }
        }
      }
    }
  }
  
  metadata {
    annotations = {
      "run.googleapis.com/ingress" = "internal-and-cloud-load-balancing"
    }
  }
  
  traffic {
    percent         = 100
    latest_revision = true
  }
}

resource "google_cloud_run_service" "query_service" {
  name     = "query-service"
  location = var.region
  
  template {
    spec {
      service_account_name = google_service_account.rag_service_account.email
      
      containers {
        image = var.query_service_image
        
        env {
          name  = "DB_CONNECTION"
          value = "postgresql+pg8000://${var.db_user}:${var.db_password}@${google_sql_database_instance.rag_db.connection_name}/${var.db_name}"
        }
        
        resources {
          limits = {
            cpu    = "2000m"
            memory = "4Gi"
          }
        }
      }
    }
  }
  
  metadata {
    annotations = {
      "run.googleapis.com/ingress" = "internal-and-cloud-load-balancing"
    }
  }
  
  traffic {
    percent         = 100
    latest_revision = true
  }
}

# Set up Cloud Storage notification
resource "google_storage_notification" "document_notification" {
  bucket         = google_storage_bucket.document_bucket.name
  payload_format = "JSON_API_V1"
  topic         = google_pubsub_topic.document_uploaded.id
  
  event_types = ["OBJECT_FINALIZE"]
  
  depends_on = [
    google_pubsub_topic.document_uploaded,
    google_storage_bucket.document_bucket
  ]
}

# Allow public access to Cloud Run services
resource "google_cloud_run_service_iam_member" "document_processor_public" {
  location = google_cloud_run_service.document_processor.location
  project  = google_cloud_run_service.document_processor.project
  service  = google_cloud_run_service.document_processor.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_service_iam_member" "query_service_public" {
  location = google_cloud_run_service.query_service.location
  project  = google_cloud_run_service.query_service.project
  service  = google_cloud_run_service.query_service.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
