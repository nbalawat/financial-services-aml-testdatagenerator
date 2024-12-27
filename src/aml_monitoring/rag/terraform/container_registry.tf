# Create Artifact Registry repository
resource "google_artifact_registry_repository" "rag_images" {
  provider = google-beta
  
  location      = var.region
  repository_id = "rag-images"
  description   = "Docker repository for RAG application images"
  format        = "DOCKER"
}

# Create Cloud Build trigger for main branch
resource "google_cloudbuild_trigger" "main_trigger" {
  name        = "rag-main-trigger"
  description = "Build and deploy RAG application images"
  
  github {
    owner = var.github_owner
    name  = var.github_repo
    push {
      branch = "^main$"
    }
  }
  
  included_files = [
    "src/aml_monitoring/rag/services/**",
    "src/aml_monitoring/rag/cloudbuild.yaml"
  ]
  
  filename = "src/aml_monitoring/rag/cloudbuild.yaml"
  
  substitutions = {
    _REGION     = var.region
    _REPOSITORY = google_artifact_registry_repository.rag_images.repository_id
    _TAG        = "latest"
  }
}

# Grant Cloud Build service account access to Artifact Registry
resource "google_artifact_registry_repository_iam_member" "cloudbuild_pusher" {
  provider = google-beta
  
  location   = google_artifact_registry_repository.rag_images.location
  repository = google_artifact_registry_repository.rag_images.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Data source for project details
data "google_project" "project" {}
