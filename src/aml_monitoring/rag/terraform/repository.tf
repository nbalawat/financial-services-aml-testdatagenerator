# Connect GitHub repository to Cloud Build
resource "google_cloudbuild_trigger" "rag_trigger" {
  name        = "rag-build-trigger"
  description = "Trigger for RAG tool builds"
  
  # GitHub repository configuration
  github {
    owner = var.github_owner
    name  = var.github_repo
    
    push {
      branch = "^main$"  # Trigger on main branch pushes
    }
  }
  
  # Build configuration
  source_to_build {
    uri       = "https://github.com/${var.github_owner}/${var.github_repo}"
    ref       = "refs/heads/main"
    repo_type = "GITHUB"
  }
  
  # Include only relevant paths
  included_files = [
    "src/aml_monitoring/rag/**",
  ]
  
  # Build configuration file
  filename = "src/aml_monitoring/rag/cloudbuild.yaml"
  
  # Substitutions for build configuration
  substitutions = {
    _REGION          = var.region
    _REPOSITORY      = google_artifact_registry_repository.rag_images.repository_id
    _TAG             = "latest"
    _PROJECT_ID      = var.project_id
    _TRIGGER_NAME    = "rag-build-trigger"
  }
}

# GitHub connection configuration
resource "google_cloudbuild_trigger_config" "github_config" {
  name     = "github-connection"
  location = var.region
  
  repository_event_config {
    repository = "projects/${var.project_id}/locations/${var.region}/connections/github-connection"
    
    push {
      branch = "^main$"
    }
  }
}

# IAM configuration for Cloud Build
resource "google_project_iam_member" "cloudbuild_roles" {
  for_each = toset([
    "roles/cloudbuild.builds.builder",
    "roles/artifactregistry.writer",
    "roles/run.admin",
    "roles/iam.serviceAccountUser"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Add variables for repository configuration
variable "github_connection_id" {
  description = "The ID of the GitHub connection in Cloud Build"
  type        = string
}

variable "github_installation_id" {
  description = "The installation ID of the GitHub App"
  type        = string
}

# Output the trigger URL
output "build_trigger_url" {
  description = "URL to the Cloud Build trigger"
  value       = "https://console.cloud.google.com/cloud-build/triggers/edit/${google_cloudbuild_trigger.rag_trigger.id}?project=${var.project_id}"
}
