variable "project_id" {
  description = "The project ID to create the bucket in"
  type        = string
}

variable "region" {
  description = "The region to create the bucket in"
  type        = string
  default     = "us-east4"
}
