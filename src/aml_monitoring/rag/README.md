# RAG Application on GCP

A secure Retrieval-Augmented Generation (RAG) application built on Google Cloud Platform.

## Security Features

- Secure service authentication using dedicated service accounts
- Private Cloud Run services with authenticated access only
- Secure key management using Secret Manager
- Least privilege access control

## Prerequisites

1. Google Cloud Project with billing enabled
2. GitHub repository for the code
3. Required tools:
   - Python 3.9+
   - Terraform
   - Google Cloud SDK
   - Docker

## Setup Instructions

1. **Install Prerequisites**
   ```bash
   sudo ./scripts/prerequisites.sh
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your project settings
   ```

3. **Deploy Infrastructure**
   ```bash
   ./scripts/deploy.sh
   ```

4. **Authenticate Service Calls**
   ```bash
   # Get authentication token
   TOKEN=$(gcloud auth print-identity-token \
     --impersonate-service-account=cloud-run-invoker@$PROJECT_ID.iam.gserviceaccount.com)
   
   # Call query service
   curl -X POST https://query-service-url/query \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query":"your question"}'
   ```

## Architecture

### Components

1. **Document Processor Service**
   - Processes uploaded documents
   - Generates embeddings using Vertex AI
   - Stores document chunks and embeddings

2. **Query Service**
   - Handles user queries
   - Performs semantic search
   - Returns relevant document chunks

### Security Architecture

1. **Service Accounts**
   - `rag-service-account`: Main application service account
   - `cloud-run-invoker`: Dedicated service account for API access

2. **Access Control**
   - Cloud Run services: Internal and load balancer access only
   - Authentication required for all API calls
   - Least privilege IAM roles

3. **Key Management**
   - Service account keys stored in Secret Manager
   - Automatic key rotation capability
   - Secure key handling in deployment

## Usage Examples

1. **Upload Document**
   ```bash
   # Get authentication token
   TOKEN=$(gcloud auth print-identity-token \
     --impersonate-service-account=cloud-run-invoker@$PROJECT_ID.iam.gserviceaccount.com)
   
   # Upload document
   gsutil cp your-document.pdf gs://$BUCKET_NAME/
   ```

2. **Query Documents**
   ```bash
   # Query with authentication
   curl -X POST https://query-service-url/query \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "What are the main points?",
       "top_k": 5
     }'
   ```

## Infrastructure Management

### Terraform Resources

- Cloud Run services
- Cloud SQL database
- Service accounts and IAM roles
- Secret Manager secrets
- Cloud Storage bucket
- Pub/Sub topics and subscriptions

### Deployment Process

1. **Infrastructure Setup**
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

2. **Service Deployment**
   - Automatic deployment via Cloud Build
   - Triggered by GitHub pushes
   - Builds and deploys both services

### Monitoring and Maintenance

1. **Logging**
   - Cloud Run service logs
   - Cloud Build logs
   - Service account audit logs

2. **Monitoring**
   - Cloud Run metrics
   - Query performance metrics
   - Error rates and latency

## Troubleshooting

1. **Authentication Issues**
   ```bash
   # Verify service account
   gcloud auth list
   
   # Check service account permissions
   gcloud projects get-iam-policy $PROJECT_ID
   ```

2. **Service Access Issues**
   ```bash
   # Check service URL
   terraform output query_service_url
   
   # Verify authentication token
   gcloud auth print-identity-token --impersonate-service-account=cloud-run-invoker@$PROJECT_ID.iam.gserviceaccount.com
   ```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Security Best Practices

1. **API Security**
   - Always use authentication tokens
   - Rotate service account keys regularly
   - Monitor service account usage

2. **Data Security**
   - Encrypt sensitive data
   - Use Secret Manager for credentials
   - Implement access logging

3. **Network Security**
   - Use internal-only services where possible
   - Implement VPC Service Controls
   - Configure Cloud Armor (optional)
