#!/bin/bash

# Exit on error
set -e

# Set project and region
echo "Configuring Google Cloud project..."
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# Setup GitHub connection if not exists
echo "Checking GitHub connection..."
if ! gcloud beta builds connections list --region=$REGION | grep -q "github-connection"; then
    echo "Setting up GitHub connection..."
    gcloud beta builds connections create github github-connection \
        --region=$REGION \
        --host-uri=github.com
    
    echo "Please complete these steps:"
    echo "1. Follow the URL above to install the Cloud Build GitHub app"
    echo "2. Grant access to your repository"
    echo "3. Press Enter when done"
    read -p "Press Enter to continue..."
    
    # Get and store GitHub installation ID
    echo "Fetching GitHub installation ID..."
    INSTALLATION_ID=$(gcloud beta builds connections list-repositories github-connection \
        --region=$REGION \
        --format="get(installationId)" \
        --filter="repository_name=$GITHUB_REPO")
    
    if [ -z "$INSTALLATION_ID" ]; then
        echo "Error: Could not fetch GitHub installation ID"
        exit 1
    fi
    
    # Update .env file with installation ID
    sed -i '' "s/GITHUB_INSTALLATION_ID=.*/GITHUB_INSTALLATION_ID=$INSTALLATION_ID/" ../.env
    
    echo "GitHub connection established successfully!"
fi

# Initialize Terraform
echo "Initializing Terraform..."
cd ../terraform
terraform init

# Validate Terraform configuration
echo "Validating Terraform configuration..."
terraform validate

# Plan Terraform changes
echo "Planning Terraform changes..."
terraform plan -out=tfplan

# Apply Terraform changes
echo "Applying Terraform changes..."
terraform apply tfplan

# Get service account keys from Secret Manager and set them up
echo "Setting up service account authentication..."
SA_KEY_SECRET=$(terraform output -raw service_account_key_secret)
INVOKER_KEY_SECRET=$(terraform output -raw invoker_key_secret)

# Set up main service account credentials
gcloud secrets versions access latest --secret=$SA_KEY_SECRET > /tmp/sa-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa-key.json

# Set up invoker service account credentials
gcloud secrets versions access latest --secret=$INVOKER_KEY_SECRET > /tmp/invoker-key.json

# Get authentication token for Cloud Run invocation
INVOKER_TOKEN=$(gcloud auth activate-service-account --key-file=/tmp/invoker-key.json --quiet && \
                gcloud auth print-identity-token)

# Get the deployed service URLs
echo "Deployment complete! Service URLs:"
echo "Document Processor: $(terraform output -raw document_processor_url)"
echo "Query Service: $(terraform output -raw query_service_url)"

echo "Deployment successful!"
echo "Next steps:"
echo "1. Push your code to GitHub repository: git push origin main"
echo "2. Cloud Build will automatically build and deploy your services"
echo "3. Upload a document to test: gsutil cp your-document.pdf gs://$(terraform output -raw bucket_name)/"
echo "4. Query the service (with authentication):"
echo "   curl -X POST $(terraform output -raw query_service_url)/query \\"
echo "        -H \"Authorization: Bearer $INVOKER_TOKEN\" \\"
echo "        -H \"Content-Type: application/json\" \\"
echo "        -d '{\"query\":\"your question\"}'"

# Display Cloud Build trigger URL
echo "View your Cloud Build trigger at:"
echo "https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID"

# Clean up temporary files
rm -f /tmp/sa-key.json /tmp/invoker-key.json
