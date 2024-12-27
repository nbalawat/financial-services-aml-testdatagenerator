#!/bin/bash

# Exit on error
set -e

# Check if running with sudo
if [ "$EUID" -ne 0 ]; then 
  echo "Please run with sudo"
  exit 1
fi

echo "Installing prerequisites for RAG application..."

# Install/Update homebrew if on MacOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Python 3.9 if not present
if ! command -v python3.9 &> /dev/null; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install python@3.9
    else
        apt-get update
        apt-get install -y python3.9 python3.9-venv
    fi
fi

# Install Terraform
if ! command -v terraform &> /dev/null; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install terraform
    else
        wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
        echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
        apt-get update && apt-get install terraform
    fi
fi

# Install Google Cloud SDK if not present
if ! command -v gcloud &> /dev/null; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install --cask google-cloud-sdk
    else
        echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
        curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
        apt-get update && apt-get install google-cloud-sdk
    fi
fi

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install --cask docker
    else
        apt-get update
        apt-get install -y docker.io
    fi
fi

# Create Python virtual environment
echo "Setting up Python virtual environment..."
python3.9 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r ../requirements.txt

# Configure gcloud
echo "Please authenticate with Google Cloud..."
gcloud auth login
gcloud auth application-default login

echo "Prerequisites installation complete!"
echo "Next steps:"
echo "1. Update terraform.tfvars with your configuration"
echo "2. Run ./deploy.sh to deploy the infrastructure"
