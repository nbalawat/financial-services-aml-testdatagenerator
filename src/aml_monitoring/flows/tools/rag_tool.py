"""RAG Tool for AML Transaction Monitoring Agents.

This tool enables agents to leverage the RAG application for document search and querying.
"""

import os
import json
import requests
from typing import Dict, List, Optional, Any
from google.cloud import storage
from google.oauth2 import service_account
from google.auth.transport.requests import Request

class RAGTool:
    """A tool for interacting with the RAG application."""

    def __init__(self, project_id: str, region: str):
        """Initialize the RAG tool.

        Args:
            project_id: GCP project ID
            region: GCP region
        """
        self.project_id = project_id
        self.region = region
        self.document_processor_url = os.getenv("DOCUMENT_PROCESSOR_URL")
        self.query_service_url = os.getenv("QUERY_SERVICE_URL")
        self.bucket_name = os.getenv("BUCKET_NAME")
        
        # Initialize GCP clients
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
        # Get service account credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def _get_auth_token(self) -> str:
        """Get authentication token for Cloud Run services.

        Returns:
            str: Authentication token
        """
        if not self.credentials.valid:
            self.credentials.refresh(Request())
        return self.credentials.token

    def upload_document(self, file_path: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Upload a document to the RAG system.

        Args:
            file_path: Path to the document file
            metadata: Optional metadata about the document

        Returns:
            Dict[str, Any]: Response from the document processor
        """
        # Upload file to Cloud Storage
        blob_name = os.path.basename(file_path)
        blob = self.bucket.blob(f"documents/{blob_name}")
        blob.upload_from_filename(file_path)

        # Trigger document processing
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        data = {
            "bucket_name": self.bucket_name,
            "blob_name": f"documents/{blob_name}",
            "metadata": metadata or {}
        }
        
        response = requests.post(
            f"{self.document_processor_url}/process",
            headers=headers,
            json=data
        )
        response.raise_for_status()
        
        return response.json()

    def query(self, 
             query: str, 
             filters: Optional[Dict[str, Any]] = None, 
             top_k: int = 5) -> List[Dict[str, Any]]:
        """Query the RAG system.

        Args:
            query: The query string
            filters: Optional filters for the query
            top_k: Number of results to return

        Returns:
            List[Dict[str, Any]]: List of relevant document chunks with metadata
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        data = {
            "query": query,
            "filters": filters or {},
            "top_k": top_k
        }
        
        response = requests.post(
            f"{self.query_service_url}/query",
            headers=headers,
            json=data
        )
        response.raise_for_status()
        
        return response.json()["results"]

    def get_document_status(self, document_id: str) -> Dict[str, Any]:
        """Get the processing status of a document.

        Args:
            document_id: ID of the document

        Returns:
            Dict[str, Any]: Document status and metadata
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            f"{self.document_processor_url}/status/{document_id}",
            headers=headers
        )
        response.raise_for_status()
        
        return response.json()

    def delete_document(self, document_id: str) -> Dict[str, str]:
        """Delete a document and its chunks from the system.

        Args:
            document_id: ID of the document to delete

        Returns:
            Dict[str, str]: Status message
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        response = requests.delete(
            f"{self.document_processor_url}/documents/{document_id}",
            headers=headers
        )
        response.raise_for_status()
        
        return response.json()

    def update_metadata(self, document_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update document metadata.

        Args:
            document_id: ID of the document
            metadata: New metadata to update

        Returns:
            Dict[str, Any]: Updated document metadata
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        response = requests.patch(
            f"{self.document_processor_url}/documents/{document_id}/metadata",
            headers=headers,
            json=metadata
        )
        response.raise_for_status()
        
        return response.json()

    def get_similar_documents(self, document_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Find documents similar to a given document.

        Args:
            document_id: ID of the reference document
            top_k: Number of similar documents to return

        Returns:
            List[Dict[str, Any]]: List of similar documents with similarity scores
        """
        headers = {
            "Authorization": f"Bearer {self._get_auth_token()}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            f"{self.query_service_url}/similar/{document_id}",
            headers=headers,
            params={"top_k": top_k}
        )
        response.raise_for_status()
        
        return response.json()["results"]
