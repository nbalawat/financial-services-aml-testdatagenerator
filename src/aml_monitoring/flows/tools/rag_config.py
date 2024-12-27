"""Configuration for the RAG Tool."""

from dataclasses import dataclass
from typing import Optional

@dataclass
class RAGConfig:
    """Configuration for the RAG Tool.
    
    Attributes:
        project_id: GCP project ID
        region: GCP region
        document_processor_url: URL of the document processor service
        query_service_url: URL of the query service
        bucket_name: Name of the GCS bucket for document storage
        service_account_path: Path to the service account key file
        max_chunk_size: Maximum size of document chunks
        chunk_overlap: Overlap between chunks
        top_k_default: Default number of results to return
        embedding_model: Name of the embedding model to use
        max_retries: Maximum number of retries for API calls
        timeout: Timeout for API calls in seconds
    """
    
    project_id: str
    region: str
    document_processor_url: str
    query_service_url: str
    bucket_name: str
    service_account_path: str
    max_chunk_size: int = 1000
    chunk_overlap: int = 200
    top_k_default: int = 5
    embedding_model: str = "textembedding-gecko@latest"
    max_retries: int = 3
    timeout: int = 30

    @classmethod
    def from_env(cls, env_prefix: str = "RAG_") -> "RAGConfig":
        """Create configuration from environment variables.
        
        Args:
            env_prefix: Prefix for environment variables

        Returns:
            RAGConfig: Configuration object
        """
        import os
        
        required_vars = {
            "PROJECT_ID": "project_id",
            "REGION": "region",
            "DOCUMENT_PROCESSOR_URL": "document_processor_url",
            "QUERY_SERVICE_URL": "query_service_url",
            "BUCKET_NAME": "bucket_name",
            "SERVICE_ACCOUNT_PATH": "service_account_path"
        }
        
        optional_vars = {
            "MAX_CHUNK_SIZE": ("max_chunk_size", int),
            "CHUNK_OVERLAP": ("chunk_overlap", int),
            "TOP_K_DEFAULT": ("top_k_default", int),
            "EMBEDDING_MODEL": ("embedding_model", str),
            "MAX_RETRIES": ("max_retries", int),
            "TIMEOUT": ("timeout", int)
        }
        
        config_dict = {}
        
        # Get required variables
        for env_var, attr_name in required_vars.items():
            env_key = f"{env_prefix}{env_var}"
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Required environment variable {env_key} not set")
            config_dict[attr_name] = value
        
        # Get optional variables
        for env_var, (attr_name, type_func) in optional_vars.items():
            env_key = f"{env_prefix}{env_var}"
            value = os.getenv(env_key)
            if value is not None:
                config_dict[attr_name] = type_func(value)
        
        return cls(**config_dict)
