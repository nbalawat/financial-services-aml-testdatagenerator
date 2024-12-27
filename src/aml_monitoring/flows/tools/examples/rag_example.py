"""Example usage of RAG tools for AML monitoring."""

import os
from typing import Dict, Any
from ..rag_tool import RAGTool
from ..rag_config import RAGConfig

def main():
    """Demonstrate RAG tools usage."""
    # Initialize RAG configuration
    rag_config = RAGConfig(
        project_id=os.environ.get("RAG_PROJECT_ID"),
        region=os.environ.get("RAG_REGION"),
        document_processor_url=os.environ.get("RAG_DOCUMENT_PROCESSOR_URL"),
        query_service_url=os.environ.get("RAG_QUERY_SERVICE_URL"),
        bucket_name=os.environ.get("RAG_BUCKET_NAME"),
        service_account_path=os.environ.get("RAG_SERVICE_ACCOUNT_PATH")
    )
    
    # Create RAG tool instance
    rag_tool = RAGTool(rag_config)
    
    # Example 1: Upload document
    print("\n=== Example 1: Upload document ===")
    document_path = "path/to/aml_policy.pdf"
    metadata = {
        "document_type": "policy",
        "department": "compliance",
        "version": "1.0"
    }
    document_id = rag_tool.upload_document(document_path, metadata)
    print(f"Uploaded document: {document_id}")
    
    # Example 2: Check document status
    print("\n=== Example 2: Check document status ===")
    status = rag_tool.get_document_status(document_id)
    print(f"Document status: {status}")
    
    # Example 3: Query documents
    print("\n=== Example 3: Query documents ===")
    query = "What are the requirements for high-risk customer due diligence?"
    filters = {
        "document_type": "policy",
        "department": "compliance"
    }
    results = rag_tool.query(query, filters=filters)
    print("\nQuery results:")
    for result in results:
        print(f"\nScore: {result['score']}")
        print(f"Content: {result['content']}")
        print(f"Document ID: {result['document_id']}")
        print(f"Metadata: {result['metadata']}")
    
    # Example 4: Find similar documents
    print("\n=== Example 4: Find similar documents ===")
    similar_docs = rag_tool.find_similar(document_id, top_k=3)
    print("\nSimilar documents:")
    for doc in similar_docs:
        print(f"\nDocument ID: {doc['document_id']}")
        print(f"Similarity Score: {doc['similarity_score']}")
        print(f"Filename: {doc['filename']}")
        print(f"Metadata: {doc['metadata']}")
    
    # Example 5: Update document metadata
    print("\n=== Example 5: Update document metadata ===")
    new_metadata = {
        "version": "1.1",
        "last_reviewed": "2024-12-27"
    }
    updated_doc = rag_tool.update_metadata(document_id, new_metadata)
    print(f"Updated metadata: {updated_doc}")
    
    # Example 6: Error handling
    print("\n=== Example 6: Error handling ===")
    try:
        # Try to get a non-existent document
        rag_tool.get_document_status("nonexistent_id")
    except Exception as e:
        print(f"Caught expected error: {e}")
    
    # Example 7: Cleanup
    print("\n=== Example 7: Cleanup ===")
    rag_tool.delete_document(document_id)
    print(f"Deleted document: {document_id}")

if __name__ == "__main__":
    main()
