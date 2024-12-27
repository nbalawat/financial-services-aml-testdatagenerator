"""Document processor service for RAG application."""

import os
import time
import json
from typing import List, Dict, Any
from flask import Flask, request, jsonify
import functions_framework
from google.cloud import storage
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from vertexai.language_models import TextEmbeddingModel

from ...models.models import Base, Document, DocumentChunk

app = Flask(__name__)

# Initialize clients
storage_client = storage.Client()
embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@latest")

# Database setup
engine = create_engine(os.environ["DB_CONNECTION"])
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

@app.route("/process", methods=["POST"])
def process_document_api():
    """Handle direct document processing requests."""
    try:
        data = request.get_json()
        if not data or "bucket_name" not in data or "blob_name" not in data:
            return jsonify({"error": "Missing required parameters"}), 400
        
        bucket_name = data["bucket_name"]
        blob_name = data["blob_name"]
        metadata = data.get("metadata", {})
        
        # Process the document
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            return jsonify({"error": "File not found"}), 404
        
        # Download file
        temp_path = f"/tmp/{os.path.basename(blob_name)}"
        blob.download_to_filename(temp_path)
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Create document record
            document = Document(
                filename=os.path.basename(blob_name),
                storage_path=f"gs://{bucket_name}/{blob_name}",
                content_type=blob.content_type,
                metadata=metadata,
                status="processing"
            )
            db.add(document)
            db.commit()
            
            # Process document
            loader = get_document_loader(temp_path, blob.content_type)
            text = loader.load()
            chunks = chunk_document(text)
            embeddings = get_embeddings(chunks)
            
            # Store chunks and embeddings
            for chunk_text, embedding in zip(chunks, embeddings):
                chunk = DocumentChunk(
                    document_id=document.id,
                    content=chunk_text,
                    embedding=embedding
                )
                db.add(chunk)
            
            # Update document status
            document.status = "completed"
            db.commit()
            
            return jsonify({
                "document_id": document.id,
                "status": "completed",
                "num_chunks": len(chunks)
            })
            
        except Exception as e:
            db.rollback()
            raise e
        
        finally:
            db.close()
            os.remove(temp_path)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status/<document_id>", methods=["GET"])
def get_document_status(document_id: str):
    """Get document processing status."""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
        if not document:
            return jsonify({"error": "Document not found"}), 404
        
        return jsonify({
            "document_id": document.id,
            "status": document.status,
            "metadata": document.metadata,
            "created_at": document.created_at.isoformat(),
            "updated_at": document.updated_at.isoformat()
        })
    
    finally:
        db.close()

@app.route("/documents/<document_id>", methods=["DELETE"])
def delete_document(document_id: str):
    """Delete a document and its chunks."""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
        if not document:
            return jsonify({"error": "Document not found"}), 404
        
        # Delete from Cloud Storage
        try:
            storage_path = document.storage_path
            if storage_path.startswith("gs://"):
                _, bucket_name, blob_name = storage_path.split("/", 2)
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_name)
                if blob.exists():
                    blob.delete()
        except Exception as e:
            print(f"Error deleting from storage: {e}")
        
        # Delete from database
        db.query(DocumentChunk).filter(DocumentChunk.document_id == document_id).delete()
        db.delete(document)
        db.commit()
        
        return jsonify({"message": "Document deleted successfully"})
    
    finally:
        db.close()

@app.route("/documents/<document_id>/metadata", methods=["PATCH"])
def update_metadata(document_id: str):
    """Update document metadata."""
    db = SessionLocal()
    try:
        document = db.query(Document).filter(Document.id == document_id).first()
        if not document:
            return jsonify({"error": "Document not found"}), 404
        
        new_metadata = request.get_json()
        if not new_metadata:
            return jsonify({"error": "No metadata provided"}), 400
        
        # Update metadata
        document.metadata.update(new_metadata)
        db.commit()
        
        return jsonify({
            "document_id": document.id,
            "metadata": document.metadata,
            "updated_at": document.updated_at.isoformat()
        })
    
    finally:
        db.close()

@functions_framework.cloud_event
def process_document(cloud_event):
    """Cloud Function triggered by Cloud Storage event."""
    try:
        # Get file information from event
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]
        content_type = data["contentType"]
        
        # Download file to temporary location
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        temp_path = f"/tmp/{os.path.basename(file_name)}"
        blob.download_to_filename(temp_path)
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Create document record
            document = Document(
                filename=os.path.basename(file_name),
                storage_path=f"gs://{bucket_name}/{file_name}",
                content_type=content_type,
                status="processing"
            )
            db.add(document)
            db.commit()
            
            # Process document
            loader = get_document_loader(temp_path, content_type)
            text = loader.load()
            chunks = chunk_document(text)
            embeddings = get_embeddings(chunks)
            
            # Store chunks and embeddings
            for chunk_text, embedding in zip(chunks, embeddings):
                chunk = DocumentChunk(
                    document_id=document.id,
                    content=chunk_text,
                    embedding=embedding
                )
                db.add(chunk)
            
            # Update document status
            document.status = "completed"
            db.commit()
            
        except Exception as e:
            db.rollback()
            raise e
        
        finally:
            db.close()
            os.remove(temp_path)
            
    except Exception as e:
        print(f"Error processing document: {e}")
        raise

def get_document_loader(file_path: str, content_type: str):
    """Get appropriate document loader based on content type."""
    if content_type == 'application/pdf':
        return PyPDFLoader(file_path)
    elif content_type in ['application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'application/msword']:
        return Docx2txtLoader(file_path)
    else:
        return TextLoader(file_path)

def chunk_document(text: str) -> List[str]:
    """Split document into chunks."""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=["\n\n", "\n", " ", ""]
    )
    return text_splitter.split_text(text)

def get_embeddings(texts: List[str]) -> List[List[float]]:
    """Generate embeddings using Vertex AI."""
    return [embedding.values for embedding in embedding_model.get_embeddings(texts)]

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
