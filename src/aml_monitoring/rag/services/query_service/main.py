"""Query service for RAG application."""

import os
import time
from typing import List, Dict, Any
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from vertexai.language_models import TextEmbeddingModel
import numpy as np
from scipy.spatial.distance import cosine

from ...models.models import Base, Document, DocumentChunk, SearchLog

app = Flask(__name__)

# Initialize Vertex AI
embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@latest")

# Database setup
engine = create_engine(os.environ["DB_CONNECTION"])
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

@app.route("/query", methods=["POST"])
def query_documents():
    """Handle document query requests."""
    try:
        start_time = time.time()
        
        # Get query from request
        data = request.get_json()
        if not data or "query" not in data:
            return jsonify({"error": "Missing query parameter"}), 400
        
        query = data["query"]
        filters = data.get("filters", {})
        top_k = data.get("top_k", 5)
        
        # Generate embedding for query
        query_embedding = get_embedding(query)
        
        # Search for similar chunks with filters
        results = search_similar_chunks(query_embedding, filters, top_k)
        
        execution_time = time.time() - start_time
        
        # Log search
        db = SessionLocal()
        try:
            log = SearchLog(
                query=query,
                filters=filters,
                num_results=len(results),
                execution_time=execution_time
            )
            db.add(log)
            db.commit()
        finally:
            db.close()
        
        return jsonify({
            "results": results,
            "execution_time": execution_time
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/similar/<document_id>", methods=["GET"])
def get_similar_documents(document_id: str):
    """Find documents similar to a given document."""
    try:
        top_k = int(request.args.get("top_k", 5))
        
        db = SessionLocal()
        try:
            # Get document chunks
            chunks = db.query(DocumentChunk).filter(
                DocumentChunk.document_id == document_id
            ).all()
            
            if not chunks:
                return jsonify({"error": "Document not found"}), 404
            
            # Calculate average embedding for the document
            doc_embedding = np.mean([chunk.get_embedding() for chunk in chunks], axis=0)
            
            # Search for similar documents
            results = search_similar_documents(doc_embedding, document_id, top_k)
            
            return jsonify({"results": results})
            
        finally:
            db.close()
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_embedding(text: str) -> List[float]:
    """Generate embedding for query text."""
    return embedding_model.get_embeddings([text])[0].values

def search_similar_chunks(query_embedding: List[float], 
                        filters: Dict[str, Any],
                        top_k: int = 5) -> List[Dict[str, Any]]:
    """Search for similar chunks using cosine similarity."""
    db = SessionLocal()
    try:
        # Build query based on filters
        query = db.query(DocumentChunk).join(Document)
        for key, value in filters.items():
            query = query.filter(Document.metadata[key].astext == str(value))
        
        chunks = query.all()
        
        # Calculate similarities
        similarities = []
        for chunk in chunks:
            chunk_embedding = chunk.get_embedding()
            similarity = 1 - cosine(query_embedding, chunk_embedding)
            similarities.append((similarity, chunk))
        
        # Sort by similarity and get top_k results
        similarities.sort(reverse=True)
        top_results = similarities[:top_k]
        
        # Format results
        results = []
        for similarity, chunk in top_results:
            document = db.query(Document).filter(Document.id == chunk.document_id).first()
            results.append({
                "score": float(similarity),
                "content": chunk.content,
                "document_id": str(document.id),
                "metadata": document.metadata
            })
        
        return results
        
    finally:
        db.close()

def search_similar_documents(doc_embedding: List[float],
                           exclude_id: str,
                           top_k: int = 5) -> List[Dict[str, Any]]:
    """Search for similar documents using average chunk embeddings."""
    db = SessionLocal()
    try:
        # Get all documents except the source document
        documents = db.query(Document).filter(Document.id != exclude_id).all()
        
        # Calculate document similarities
        similarities = []
        for document in documents:
            chunks = db.query(DocumentChunk).filter(
                DocumentChunk.document_id == document.id
            ).all()
            
            if chunks:
                # Calculate average embedding for the document
                doc_embedding_b = np.mean([chunk.get_embedding() for chunk in chunks], axis=0)
                similarity = 1 - cosine(doc_embedding, doc_embedding_b)
                similarities.append((similarity, document))
        
        # Sort by similarity and get top_k results
        similarities.sort(reverse=True)
        top_results = similarities[:top_k]
        
        # Format results
        results = []
        for similarity, document in top_results:
            results.append({
                "document_id": str(document.id),
                "similarity_score": float(similarity),
                "metadata": document.metadata,
                "filename": document.filename
            })
        
        return results
        
    finally:
        db.close()

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
