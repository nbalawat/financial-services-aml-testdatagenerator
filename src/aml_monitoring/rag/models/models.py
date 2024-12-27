"""Database models for RAG application."""

from datetime import datetime
from typing import List
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import numpy as np

Base = declarative_base()

class Document(Base):
    """Document metadata and storage information."""
    
    __tablename__ = 'documents'
    
    id = Column(Integer, primary_key=True)
    filename = Column(String(255), nullable=False)
    storage_path = Column(String(512), nullable=False)
    content_type = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    metadata = Column(JSON)
    
    # Relationships
    chunks = relationship("DocumentChunk", back_populates="document")

class DocumentChunk(Base):
    """Document chunks with embeddings."""
    
    __tablename__ = 'document_chunks'
    
    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents.id'))
    chunk_index = Column(Integer)
    content = Column(Text, nullable=False)
    embedding = Column(String, nullable=False)  # Store as base64 encoded string
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    document = relationship("Document", back_populates="chunks")
    
    def set_embedding(self, embedding: List[float]):
        """Convert numpy array to base64 string for storage."""
        self.embedding = np.array(embedding).tobytes().hex()
    
    def get_embedding(self) -> np.ndarray:
        """Convert stored base64 string back to numpy array."""
        return np.frombuffer(bytes.fromhex(self.embedding), dtype=np.float32)

class SearchLog(Base):
    """Log of search queries and results."""
    
    __tablename__ = 'search_logs'
    
    id = Column(Integer, primary_key=True)
    query = Column(String(512), nullable=False)
    query_embedding = Column(String)  # Store as base64 encoded string
    results = Column(JSON)  # Store search results
    created_at = Column(DateTime, default=datetime.utcnow)
    execution_time = Column(Float)  # Time taken to execute query in seconds
