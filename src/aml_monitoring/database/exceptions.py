"""Custom exceptions for database operations."""

class DatabaseError(Exception):
    """Base exception for database operations."""
    pass

class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass

class ValidationError(DatabaseError):
    """Raised when data validation fails."""
    pass

class SchemaError(DatabaseError):
    """Raised when database schema is invalid or incompatible."""
    pass

class TransactionError(DatabaseError):
    """Raised when a database transaction fails."""
    pass

class BatchError(DatabaseError):
    """Raised when batch processing fails."""
    def __init__(self, message: str, failed_items: list):
        super().__init__(message)
        self.failed_items = failed_items
