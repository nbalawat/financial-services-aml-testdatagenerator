"""Custom exceptions for database operations."""

class DatabaseError(Exception):
    """Base class for database exceptions."""
    pass


class ConnectionError(DatabaseError):
    """Exception raised for database connection errors."""
    pass


class ValidationError(DatabaseError):
    """Exception raised for data validation errors."""
    pass


class SchemaError(DatabaseError):
    """Exception raised for database schema errors."""
    pass


class TransactionError(DatabaseError):
    """Raised when a database transaction fails."""
    pass


class BatchError(DatabaseError):
    """Exception raised for batch operation errors."""
    def __init__(self, message: str, failed_items: list):
        super().__init__(message)
        self.failed_items = failed_items


class DatabaseInitializationError(DatabaseError):
    """Exception raised for database initialization errors."""
    pass
