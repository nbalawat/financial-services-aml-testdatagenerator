"""Base database handler class."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import logging
from .exceptions import DatabaseError, ValidationError

logger = logging.getLogger(__name__)

class DatabaseHandler(ABC):
    """Abstract base class for database handlers."""
    
    def __init__(self):
        """Initialize database handler."""
        self.is_connected = False
        self._setup_logging()
        
    def _setup_logging(self):
        """Set up logging for the database handler."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    async def validate_schema(self) -> bool:
        """Validate database schema against models."""
        pass
    
    @abstractmethod
    async def create_schema(self) -> None:
        """Create database schema."""
        pass
    
    @abstractmethod
    async def save_batch(self, data: Dict[str, pd.DataFrame], batch_size: int = 1000) -> None:
        """Save a batch of data to the database."""
        pass
    
    async def validate_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """Validate data before saving."""
        if not isinstance(data, dict):
            raise ValidationError("Data must be a dictionary")

        # Check that all provided tables are valid
        valid_tables = {'institutions', 'accounts', 'transactions',
                       'beneficial_owners', 'addresses', 'risk_assessments',
                       'authorized_persons', 'documents', 'jurisdiction_presences',
                       'compliance_events', 'subsidiaries'}

        invalid_tables = set(data.keys()) - valid_tables
        if invalid_tables:
            raise ValidationError(f"Invalid table names: {invalid_tables}")

        # Check that each table has required fields
        for table_name, df in data.items():
            if not isinstance(df, pd.DataFrame):
                raise ValidationError(f"Data for table {table_name} must be a DataFrame")

            # Get required fields for each table
            required_fields = self.get_required_fields(table_name)
            missing_fields = required_fields - set(df.columns)
            if missing_fields:
                raise ValidationError(f"Missing required fields in {table_name}: {missing_fields}")

    def get_required_fields(self, table_name: str) -> set:
        """Get required fields for a table."""
        required_fields = {
            'institutions': {'institution_id', 'legal_name', 'business_type',
                           'incorporation_country', 'incorporation_date',
                           'risk_rating', 'operational_status'},
            'accounts': {'account_id', 'account_number', 'account_type',
                        'institution_id', 'balance', 'currency',
                        'opening_date', 'status', 'risk_rating'},
            'transactions': {'transaction_id', 'account_id', 'transaction_type',
                           'amount', 'currency', 'status', 'timestamp',
                           'entity_id'},
            'beneficial_owners': {'owner_id', 'institution_id', 'first_name',
                                'last_name', 'ownership_percentage', 'nationality',
                                'risk_rating'},
            'addresses': {'address_id', 'entity_id', 'address_type', 'country',
                         'city', 'postal_code', 'address_line1'},
            'risk_assessments': {'assessment_id', 'entity_id', 'assessment_date',
                               'risk_rating', 'assessment_type'},
            'authorized_persons': {'person_id', 'institution_id', 'first_name',
                                 'last_name', 'role', 'nationality'},
            'documents': {'document_id', 'entity_id', 'document_type',
                         'issue_date', 'expiry_date', 'issuing_country'},
            'jurisdiction_presences': {'presence_id', 'institution_id', 'country',
                                     'presence_type', 'registration_number'},
            'compliance_events': {'event_id', 'entity_id', 'event_type',
                                'event_date', 'severity', 'status'},
            'subsidiaries': {'subsidiary_id', 'parent_institution_id', 'legal_name',
                           'tax_id', 'incorporation_country', 'incorporation_date',
                           'business_type', 'operational_status'}
        }
        return required_fields.get(table_name, set())

    def _log_operation(self, operation: str, details: Optional[Dict] = None):
        """Log database operations - disabled for cleaner output."""
        pass
    
        
    @abstractmethod
    async def healthcheck(self) -> bool:
        """Check database health."""
        pass

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize database connection and create tables if they don't exist."""
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def wipe_clean(self):
        """Clean all data from the graph database."""
        async with self.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
