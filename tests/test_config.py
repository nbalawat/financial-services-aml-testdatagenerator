"""Configuration module for tests."""

import os
from typing import Dict, Any
from dataclasses import dataclass
from datetime import datetime, date
from uuid import UUID

@dataclass
class TestConfig:
    """Test configuration settings."""
    
    # Database settings
    postgres_config: Dict[str, Any] = None
    neo4j_config: Dict[str, Any] = None
    
    # Test data settings
    batch_size: int = 100
    default_currency: str = "USD"
    default_country: str = "US"
    
    # Time settings
    default_date: date = date(2024, 1, 1)
    
    def __post_init__(self):
        """Initialize database configurations from environment variables."""
        # Get Postgres connection parameters
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', ''),
            'database': os.getenv('POSTGRES_DB', 'test_db')
        }
        
        # Construct Neo4j config
        host = os.getenv('NEO4J_HOST', 'localhost')
        port = os.getenv('NEO4J_PORT', '7687')
        
        self.neo4j_config = {
            'config': {
                'uri': f"bolt://{host}:{port}",
                'user': os.getenv('NEO4J_USER', 'neo4j'),
                'password': os.getenv('NEO4J_PASSWORD', '')
            }
        }

@dataclass
class TestData:
    """Test data templates."""
    
    @staticmethod
    def entity_data(entity_id: UUID = None, entity_type: str = 'institution') -> Dict[str, Any]:
        """Generate test entity data."""
        return {
            'entity_id': str(entity_id or UUID('123e4567-e89b-12d3-a456-426614174000')),
            'entity_type': entity_type,
            'parent_entity_id': None,
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00',
            'deleted_at': None
        }
    
    @staticmethod
    def institution_data(institution_id: UUID = None) -> Dict[str, Any]:
        """Generate test institution data."""
        return {
            # Required fields
            'institution_id': str(institution_id or UUID('123e4567-e89b-12d3-a456-426614174000')),
            'legal_name': 'Test Bank',
            'business_type': 'bank',
            'incorporation_country': 'US',
            'incorporation_date': '2024-01-01',
            'onboarding_date': '2024-01-01',
            'risk_rating': 'low',
            'operational_status': 'active',
            # Optional fields
            'primary_currency': 'USD',
            'regulatory_status': 'active',
            'primary_business_activity': 'banking',
            'primary_regulator': 'FED'
        }
    
    @staticmethod
    def account_data(institution_id: UUID = None, account_id: UUID = None) -> Dict[str, Any]:
        """Generate test account data."""
        return {
            # Required fields
            'account_id': str(account_id or UUID('223e4567-e89b-12d3-a456-426614174000')),
            'entity_id': str(institution_id or UUID('123e4567-e89b-12d3-a456-426614174000')),
            'entity_type': 'Institution',
            'account_type': 'checking',
            'account_number': 'TEST123',
            'currency': 'USD',
            'status': 'active',
            'opening_date': '2024-01-01',
            'balance': 10000.00,
            'risk_rating': 'low',
            # Optional fields
            'purpose': 'business',
            'custodian_bank': 'Test Bank',
            'custodian_country': 'US'
        }
    
    @staticmethod
    def transaction_data(account_id: UUID = None, transaction_id: UUID = None) -> Dict[str, Any]:
        """Generate test transaction data."""
        return {
            # Required fields
            'transaction_id': str(transaction_id or UUID('323e4567-e89b-12d3-a456-426614174000')),
            'transaction_type': 'credit',
            'transaction_date': '2024-01-01',
            'amount': 1000.00,
            'currency': 'USD',
            'transaction_status': 'completed',
            'is_debit': False,
            'account_id': str(account_id or UUID('223e4567-e89b-12d3-a456-426614174000')),
            'entity_id': str(UUID('123e4567-e89b-12d3-a456-426614174000')),
            'entity_type': 'Institution',
            # Optional fields
            'counterparty_name': 'Test Counterparty',
            'counterparty_bank': 'Test Bank',
            'purpose': 'test transaction'
        }
    
    @staticmethod
    def risk_assessment_data(entity_id: UUID = None, assessment_id: UUID = None) -> Dict[str, Any]:
        """Generate test risk assessment data."""
        return {
            # Required fields
            'assessment_id': str(assessment_id or UUID('423e4567-e89b-12d3-a456-426614174000')),
            'entity_id': str(entity_id or UUID('123e4567-e89b-12d3-a456-426614174000')),
            'entity_type': 'Institution',
            'assessment_date': '2024-01-01',
            'risk_rating': 'low',
            'risk_score': 0.3,
            'assessment_type': 'periodic',
            'risk_factors': ['jurisdiction', 'business_type'],
            # Optional fields
            'conducted_by': 'Test Assessor',
            'approved_by': 'Test Approver',
            'notes': 'Test assessment'
        }
