"""Pytest configuration and fixtures."""

import os
import pytest
import pytest_asyncio
import asyncio
import pandas as pd
from dotenv import load_dotenv
from aml_monitoring.database.postgres import PostgresHandler
from aml_monitoring.database.neo4j import Neo4jHandler

# Load test environment variables
load_dotenv('test.env')

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="function")
async def postgres_handler():
    """Create a PostgreSQL handler for testing."""
    handler = PostgresHandler()
    await handler.connect()
    await handler.create_schema()
    yield handler
    await handler.wipe_clean()
    await handler.disconnect()

@pytest_asyncio.fixture(scope="function")
async def neo4j_handler():
    """Create a Neo4j handler for testing."""
    handler = Neo4jHandler()
    await handler.connect()
    await handler.wipe_clean()  # Clean before creating schema to avoid conflicts
    await handler.create_schema()
    yield handler
    await handler.wipe_clean()
    await handler.disconnect()

@pytest.fixture
def sample_institution_data():
    """Sample institution data for testing."""
    return {
        'institution_id': '123e4567-e89b-12d3-a456-426614174000',
        'legal_name': 'Test Bank',
        'business_type': 'bank',
        'incorporation_country': 'US',
        'incorporation_date': '2020-01-01',
        'onboarding_date': '2020-02-01',
        'operational_status': 'active',
        'regulatory_status': 'licensed',
        'licenses': ['banking', 'securities'],
        'industry_codes': ['6021', '6022'],
        'public_company': True,
        'stock_symbol': 'TBNK',
        'stock_exchange': 'NYSE',
        'risk_rating': 'low'
    }

@pytest.fixture
def sample_account_data(sample_institution_data):
    """Sample account data for testing."""
    return {
        'account_id': '123e4567-e89b-12d3-a456-426614174002',
        'account_number': 'TEST123456',
        'account_type': 'checking',
        'balance': 10000.0,
        'currency': 'USD',
        'risk_rating': 'low',
        'entity_id': '123e4567-e89b-12d3-a456-426614174000',
        'entity_type': 'institution',
        'status': 'active',
        'opening_date': '2020-01-01',
        'institution_id': '123e4567-e89b-12d3-a456-426614174000'
    }

@pytest.fixture
def sample_transaction_data():
    """Sample transaction data for testing."""
    return {
        'transaction_id': '123e4567-e89b-12d3-a456-426614174001',
        'account_id': '123e4567-e89b-12d3-a456-426614174002',
        'amount': 1000.0,
        'currency': 'USD',
        'transaction_type': 'wire',
        'transaction_date': '2020-01-01',
        'transaction_status': 'completed',
        'is_debit': True,
        'entity_id': '123e4567-e89b-12d3-a456-426614174000',
        'entity_type': 'institution'
    }

@pytest.fixture
def sample_risk_assessment_data(sample_institution_data):
    """Sample risk assessment data for testing."""
    return {
        'assessment_id': '123e4567-e89b-12d3-a456-426614174004',
        'entity_id': sample_institution_data['institution_id'],
        'entity_type': 'institution',
        'assessment_date': '2020-01-01',
        'risk_rating': 'high',
        'risk_score': '0.85',
        'assessment_type': 'annual',
        'risk_factors': {'jurisdiction': 3, 'product': 2, 'client': 2}
    }
