"""Pytest configuration and fixtures."""

import os
import pytest
import pytest_asyncio
import asyncio
import pandas as pd
from dotenv import load_dotenv
from aml_monitoring.datagenerator.database.postgres import PostgresHandler
from aml_monitoring.datagenerator.database.neo4j import Neo4jHandler
from .test_config import TestConfig, TestData

# Load test environment variables
load_dotenv('test.env')

# Configure pytest-asyncio
def pytest_configure(config):
    """Configure pytest-asyncio defaults."""
    config.option.asyncio_mode = "strict"
    config.option.asyncio_default_fixture_loop_scope = "function"

@pytest.fixture(scope="session")
def test_config():
    """Create test configuration."""
    return TestConfig()

@pytest_asyncio.fixture(scope="function")
async def postgres_handler(test_config):
    """Create a PostgreSQL handler for testing."""
    handler = PostgresHandler(**test_config.postgres_config)
    await handler.connect()
    await handler.create_schema()
    yield handler
    await handler.wipe_clean()
    await handler.close()

@pytest.fixture
def sample_institution_data():
    """Sample institution data for testing."""
    return TestData.institution_data()

@pytest.fixture
def sample_account_data(sample_institution_data):
    """Sample account data for testing."""
    return TestData.account_data(
        institution_id=sample_institution_data['institution_id']
    )

@pytest.fixture
def sample_transaction_data(sample_account_data):
    """Sample transaction data for testing."""
    return TestData.transaction_data(
        account_id=sample_account_data['account_id']
    )

@pytest.fixture
def sample_risk_assessment_data(sample_institution_data):
    """Sample risk assessment data for testing."""
    return TestData.risk_assessment_data(
        entity_id=sample_institution_data['institution_id']
    )

@pytest.fixture
def sample_batch_data(sample_institution_data, sample_account_data,
                     sample_transaction_data, sample_risk_assessment_data):
    """Sample batch of data for testing."""
    return {
        'institutions': pd.DataFrame([sample_institution_data]),
        'accounts': pd.DataFrame([sample_account_data]),
        'transactions': pd.DataFrame([sample_transaction_data]),
        'risk_assessments': pd.DataFrame([sample_risk_assessment_data])
    }
