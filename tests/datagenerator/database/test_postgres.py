"""Test suite for PostgreSQL database handler."""

import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime
import uuid
import asyncio
from uuid import UUID

from ..database.exceptions import (
    ConnectionError, ValidationError, SchemaError, BatchError, DatabaseError
)
from ..database.postgres import PostgresHandler
from .test_config import TestConfig, TestData

@pytest.fixture
def test_config():
    """Create test configuration."""
    return TestConfig()

@pytest_asyncio.fixture
async def postgres_handler(test_config):
    """Create PostgreSQL handler instance."""
    handler = PostgresHandler(test_config.postgres_config)
    await handler.connect()
    await handler.wipe_clean()  # Start with clean slate
    yield handler
    await handler.close()

@pytest.fixture
def sample_institution_data():
    """Generate sample institution data."""
    return TestData.institution_data()

@pytest.fixture
def sample_account_data(sample_institution_data):
    """Generate sample account data."""
    return TestData.account_data(
        institution_id=uuid.UUID(sample_institution_data['institution_id'])
    )

@pytest.fixture
def sample_transaction_data(sample_account_data):
    """Generate sample transaction data."""
    return TestData.transaction_data(
        account_id=uuid.UUID(sample_account_data['account_id'])
    )

class TestPostgresHandler:
    """Test suite for PostgreSQL database handler."""
    
    @pytest.mark.asyncio
    async def test_connection(self, postgres_handler):
        """Test database connection and closing."""
        assert postgres_handler.is_connected
        await postgres_handler.close()
        assert not postgres_handler.is_connected
        await postgres_handler.connect()
        assert postgres_handler.is_connected
        
    @pytest.mark.asyncio
    async def test_schema_validation(self, postgres_handler):
        """Test schema validation."""
        # Test with valid schema
        await postgres_handler.validate_schema()
        
        # Test with invalid table
        with pytest.raises(ValidationError):
            await postgres_handler._validate_dataframe_schema(
                'invalid_table',
                pd.DataFrame()
            )
            
    @pytest.mark.asyncio
    async def test_save_institution(self, postgres_handler, sample_institution_data):
        """Test saving institution data."""
        # Create entity first
        entity_id = UUID('123e4567-e89b-12d3-a456-426614174000')
        entity_data = TestData.entity_data(entity_id)
        await postgres_handler.save_batch({'entities': pd.DataFrame([entity_data])})
        
        # Then save institution
        df = pd.DataFrame([sample_institution_data])
        data = {'institutions': df}
        await postgres_handler.save_batch(data)
        
        # Test duplicate save (should raise error due to unique constraint)
        with pytest.raises(DatabaseError):
            await postgres_handler.save_batch(data)
        
    @pytest.mark.asyncio
    async def test_save_transaction(self, postgres_handler, sample_transaction_data,
                                 sample_account_data, sample_institution_data):
        """Test saving transaction data."""
        # Save entity first
        entity_id = UUID('123e4567-e89b-12d3-a456-426614174000')
        entity_data = TestData.entity_data(entity_id)
        await postgres_handler.save_batch({'entities': pd.DataFrame([entity_data])})
        
        # Save institution
        institution_df = pd.DataFrame([sample_institution_data])
        await postgres_handler.save_batch({'institutions': institution_df})

        # Save account
        account_df = pd.DataFrame([sample_account_data])
        await postgres_handler.save_batch({'accounts': account_df})

        # Save transaction
        transaction_df = pd.DataFrame([sample_transaction_data])
        await postgres_handler.save_batch({'transactions': transaction_df})

        # Verify transaction was saved
        async with postgres_handler.pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT * FROM transactions WHERE transaction_id = $1
            """, sample_transaction_data['transaction_id'])
            assert result is not None
            assert float(result['amount']) == sample_transaction_data['amount']
            
    @pytest.mark.asyncio
    async def test_foreign_key_violation(self, postgres_handler, sample_transaction_data):
        """Test foreign key constraint violation."""
        # Try to save transaction without corresponding account
        transaction_df = pd.DataFrame([{
            **sample_transaction_data,
            'account_id': '00000000-0000-0000-0000-000000000000'  # Non-existent account
        }])
        
        with pytest.raises(DatabaseError):
            await postgres_handler.save_batch({'transactions': transaction_df})
            
    @pytest.mark.asyncio
    async def test_invalid_data_type(self, postgres_handler, sample_account_data):
        """Test invalid data type handling."""
        # Create entity first
        entity_id = UUID('123e4567-e89b-12d3-a456-426614174000')
        entity_data = TestData.entity_data(entity_id)
        await postgres_handler.save_batch({'entities': pd.DataFrame([entity_data])})
        
        data = sample_account_data.copy()
        data['balance'] = 'invalid_balance'  # Should be float
        df = pd.DataFrame([data])
        
        with pytest.raises(DatabaseError):
            await postgres_handler.save_batch({'accounts': df})
            
    @pytest.mark.asyncio
    async def test_wipe_clean(self, postgres_handler, sample_institution_data):
        """Test wiping database clean."""
        # Create entity first
        entity_id = UUID('123e4567-e89b-12d3-a456-426614174000')
        entity_data = TestData.entity_data(entity_id)
        await postgres_handler.save_batch({'entities': pd.DataFrame([entity_data])})
        
        # Save some data
        df = pd.DataFrame([sample_institution_data])
        await postgres_handler.save_batch({'institutions': df})
        
        # Wipe clean
        await postgres_handler.wipe_clean()
        
        # Verify data is gone
        async with postgres_handler.pool.acquire() as conn:
            result = await conn.fetch('SELECT * FROM institutions')
            assert len(result) == 0
            
    @pytest.mark.asyncio
    async def test_batch_size(self, postgres_handler, sample_institution_data):
        """Test batch size handling."""
        # Create multiple entities first
        entities = []
        records = []
        for i in range(5):
            entity_id = UUID(f'123e4567-e89b-12d3-a456-{426614174000 + i}')
            entities.append(TestData.entity_data(entity_id))
            
            # Create institution with matching ID
            institution = sample_institution_data.copy()
            institution['institution_id'] = str(entity_id)
            records.append(institution)
            
        # Save entities
        await postgres_handler.save_batch({'entities': pd.DataFrame(entities)})
        
        # Test saving institutions
        await postgres_handler.save_batch({'institutions': pd.DataFrame(records)})
            
if __name__ == "__main__":
    asyncio.run(test_check_schema())
