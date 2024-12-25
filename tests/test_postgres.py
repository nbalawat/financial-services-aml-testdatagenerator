"""Test suite for PostgreSQL database handler."""

import pytest
import pandas as pd
from datetime import datetime
import uuid
import asyncio

from aml_monitoring.database.exceptions import (
    ConnectionError, ValidationError, SchemaError, BatchError
)
from aml_monitoring.database.postgres import PostgresHandler

class TestPostgresHandler:
    """Test suite for PostgreSQL database handler."""
    
    @pytest.mark.asyncio
    async def test_connection(self, postgres_handler):
        """Test database connection and disconnection."""
        assert postgres_handler.is_connected
        await postgres_handler.disconnect()
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
        df = pd.DataFrame([sample_institution_data])
        data = {'institutions': df}
        
        # Test successful save
        await postgres_handler.save_batch(data)
        
        # Test duplicate save (should not raise error due to ON CONFLICT DO NOTHING)
        await postgres_handler.save_batch(data)
        
    @pytest.mark.asyncio
    async def test_save_transaction(self, postgres_handler, sample_transaction_data,
                                 sample_account_data, sample_institution_data):
        """Test saving transaction data."""
        # Save institution first
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
        
        with pytest.raises(BatchError):
            await postgres_handler.save_batch({'transactions': transaction_df})
            
    @pytest.mark.asyncio
    async def test_invalid_data_type(self, postgres_handler, sample_account_data):
        """Test invalid data type handling."""
        data = sample_account_data.copy()
        data['balance'] = 'invalid_balance'  # Should be float
        df = pd.DataFrame([data])
        
        with pytest.raises(ValidationError):
            await postgres_handler.save_batch({'accounts': df})
            
    @pytest.mark.asyncio
    async def test_missing_required_field(self, postgres_handler, sample_institution_data):
        """Test handling of missing required fields."""
        data = sample_institution_data.copy()
        del data['legal_name']  # Required field
        df = pd.DataFrame([data])
        
        with pytest.raises(ValidationError):
            await postgres_handler.save_batch({'institutions': df})
            
    @pytest.mark.asyncio
    async def test_wipe_clean(self, postgres_handler, sample_institution_data):
        """Test wiping database clean."""
        # First save some data
        df = pd.DataFrame([sample_institution_data])
        await postgres_handler.save_batch({'institutions': df})
        
        # Wipe clean
        await postgres_handler.wipe_clean()
        
        # Verify data is gone but schema exists
        await postgres_handler.validate_schema()  # Should not raise error
        
    @pytest.mark.asyncio
    async def test_batch_size(self, postgres_handler, sample_transaction_data,
                            sample_account_data, sample_institution_data):
        """Test saving data in batches."""
        # Save institution first
        institution_df = pd.DataFrame([sample_institution_data])
        await postgres_handler.save_batch({'institutions': institution_df})

        # Create multiple records
        num_records = 1000
        account_data = [sample_account_data.copy() for _ in range(num_records)]
        for i, data in enumerate(account_data):
            data['account_id'] = str(uuid.uuid4())

        account_df = pd.DataFrame(account_data)
        
        # Test with different batch sizes
        await postgres_handler.save_batch({'accounts': account_df}, batch_size=100)
        await postgres_handler.save_batch({'accounts': account_df}, batch_size=500)
        await postgres_handler.save_batch({'accounts': account_df}, batch_size=1000)

@pytest.mark.asyncio
async def test_check_schema():
    """Test to inspect database schema"""
    handler = PostgresHandler()
    await handler.connect()
    
    async with handler.pool.acquire() as conn:
        # Check if table exists
        table_exists = await conn.fetch("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'institutions'
            );
        """)
        print(f"\nTable exists: {table_exists[0]['exists']}")
        
        # Get all columns and their types
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'institutions'
            ORDER BY ordinal_position;
        """)
        
        print("\nCurrent table structure:")
        for col in columns:
            print(f"Column: {col['column_name']}")
            print(f"Type: {col['data_type']}")
            print(f"Nullable: {col['is_nullable']}")
            print("---")
        
        await handler.disconnect()

if __name__ == "__main__":
    asyncio.run(test_check_schema())
