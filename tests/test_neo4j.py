"""Test suite for Neo4j database handler."""

import pytest
import pandas as pd
from datetime import datetime

from aml_monitoring.database.exceptions import (
    ConnectionError, ValidationError, SchemaError, BatchError
)

class TestNeo4jHandler:
    """Test suite for Neo4j database handler."""
    
    @pytest.mark.asyncio
    async def test_connection(self, neo4j_handler):
        """Test database connection and disconnection."""
        assert neo4j_handler.is_connected
        await neo4j_handler.disconnect()
        assert not neo4j_handler.is_connected
        await neo4j_handler.connect()
        assert neo4j_handler.is_connected
        
    @pytest.mark.asyncio
    async def test_schema_validation(self, neo4j_handler):
        """Test schema validation."""
        # Test with valid schema
        await neo4j_handler.validate_schema()
        
        # Test with invalid node type
        with pytest.raises(ValidationError):
            await neo4j_handler._validate_dataframe_schema(
                'invalid_node',
                pd.DataFrame()
            )
            
    @pytest.mark.asyncio
    async def test_save_institution(self, neo4j_handler, sample_institution_data):
        """Test saving institution node."""
        df = pd.DataFrame([sample_institution_data])
        data = {'institutions': df}
        
        # Test successful save
        await neo4j_handler.save_batch(data)
        
        # Test duplicate save (should merge)
        await neo4j_handler.save_batch(data)
        
    @pytest.mark.asyncio
    async def test_save_transaction(self, neo4j_handler, sample_transaction_data,
                                  sample_account_data):
        """Test saving transaction with relationships."""
        # First save the account
        account_df = pd.DataFrame([sample_account_data])
        await neo4j_handler.save_batch({'Account': account_df})
        
        # Then save the transaction
        transaction_df = pd.DataFrame([sample_transaction_data])
        await neo4j_handler.save_batch({'Transaction': transaction_df})
        
    @pytest.mark.asyncio
    async def test_relationship_creation(self, neo4j_handler, sample_transaction_data,
                                      sample_account_data):
        """Test creation of relationships between nodes."""
        # Save account and transaction
        account_df = pd.DataFrame([sample_account_data])
        transaction_df = pd.DataFrame([sample_transaction_data])
        
        await neo4j_handler.save_batch({
            'Account': account_df,
            'Transaction': transaction_df
        })
        
        # Verify relationship exists
        async with neo4j_handler.driver.session() as session:
            result = await session.run("""
                MATCH (t:Transaction)-[r:BELONGS_TO]->(a:Account)
                WHERE t.transaction_id = $transaction_id
                AND a.account_id = $account_id
                RETURN count(r) as rel_count
            """, {
                'transaction_id': sample_transaction_data['transaction_id'],
                'account_id': sample_account_data['account_id']
            })
            record = await result.single()
            assert record['rel_count'] == 1
            
    @pytest.mark.asyncio
    async def test_invalid_data_type(self, neo4j_handler, sample_account_data):
        """Test invalid data type handling."""
        data = sample_account_data.copy()
        data['balance'] = 'invalid_balance'  # Should be float
        df = pd.DataFrame([data])
        
        with pytest.raises(BatchError):
            await neo4j_handler.save_batch({'Account': df})
            
    @pytest.mark.asyncio
    async def test_missing_required_property(self, neo4j_handler, sample_institution_data):
        """Test handling of missing required properties."""
        data = sample_institution_data.copy()
        del data['legal_name']  # Required property
        df = pd.DataFrame([data])
        
        with pytest.raises(ValidationError):
            await neo4j_handler.save_batch({'Institution': df})
            
    @pytest.mark.asyncio
    async def test_wipe_clean(self, neo4j_handler, sample_institution_data):
        """Test wiping database clean."""
        # First save some data
        df = pd.DataFrame([sample_institution_data])
        await neo4j_handler.save_batch({'Institution': df})
        
        # Wipe clean
        await neo4j_handler.wipe_clean()
        
        # Verify data is gone but constraints exist
        await neo4j_handler.validate_schema()  # Should not raise error
        
        # Verify no nodes exist
        async with neo4j_handler.driver.session() as session:
            result = await session.run("MATCH (n) RETURN count(n) as node_count")
            record = await result.single()
            assert record['node_count'] == 0
            
    @pytest.mark.asyncio
    async def test_batch_processing(self, neo4j_handler, sample_institution_data):
        """Test processing data in batches."""
        # Create multiple records
        num_records = 1000
        institution_data = [sample_institution_data.copy() for _ in range(num_records)]
        for i, data in enumerate(institution_data):
            data['institution_id'] = f'test-institution-{i}'
            
        institution_df = pd.DataFrame(institution_data)
        
        # Test with different batch sizes
        await neo4j_handler.save_batch({'Institution': institution_df}, batch_size=100)
        await neo4j_handler.save_batch({'Institution': institution_df}, batch_size=500)
        await neo4j_handler.save_batch({'Institution': institution_df}, batch_size=1000)
