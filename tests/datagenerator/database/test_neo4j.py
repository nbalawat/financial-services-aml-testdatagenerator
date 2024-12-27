"""Tests for Neo4j database handler."""

import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime
from uuid import UUID
import asyncio

from ..exceptions import (
    ConnectionError, ValidationError, SchemaError, BatchError
)
from ..neo4j import Neo4jHandler
from ...test_config import TestConfig, TestData

class TestNeo4jHandler:
    """Test suite for Neo4j database handler."""
    
    @pytest_asyncio.fixture(scope="function")
    async def neo4j_handler(self, test_config):
        """Create Neo4j handler instance."""
        handler = Neo4jHandler(**test_config.neo4j_config['config'])
        await handler.connect()
        yield handler
        await handler.close()
    
    @pytest.mark.asyncio
    async def test_connection(self, neo4j_handler):
        """Test database connection."""
        # Test successful connection
        assert neo4j_handler.is_connected
        
        # Test disconnection
        await neo4j_handler.close()
        assert not neo4j_handler.is_connected
            
    @pytest.mark.asyncio
    async def test_save_institution(self, neo4j_handler):
        """Test saving an institution."""
        await neo4j_handler.wipe_clean()
        
        institution_data = TestData.institution_data()
        await neo4j_handler.save_batch('institutions', [institution_data])
        
        # Verify institution was saved
        async with neo4j_handler.driver.session() as session:
            result = await session.run(
                "MATCH (i:Institution) RETURN i"
            )
            nodes = [record['i'] async for record in result]
            assert len(nodes) == 1
            assert nodes[0]['institution_id'] == institution_data['institution_id']
            assert nodes[0]['legal_name'] == institution_data['legal_name']

    @pytest.mark.asyncio
    async def test_save_transaction(self, neo4j_handler):
        """Test saving a transaction."""
        await neo4j_handler.wipe_clean()
        
        # Create institution and account first
        institution_data = TestData.institution_data()
        await neo4j_handler.save_batch('institutions', [institution_data])
        
        account_data = TestData.account_data(
            institution_id=UUID(institution_data['institution_id'])
        )
        await neo4j_handler.save_batch('accounts', [account_data])
        
        # Create transaction
        transaction_data = TestData.transaction_data(
            account_id=UUID(account_data['account_id'])
        )
        await neo4j_handler.save_batch('transactions', [transaction_data])
        
        # Verify transaction was saved with relationships
        async with neo4j_handler.driver.session() as session:
            # First verify the transaction -> account relationship
            result = await session.run("""
                MATCH (t:Transaction)-[:BELONGS_TO]->(a:Account)
                WHERE t.transaction_id = $transaction_id
                RETURN t, a
            """, transaction_id=transaction_data['transaction_id'])
            records = [record async for record in result]
            assert len(records) == 1
            assert records[0]['t']['transaction_id'] == transaction_data['transaction_id']
            assert records[0]['a']['account_id'] == account_data['account_id']
            
            # Then verify the account -> institution relationship
            result = await session.run("""
                MATCH (i:Institution)-[:HAS_ACCOUNT]->(a:Account)
                WHERE a.account_id = $account_id
                RETURN a, i
            """, account_id=account_data['account_id'])
            records = [record async for record in result]
            assert len(records) == 1
            assert records[0]['a']['account_id'] == account_data['account_id']
            assert records[0]['i']['institution_id'] == institution_data['institution_id']
        
    @pytest.mark.asyncio
    async def test_relationship_creation(self, neo4j_handler):
        """Test creating relationships between nodes."""
        await neo4j_handler.wipe_clean()
        
        # Create institution and account
        institution_data = TestData.institution_data()
        await neo4j_handler.save_batch('institutions', [institution_data])
        
        account_data = TestData.account_data(
            institution_id=UUID(institution_data['institution_id'])
        )
        await neo4j_handler.save_batch('accounts', [account_data])
        
        # Create risk assessment
        risk_assessment_data = TestData.risk_assessment_data(
            entity_id=UUID(institution_data['institution_id'])
        )
        await neo4j_handler.save_batch('risk_assessments', [risk_assessment_data])
        
        # Verify relationships were created
        async with neo4j_handler.driver.session() as session:
            # Check account relationship
            result = await session.run("""
                MATCH (i:Institution)-[:HAS_ACCOUNT]->(a:Account)
                WHERE i.institution_id = $institution_id
                RETURN i, a
            """, institution_id=institution_data['institution_id'])
            records = [record async for record in result]
            assert len(records) == 1
            assert records[0]['i']['institution_id'] == institution_data['institution_id']
            assert records[0]['a']['account_id'] == account_data['account_id']
            
            # Check risk assessment relationship
            result = await session.run("""
                MATCH (i:Institution)-[:HAS_RISK_ASSESSMENT]->(r:RiskAssessment)
                WHERE i.institution_id = $institution_id
                RETURN i, r
            """, institution_id=institution_data['institution_id'])
            records = [record async for record in result]
            assert len(records) == 1
            assert records[0]['i']['institution_id'] == institution_data['institution_id']
            assert records[0]['r']['assessment_id'] == risk_assessment_data['assessment_id']

    @pytest.mark.asyncio
    async def test_invalid_data_type(self, neo4j_handler):
        """Test handling of invalid data types."""
        await neo4j_handler.wipe_clean()
        
        # Create invalid institution data with wrong data type
        invalid_data = TestData.institution_data()
        invalid_data['incorporation_date'] = 12345  # Should be string date
        
        # Should raise BatchError
        with pytest.raises(BatchError):
            await neo4j_handler.save_batch('institutions', [invalid_data])
            
    @pytest.mark.asyncio
    async def test_missing_required_property(self, neo4j_handler):
        """Test handling of missing required property."""
        await neo4j_handler.wipe_clean()
        
        # Create invalid institution data missing required field
        invalid_data = TestData.institution_data()
        del invalid_data['incorporation_date']
        
        # Should raise ValidationError
        with pytest.raises(ValidationError):
            await neo4j_handler.save_batch('institutions', [invalid_data])
    
    @pytest.mark.asyncio
    async def test_wipe_clean(self, neo4j_handler):
        """Test wiping database clean."""
        # Create test data
        institution_data = TestData.institution_data()
        await neo4j_handler.save_batch('institutions', [institution_data])
        
        # Wipe database
        await neo4j_handler.wipe_clean()
        
        # Verify database is empty
        async with neo4j_handler.driver.session() as session:
            result = await session.run("MATCH (n) RETURN count(n) as count")
            record = await result.single()
            assert record['count'] == 0
    
    @pytest.mark.asyncio
    async def test_batch_processing(self, neo4j_handler):
        """Test processing multiple records in a batch."""
        await neo4j_handler.wipe_clean()
        
        # Create multiple institutions
        institutions = [
            TestData.institution_data(UUID('123e4567-e89b-12d3-a456-426614174001')),
            TestData.institution_data(UUID('123e4567-e89b-12d3-a456-426614174002')),
            TestData.institution_data(UUID('123e4567-e89b-12d3-a456-426614174003'))
        ]
        
        await neo4j_handler.save_batch('institutions', institutions)
        
        # Verify all institutions were saved
        async with neo4j_handler.driver.session() as session:
            result = await session.run("MATCH (i:Institution) RETURN count(i) as count")
            record = await result.single()
            assert record['count'] == len(institutions)
