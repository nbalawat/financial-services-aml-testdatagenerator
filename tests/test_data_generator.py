"""Test suite for data generation functionality."""

import pytest
import asyncio
import pytest_asyncio
import pandas as pd
from datetime import datetime, timedelta
import uuid
from typing import Dict, Any
from aml_monitoring.data_generator import (
    DataGenerator, InstitutionGenerator, AddressGenerator,
    AccountGenerator, TransactionGenerator, BeneficialOwnerGenerator,
    RiskAssessmentGenerator, AuthorizedPersonGenerator,
    DocumentGenerator, JurisdictionPresenceGenerator,
    ComplianceEventGenerator, SubsidiaryGenerator
)
from aml_monitoring.models import (
    Institution, Address, Account, Transaction,
    BusinessType, OperationalStatus, RiskRating, TransactionType,
    TransactionStatus, RiskAssessment, AuthorizedPerson, Document,
    JurisdictionPresence, ComplianceEvent, ComplianceEventType, Subsidiary
)
from aml_monitoring.database.postgres import PostgresHandler
from aml_monitoring.database.neo4j import Neo4jHandler

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Create test configuration."""
    return {
        'num_institutions': 5,
        'min_transactions_per_account': 1,
        'max_transactions_per_account': 3,
        'batch_size': 100
    }

@pytest.fixture
def data_generator(test_config):
    """Create a DataGenerator instance."""
    return DataGenerator(test_config)

@pytest_asyncio.fixture
async def data_generator():
    """Create a DataGenerator instance."""
    config = {
        'num_institutions': 5,
        'min_transactions_per_account': 1,
        'max_transactions_per_account': 3,
        'batch_size': 100
    }
    generator = DataGenerator(config)
    await generator.initialize_db()
    yield generator
    await generator.close_db()

@pytest_asyncio.fixture
async def postgres_handler():
    """Create a PostgresHandler instance."""
    handler = PostgresHandler()
    await handler.connect()
    yield handler
    await handler.disconnect()

@pytest_asyncio.fixture
async def neo4j_handler():
    """Create a Neo4jHandler instance."""
    handler = Neo4jHandler()
    await handler.connect()
    yield handler
    await handler.disconnect()

@pytest.mark.asyncio
async def test_institution_generator(test_config):
    """Test institution data generation."""
    generator = InstitutionGenerator(test_config)
    institutions = []
    
    async for institution in generator.generate():
        institutions.append(institution)
        
    assert len(institutions) == test_config['num_institutions']
    
    # Verify institution attributes
    for inst in institutions:
        assert isinstance(inst.institution_id, str)
        assert isinstance(inst.legal_name, str)
        assert inst.business_type in [bt.value for bt in BusinessType]
        assert inst.operational_status in [os.value for os in OperationalStatus]
        assert inst.risk_rating in [rr.value for rr in RiskRating]
        assert isinstance(inst.incorporation_date, str)
        assert isinstance(inst.licenses, list)

@pytest.mark.asyncio
async def test_address_generator(test_config):
    """Test address data generation."""
    generator = AddressGenerator(test_config)
    entity_id = str(uuid.uuid4())
    addresses = []
    
    async for address in generator.generate(entity_id, 'institution'):
        addresses.append(address)
        
    assert len(addresses) > 0
    
    # Verify address attributes
    for addr in addresses:
        assert addr.entity_id == entity_id
        assert isinstance(addr.address_id, str)
        assert isinstance(addr.address_line1, str)
        assert isinstance(addr.city, str)
        assert isinstance(addr.country, str)
        assert isinstance(addr.status, str)

@pytest.mark.asyncio
async def test_account_generator(test_config):
    """Test account data generation."""
    generator = AccountGenerator(test_config)
    entity_id = str(uuid.uuid4())
    accounts = []
    
    async for account in generator.generate(entity_id, 'institution'):
        accounts.append(account)
        
    assert len(accounts) > 0
    
    # Verify account attributes
    for acc in accounts:
        assert acc.entity_id == entity_id
        assert isinstance(acc.account_id, str)
        assert isinstance(acc.account_number, str)
        assert isinstance(acc.balance, float)
        assert isinstance(acc.currency, str)

@pytest.mark.asyncio
async def test_transaction_generator(test_config):
    """Test transaction data generation."""
    generator = TransactionGenerator(test_config)
    account = Account(
        account_id=str(uuid.uuid4()),
        entity_id=str(uuid.uuid4()),
        entity_type='institution',
        account_number='TEST123',
        account_type='checking',
        balance=1000.0,
        currency='USD',
        status='active',
        opening_date=datetime.now().strftime('%Y-%m-%d'),
        risk_rating='low',
        purpose='business'
    )
    transactions = []
    
    async for transaction in generator.generate(account):
        transactions.append(transaction)
        
    assert len(transactions) >= test_config['min_transactions_per_account']
    assert len(transactions) <= test_config['max_transactions_per_account']
    
    # Verify transaction attributes
    for txn in transactions:
        assert txn.account_id == account.account_id
        assert isinstance(txn.transaction_id, str)
        assert isinstance(txn.amount, float)
        assert txn.transaction_type in [tt.value for tt in TransactionType]
        assert txn.transaction_status in [ts.value for ts in TransactionStatus]
        assert isinstance(txn.transaction_date, str)

@pytest.mark.asyncio
async def test_beneficial_owner_generator(test_config):
    """Test beneficial owner data generation."""
    generator = BeneficialOwnerGenerator(test_config)
    entity_id = str(uuid.uuid4())
    owners = []
    
    async for owner in generator.generate(entity_id, 'institution'):
        owners.append(owner)
        
    assert len(owners) > 0
    
    # Verify beneficial owner attributes
    for owner in owners:
        assert owner.entity_id == entity_id
        assert isinstance(owner.owner_id, str)
        assert isinstance(owner.name, str)
        assert isinstance(owner.nationality, str)
        assert isinstance(owner.ownership_percentage, float)
        assert 0 <= owner.ownership_percentage <= 100

@pytest.mark.asyncio
async def test_data_generator_initialization(data_generator):
    """Test DataGenerator initialization."""
    assert data_generator.config['num_institutions'] == 5
    assert isinstance(data_generator.institution_gen, InstitutionGenerator)
    assert isinstance(data_generator.address_gen, AddressGenerator)
    assert isinstance(data_generator.account_gen, AccountGenerator)
    assert isinstance(data_generator.transaction_gen, TransactionGenerator)

@pytest.mark.asyncio
async def test_data_generator_dataframe_conversion(data_generator):
    """Test conversion of models to DataFrame."""
    # Create test institutions
    institutions = []
    generator = InstitutionGenerator(data_generator.config)
    async for inst in generator.generate():
        institutions.append(inst)
    
    # Convert to DataFrame
    df = data_generator._convert_to_dataframe(institutions)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == data_generator.config['num_institutions']
    assert 'institution_id' in df.columns
    assert 'legal_name' in df.columns

@pytest.mark.asyncio
async def test_data_generator_persist_batch(data_generator):
    """Test batch persistence."""
    # Debug information
    print("\nDebugging PostgresHandler:")
    print(f"Type: {type(data_generator.postgres_handler)}")
    print(f"Base classes: {type(data_generator.postgres_handler).__bases__}")
    print(f"MRO: {type(data_generator.postgres_handler).__mro__}")
    print(f"Methods: {dir(data_generator.postgres_handler)}")
    print(f"Has wipe_clean: {'wipe_clean' in dir(data_generator.postgres_handler)}")
    
    # Create test institutions
    institutions = []
    generator = InstitutionGenerator(data_generator.config)
    async for inst in generator.generate():
        institutions.append(inst)
   
    # Initialize database connections
    await data_generator.initialize_db()
    
    try:
        # Persist batch
        await data_generator.persist_batch({'institutions': institutions}, batch_size=2)
    finally:
        # Clean up
        print(f"\nCleaning up - methods available: {dir(data_generator.postgres_handler)}")
        await data_generator.postgres_handler.wipe_clean()
        await data_generator.neo4j_handler.wipe_clean()
        await data_generator.close_db()

@pytest.mark.asyncio
async def test_generate_all(data_generator):
    """Test complete data generation process."""
    try:
        # Initialize DB first
        await data_generator.initialize_db()
        
        # Clean existing data
        await data_generator.postgres_handler.wipe_clean()
        if data_generator.neo4j_handler.is_connected:
            await data_generator.neo4j_handler.wipe_clean()
        
        # Generate all data
        await data_generator.generate_all()
        
        # Reconnect to postgres if needed
        if not data_generator.postgres_handler.is_connected:
            await data_generator.postgres_handler.connect()

        # Verify data was saved
        async with data_generator.postgres_handler.pool.acquire() as conn:
            # Check institutions
            result = await conn.fetch("SELECT COUNT(*) FROM institutions")
            assert result[0]['count'] == data_generator.config['num_institutions']

            # Check accounts
            result = await conn.fetch("SELECT COUNT(*) FROM accounts")
            assert result[0]['count'] > 0

            # Check transactions
            result = await conn.fetch("SELECT COUNT(*) FROM transactions")
            assert result[0]['count'] > 0

    except Exception as e:
        print(f"Error during test: {str(e)}")
        raise

    finally:
        # Clean up
        if data_generator.postgres_handler.is_connected:
            await data_generator.postgres_handler.wipe_clean()
            await data_generator.postgres_handler.disconnect()
        if data_generator.neo4j_handler.is_connected:
            await data_generator.neo4j_handler.wipe_clean()
            await data_generator.neo4j_handler.disconnect()

@pytest.mark.asyncio
async def test_institution_generation(data_generator):
    """Test that institutions are generated with required fields."""
    data = {'institutions': []}
    async for institution in data_generator.institution_gen.generate():
        data['institutions'].append(institution)
    
    assert len(data['institutions']) == 5
    for inst in data['institutions']:
        assert inst.legal_name
        assert inst.business_type
        assert inst.incorporation_country
        assert inst.incorporation_date
        assert inst.onboarding_date
        assert inst.risk_rating
        assert inst.operational_status

@pytest.mark.asyncio
async def test_subsidiary_generation(data_generator):
    """Test that subsidiaries are generated with required fields and proper relationships."""
    data = {'subsidiaries': [], 'institutions': []}
    
    # First generate an institution
    async for institution in data_generator.institution_gen.generate():
        data['institutions'].append(institution)
        # Generate subsidiaries for this institution
        async for subsidiary in data_generator.subsidiary_gen.generate(institution.institution_id):
            data['subsidiaries'].append(subsidiary)
    
    assert len(data['subsidiaries']) > 0
    for sub in data['subsidiaries']:
        # Check required fields
        assert sub.subsidiary_id
        assert sub.parent_institution_id
        assert sub.legal_name
        assert sub.tax_id
        assert sub.incorporation_country
        assert sub.incorporation_date
        assert sub.acquisition_date
        assert sub.business_type
        assert sub.operational_status
        assert 0 < sub.parent_ownership_percentage <= 100
        assert sub.consolidation_status
        assert sub.capital_investment > 0
        assert sub.functional_currency
        assert isinstance(sub.material_subsidiary, bool)
        assert sub.risk_classification
        assert sub.regulatory_status
        assert isinstance(sub.local_licenses, list)
        assert sub.integration_status
        assert isinstance(sub.financial_metrics, dict)
        assert sub.reporting_frequency
        assert isinstance(sub.requires_local_audit, bool)
        assert sub.corporate_governance_model
        assert isinstance(sub.is_regulated, bool)
        assert isinstance(sub.is_customer, bool)
        
        # Check relationships
        assert any(inst.institution_id == sub.parent_institution_id for inst in data['institutions'])
        
        # Check date validations
        inc_date = datetime.strptime(sub.incorporation_date, '%Y-%m-%d')
        acq_date = datetime.strptime(sub.acquisition_date, '%Y-%m-%d')
        assert acq_date >= inc_date
        
        # Check customer fields if is_customer is True
        if sub.is_customer:
            assert sub.customer_id
            assert sub.customer_onboarding_date
            assert sub.customer_risk_rating
            assert sub.customer_status

@pytest.mark.asyncio
async def test_neo4j_subsidiary_relationships(data_generator, neo4j_handler):
    """Test that subsidiary relationships are correctly created in Neo4j."""
    # Generate and persist data
    await data_generator.generate_all()
    
    # Query Neo4j for subsidiary relationships
    async with neo4j_handler.driver.session() as session:
        result = await session.run("""
            MATCH (i:Institution)-[r:OWNS_SUBSIDIARY]->(s:Subsidiary)
            RETURN i.institution_id, s.subsidiary_id, r.ownership_percentage, r.acquisition_date
        """)
        relationships = await result.data()
        
        assert len(relationships) > 0
        for rel in relationships:
            assert rel['i.institution_id']
            assert rel['s.subsidiary_id']
            assert 0 < rel['r.ownership_percentage'] <= 100
            assert rel['r.acquisition_date']
            
        # Check incorporation relationships
        result = await session.run("""
            MATCH (s:Subsidiary)-[:INCORPORATED_IN]->(c:Country)
            RETURN s.subsidiary_id, c.code
        """)
        incorporations = await result.data()
        
        assert len(incorporations) > 0
        for inc in incorporations:
            assert inc['s.subsidiary_id']
            assert inc['c.code']
            
        # Check temporal relationships
        result = await session.run("""
            MATCH (s:Subsidiary)-[:INCORPORATED_ON]->(bd:BusinessDate)
            RETURN s.subsidiary_id, bd.date
        """)
        dates = await result.data()
        
        assert len(dates) > 0
        for d in dates:
            assert d['s.subsidiary_id']
            assert d['bd.date']

@pytest.mark.asyncio
async def test_data_integrity(data_generator, postgres_handler, neo4j_handler):
    """Test data integrity between PostgreSQL and Neo4j."""
    # Clean up existing data
    async with neo4j_handler.driver.session() as session:
        await session.run("MATCH (n) DETACH DELETE n")
    
    await postgres_handler.execute("TRUNCATE TABLE subsidiaries, institutions, addresses, accounts, transactions CASCADE")

    # Generate and persist data
    await data_generator.generate_all()
    
    # Check subsidiaries in PostgreSQL
    pg_result = await postgres_handler.fetch_all("SELECT subsidiary_id FROM subsidiaries")
    pg_subsidiary_ids = {str(row['subsidiary_id']) for row in pg_result}
    
    # Check subsidiaries in Neo4j
    async with neo4j_handler.driver.session() as session:
        result = await session.run("MATCH (s:Subsidiary) RETURN s.subsidiary_id")
        neo_subsidiary_ids = {record['s.subsidiary_id'] for record in await result.data()}
    
    # Verify all subsidiaries exist in both databases
    assert pg_subsidiary_ids == neo_subsidiary_ids
    
    # Check relationship consistency
    async with neo4j_handler.driver.session() as session:
        result = await session.run("""
            MATCH (i:Institution)-[r:OWNS_SUBSIDIARY]->(s:Subsidiary)
            RETURN i.institution_id, s.subsidiary_id, r.ownership_percentage
        """)
        neo_relationships = {
            (r['i.institution_id'], r['s.subsidiary_id']): r['r.ownership_percentage']
            for r in await result.data()
        }
    
    # Check corresponding relationships in PostgreSQL
    pg_result = await postgres_handler.fetch_all("""
        SELECT parent_institution_id, subsidiary_id, parent_ownership_percentage
        FROM subsidiaries
    """)
    pg_relationships = {
        (str(r['parent_institution_id']), str(r['subsidiary_id'])): float(r['parent_ownership_percentage'])
        for r in pg_result
    }
    
    # Verify relationships and ownership percentages match
    assert neo_relationships.keys() == pg_relationships.keys()
    for key in neo_relationships:
        assert abs(neo_relationships[key] - pg_relationships[key]) < 0.01  # Account for floating point differences
