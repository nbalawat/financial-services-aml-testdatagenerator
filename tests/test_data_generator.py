"""Test suite for data generation functionality."""

import pytest
import asyncio
import pytest_asyncio
import pandas as pd
from datetime import datetime, timedelta
import uuid
from typing import Dict, Any

from aml_monitoring.data_generator import DataGenerator
from aml_monitoring.generators import (
    InstitutionGenerator, AddressGenerator, AccountGenerator,
    TransactionGenerator, BeneficialOwnerGenerator, RiskAssessmentGenerator,
    AuthorizedPersonGenerator, DocumentGenerator, JurisdictionPresenceGenerator,
    ComplianceEventGenerator, SubsidiaryGenerator
)
from aml_monitoring.models import (
    Institution, Address, Account, Transaction,
    BusinessType, OperationalStatus, RiskRating, TransactionType,
    TransactionStatus, RiskAssessment, AuthorizedPerson, Document,
    JurisdictionPresence, ComplianceEvent, ComplianceEventType, Subsidiary, Entity
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
        'batch_size': 100,
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'user': 'aml_user',
            'password': 'aml_password',
            'database': 'aml_monitoring_test'
        },
        'neo4j': {
            'uri': 'bolt://localhost:7687',
            'user': 'neo4j',
            'password': 'aml_password'
        }
    }

@pytest_asyncio.fixture
async def postgres_handler(test_config):
    """Create a PostgresHandler instance."""
    handler = PostgresHandler(test_config['postgres'])
    await handler.initialize()
    yield handler
    await handler.close()

@pytest_asyncio.fixture
async def neo4j_handler(test_config):
    """Create a Neo4jHandler instance."""
    handler = Neo4jHandler(
        uri=test_config['neo4j']['uri'],
        user=test_config['neo4j']['user'],
        password=test_config['neo4j']['password']
    )
    await handler.initialize()
    yield handler
    await handler.close()

@pytest_asyncio.fixture
async def data_generator(test_config, postgres_handler, neo4j_handler):
    """Create a DataGenerator instance."""
    generator = DataGenerator(test_config, postgres_handler, neo4j_handler)
    await generator.initialize_db()
    yield generator
    await generator.close_db()

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
    
    # Initialize database connections
    await data_generator.initialize_db()
    
    try:
        # First create test entities
        entities = []
        institutions = []
        generator = InstitutionGenerator(data_generator.config)
        async for inst in generator.generate():
            # Create entity for institution
            entity = Entity(
                entity_id=inst.institution_id,
                entity_type='institution',
                parent_entity_id=None,
                created_at=datetime.now().replace(microsecond=0),
                updated_at=datetime.now().replace(microsecond=0),
                deleted_at=None
            )
            entities.append(entity)
            institutions.append(inst)
        
        # Persist entities first
        await data_generator.persist_batch({'entities': entities}, batch_size=2)
        
        # Then persist institutions
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
            await data_generator.postgres_handler.close()
        if data_generator.neo4j_handler.is_connected:
            await data_generator.neo4j_handler.wipe_clean()
            await data_generator.neo4j_handler.close()

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
        assert any(str(inst.institution_id) == str(sub.parent_institution_id) for inst in data['institutions'])
        
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
            assert 0 < float(rel['r.ownership_percentage']) <= 100
            assert rel['r.acquisition_date']
            
        # Check incorporation relationships
        result = await session.run("""
            MATCH (s:Subsidiary)-[:INCORPORATED_IN]->(c:Country)
            RETURN count(s) as count
        """)
        subsidiary_count = (await result.data())[0]['count']
        assert subsidiary_count > 0

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

@pytest.mark.asyncio
async def test_neo4j_relationship_completeness(data_generator, neo4j_handler):
    """Test that all expected relationships are created in Neo4j."""
    # Generate and persist data
    await data_generator.generate_all()

    # Define expected relationship types
    expected_relationships = [
        # Institution and Subsidiary relationships
        "OWNS_SUBSIDIARY",
        "IS_CUSTOMER",
        "INCORPORATED_IN",
        "INCORPORATED_ON",
        
        # Account relationships
        "HAS_ACCOUNT",
        "OPENED_ON",
        
        # Transaction relationships
        "TRANSACTED",
        "TRANSACTED_ON",
        
        # Document relationships
        "HAS_DOCUMENT",
        "ISSUED_ON",
        
        # Risk Assessment relationships
        "HAS_RISK_ASSESSMENT",
        
        # Compliance Event relationships
        "HAS_COMPLIANCE_EVENT",
        
        # Beneficial Owner relationships
        "OWNED_BY",
        "CITIZEN_OF",
        
        # Authorized Person relationships
        "HAS_AUTHORIZED_PERSON"
    ]

    async with neo4j_handler.driver.session() as session:
        # Check each relationship type
        for rel_type in expected_relationships:
            result = await session.run(f"""
                MATCH ()-[r:{rel_type}]->()
                RETURN count(r) as count
            """)
            count = (await result.data())[0]['count']
            assert count > 0, f"Expected relationships of type {rel_type} but found none"

        # Test specific relationship patterns
        # 1. Institution to Subsidiary relationships
        result = await session.run("""
            MATCH path = (i:Institution)-[:OWNS_SUBSIDIARY]->(s:Subsidiary)
            RETURN count(path) as count
        """)
        subsidiary_count = (await result.data())[0]['count']
        assert subsidiary_count > 0, "No Institution-Subsidiary relationships found"

        # 2. Account relationships
        result = await session.run("""
            MATCH path = (i:Institution)-[:HAS_ACCOUNT]->(a:Account)
            RETURN count(path) as count
        """)
        account_count = (await result.data())[0]['count']
        assert account_count > 0, "No Account relationships found"

        # 3. Transaction relationships
        result = await session.run("""
            MATCH path = (a:Account)-[:TRANSACTED]->(t:Transaction)
            RETURN count(path) as count
        """)
        transaction_count = (await result.data())[0]['count']
        assert transaction_count > 0, "No Transaction relationships found"

        # 4. Document relationships
        result = await session.run("""
            MATCH path = (e)-[:HAS_DOCUMENT]->(d:Document)
            WHERE e:Institution OR e:Subsidiary
            RETURN count(path) as count
        """)
        document_count = (await result.data())[0]['count']
        assert document_count > 0, "No Document relationships found"

        # 5. Beneficial Owner relationships
        result = await session.run("""
            MATCH path = (i:Institution)-[:OWNED_BY]->(b:BeneficialOwner)
            RETURN count(path) as count
        """)
        owner_count = (await result.data())[0]['count']
        assert owner_count > 0, "No Beneficial Owner relationships found"

        # 6. Country relationships
        result = await session.run("""
            MATCH path = (n)-[r:INCORPORATED_IN|OPERATES_IN]->(c:Country)
            RETURN count(path) as count
        """)
        country_count = (await result.data())[0]['count']
        assert country_count > 0, "No Country relationships found"

        # 7. BusinessDate relationships
        result = await session.run("""
            MATCH path = (n)-[r:INCORPORATED_ON|OPENED_ON|TRANSACTED_ON|ISSUED_ON]->(d:BusinessDate)
            RETURN count(path) as count
        """)
        date_count = (await result.data())[0]['count']
        assert date_count > 0, "No BusinessDate relationships found"

        # 8. Risk and Compliance relationships
        result = await session.run("""
            MATCH path = (e)-[r:HAS_RISK_ASSESSMENT|HAS_COMPLIANCE_EVENT]->(n)
            WHERE e:Institution OR e:Subsidiary
            RETURN count(path) as count
        """)
        risk_compliance_count = (await result.data())[0]['count']
        assert risk_compliance_count > 0, "No Risk/Compliance relationships found"

        # 9. Authorized Person relationships
        result = await session.run("""
            MATCH path = (e)-[:HAS_AUTHORIZED_PERSON]->(a:AuthorizedPerson)
            WHERE e:Institution OR e:Subsidiary
            RETURN count(path) as count
        """)
        auth_person_count = (await result.data())[0]['count']
        assert auth_person_count > 0, "No Authorized Person relationships found"

@pytest.mark.asyncio
async def test_relationship_data_integrity(data_generator, neo4j_handler):
    """Test that relationship properties contain valid data."""
    await data_generator.generate_all()

    async with neo4j_handler.driver.session() as session:
        # Test ownership percentage in OWNS_SUBSIDIARY
        result = await session.run("""
            MATCH (i:Institution)-[r:OWNS_SUBSIDIARY]->(s:Subsidiary)
            RETURN r.ownership_percentage as ownership
        """)
        ownerships = [record['ownership'] async for record in result]
        assert all(0 < ownership <= 100 for ownership in ownerships), "Invalid ownership percentages found"

        # Test temporal relationship dates
        result = await session.run("""
            MATCH (n)-[r:INCORPORATED_ON|ASSESSED_ON|OCCURRED_ON|FILED_ON]->(d:BusinessDate)
            RETURN d.date as date
        """)
        dates = [record['date'] async for record in result]
        assert all(isinstance(date, str) and len(date) == 10 for date in dates), "Invalid date format found"

        # Test country codes in location relationships
        result = await session.run("""
            MATCH (n)-[r:INCORPORATED_IN|OPERATES_IN]->(c:Country)
            RETURN c.code as code
        """)
        country_codes = [record['code'] async for record in result]
        assert all(len(code) == 2 for code in country_codes), "Invalid country codes found"

@pytest.mark.asyncio
async def test_subsidiary_generation(data_generator):
    """Test that subsidiaries are generated with required fields and proper relationships."""
    # Generate all data
    await data_generator.generate_all()
    
    # Get subsidiaries from Neo4j
    async with data_generator.neo4j_handler.driver.session() as session:
        result = await session.run("""
            MATCH (s:Subsidiary)
            RETURN s {
                .subsidiary_id,
                .parent_institution_id,
                .legal_name,
                .tax_id,
                .incorporation_country,
                .incorporation_date,
                .acquisition_date,
                .business_type,
                .operational_status,
                .parent_ownership_percentage,
                .consolidation_status,
                .capital_investment,
                .functional_currency,
                material_subsidiary: CASE 
                    WHEN s.material_subsidiary = true OR s.material_subsidiary = 1.0 THEN true 
                    ELSE false 
                END
            } as s
        """)
        subsidiaries = await result.data()
    
    assert len(subsidiaries) > 0
    
    # Verify subsidiary attributes
    for record in subsidiaries:
        sub = record['s']
        
        # Basic attributes
        assert sub['subsidiary_id']
        assert sub['parent_institution_id']
        assert sub['legal_name']
        assert sub['tax_id']
        assert sub['incorporation_country']
        assert sub['incorporation_date']
        assert sub['acquisition_date']
        assert sub['business_type']
        assert sub['operational_status']
        assert isinstance(float(sub['parent_ownership_percentage']), float)
        assert sub['consolidation_status']
        assert isinstance(float(sub['capital_investment']), float)
        assert sub['functional_currency']
        assert isinstance(sub['material_subsidiary'], bool)
