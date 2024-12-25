"""Test suite for data generation functionality."""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import uuid
from typing import Dict, Any

from aml_monitoring.data_generator import (
    DataGenerator, InstitutionGenerator, AddressGenerator,
    AccountGenerator, TransactionGenerator, BeneficialOwnerGenerator,
    RiskAssessmentGenerator, AuthorizedPersonGenerator,
    DocumentGenerator, JurisdictionPresenceGenerator,
    ComplianceEventGenerator
)
from aml_monitoring.models import (
    Institution, Address, Account, Transaction,
    BusinessType, OperationalStatus, RiskRating,
    TransactionType, TransactionStatus
)

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Test configuration for data generation."""
    return {
        'num_institutions': 1,
        'min_transactions_per_account': 2,
        'max_transactions_per_account': 5,
        'high_risk_percentage': 0.2,
        'date_start': datetime(2020, 1, 1),
        'date_end': datetime(2023, 12, 31),
        'batch_size': 100
    }

@pytest.fixture
def data_generator(test_config):
    """Create a DataGenerator instance."""
    return DataGenerator(test_config)

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
    # Add debugging prints
    print("\nDebugging PostgresHandler:")
    print(f"Type of postgres_handler: {type(data_generator.postgres_handler)}")
    print(f"Available methods: {dir(data_generator.postgres_handler)}")
    
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
    await data_generator.initialize_db()
    
    try:
        # Generate all data
        await data_generator.generate_all()
        
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
            
    finally:
        # Clean up
        await data_generator.postgres_handler.wipe_clean()
        await data_generator.neo4j_handler.wipe_clean()
        await data_generator.close_db()
