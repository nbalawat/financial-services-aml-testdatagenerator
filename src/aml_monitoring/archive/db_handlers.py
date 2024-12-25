import os
import pandas as pd
import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from neo4j import AsyncGraphDatabase
from enum import Enum
import uuid
import numpy as np

class PostgresHandler:
    def __init__(self):
        """Initialize PostgreSQL handler."""
        self.engine = None
    
    async def initialize(self):
        """Initialize database connection."""
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        db = os.getenv('POSTGRES_DB', 'aml_monitoring')
        user = os.getenv('POSTGRES_USER', 'aml_user')
        password = os.getenv('POSTGRES_PASSWORD', 'aml_password')
        
        db_url = f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'
        self.engine = create_async_engine(db_url, echo=True)
        
        # Create tables if they don't exist
        async with self.engine.begin() as conn:
            # First drop all tables if they exist to ensure a clean state
            tables = [
                'institutions',
                'subsidiaries',
                'accounts',
                'transactions',
                'beneficial_owners',
                'addresses',
                'compliance_events',
                'jurisdiction_presence',
                'customers',
                'risk_assessments',
                'authorized_persons',
                'documents'
            ]
            
            print("\nDropping existing tables...")
            for table in tables:
                try:
                    await conn.execute(text(f'DROP TABLE IF EXISTS {table} CASCADE'))
                    print(f"✓ Dropped table {table}")
                except Exception as e:
                    print(f"Error dropping table {table}: {str(e)}")
            
            # Then create all tables
            print("\nCreating tables...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS institutions (
                    institution_id UUID PRIMARY KEY,
                    legal_name VARCHAR(255) NOT NULL,
                    business_type VARCHAR(50) NOT NULL,
                    incorporation_country VARCHAR(2) NOT NULL,
                    incorporation_date DATE NOT NULL,
                    onboarding_date DATE NOT NULL,
                    operational_status VARCHAR(20) NOT NULL,
                    regulatory_status VARCHAR(50),
                    licenses JSONB,
                    industry_codes JSONB,
                    public_company BOOLEAN,
                    stock_symbol VARCHAR(10),
                    stock_exchange VARCHAR(50),
                    kyc_refresh_date DATE,
                    last_audit_date DATE
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS subsidiaries (
                    subsidiary_id UUID PRIMARY KEY,
                    institution_id UUID NOT NULL REFERENCES institutions(institution_id),
                    legal_name VARCHAR(255) NOT NULL,
                    business_type VARCHAR(50) NOT NULL,
                    incorporation_country VARCHAR(2) NOT NULL,
                    incorporation_date DATE NOT NULL,
                    operational_status VARCHAR(20) NOT NULL,
                    parent_ownership_percentage DECIMAL NOT NULL,
                    local_licenses JSONB,
                    financial_metrics JSONB,
                    industry_codes JSONB
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    account_type VARCHAR(50) NOT NULL,
                    account_status VARCHAR(20) NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    opening_date DATE NOT NULL,
                    closing_date DATE,
                    last_transaction_date DATE,
                    risk_rating VARCHAR(20) NOT NULL,
                    purpose VARCHAR(255) NOT NULL
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    account_id UUID NOT NULL,
                    transaction_type VARCHAR(50) NOT NULL,
                    transaction_date DATE NOT NULL,
                    amount DECIMAL NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    transaction_status VARCHAR(20) NOT NULL,
                    is_debit BOOLEAN NOT NULL,
                    counterparty_account VARCHAR(50),
                    counterparty_name VARCHAR(255),
                    counterparty_bank VARCHAR(255),
                    counterparty_entity_name VARCHAR(255),
                    originating_country VARCHAR(2),
                    destination_country VARCHAR(2),
                    purpose VARCHAR(255),
                    reference_number VARCHAR(50)
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS beneficial_owners (
                    owner_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    nationality VARCHAR(2) NOT NULL,
                    country_of_residence VARCHAR(2) NOT NULL,
                    ownership_percentage DECIMAL NOT NULL,
                    dob DATE NOT NULL,
                    verification_date DATE NOT NULL,
                    pep_status BOOLEAN NOT NULL,
                    sanctions_status BOOLEAN NOT NULL,
                    adverse_media_status BOOLEAN NOT NULL,
                    verification_source VARCHAR(50) NOT NULL,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS addresses (
                    address_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    address_type VARCHAR(50) NOT NULL,
                    street_address TEXT NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    state VARCHAR(100),
                    postal_code VARCHAR(20),
                    country VARCHAR(2) NOT NULL,
                    verification_date DATE,
                    verification_source VARCHAR(50)
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS compliance_events (
                    event_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    event_date DATE NOT NULL,
                    event_description TEXT NOT NULL,
                    old_state VARCHAR(50),
                    new_state VARCHAR(50),
                    decision VARCHAR(50) NOT NULL,
                    decision_date DATE NOT NULL,
                    decision_maker VARCHAR(100) NOT NULL,
                    next_review_date DATE
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS jurisdiction_presence (
                    presence_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    country VARCHAR(2) NOT NULL,
                    presence_type VARCHAR(50) NOT NULL,
                    registration_number VARCHAR(100),
                    registration_date DATE,
                    expiry_date DATE,
                    regulatory_status VARCHAR(50),
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS risk_assessments (
                    assessment_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    assessment_date DATE NOT NULL,
                    risk_rating VARCHAR(50) NOT NULL,
                    risk_score VARCHAR(20) NOT NULL,
                    assessment_type VARCHAR(50) NOT NULL,
                    risk_factors JSONB NOT NULL,
                    conducted_by VARCHAR(100) NOT NULL,
                    approved_by VARCHAR(100) NOT NULL,
                    findings TEXT NOT NULL,
                    assessor VARCHAR(100) NOT NULL,
                    next_review_date DATE NOT NULL,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS authorized_persons (
                    person_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    authorization_level VARCHAR(50) NOT NULL,
                    authorization_type VARCHAR(50) NOT NULL,
                    authorization_start DATE NOT NULL,
                    authorization_end DATE,
                    contact_info JSONB,
                    is_active BOOLEAN NOT NULL DEFAULT true,
                    last_verification_date DATE,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    document_type VARCHAR(50) NOT NULL,
                    document_number VARCHAR(100) NOT NULL,
                    issuing_country VARCHAR(2) NOT NULL,
                    issuing_authority VARCHAR(100) NOT NULL,
                    issue_date DATE NOT NULL,
                    expiry_date DATE,
                    verification_status VARCHAR(50) NOT NULL,
                    verification_date DATE NOT NULL,
                    document_category VARCHAR(50) NOT NULL,
                    notes TEXT
                )
            """))
            
            print("✓ All tables created successfully")
    
    async def initialize_tables(self):
        """Initialize database tables."""
        if not self.engine:
            await self.initialize()

        async with self.engine.begin() as conn:
            print("\nCreating tables in PostgreSQL...")
            
            # Create institutions table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS institutions (
                        institution_id TEXT PRIMARY KEY,
                        legal_name TEXT NOT NULL,
                        business_type TEXT NOT NULL,
                        incorporation_country TEXT NOT NULL,
                        incorporation_date DATE NOT NULL,
                        onboarding_date DATE NOT NULL,
                        operational_status TEXT NOT NULL,
                        regulatory_status TEXT,
                        licenses JSONB,
                        industry_codes JSONB,
                        public_company BOOLEAN,
                        stock_symbol TEXT,
                        stock_exchange TEXT,
                        kyc_refresh_date DATE,
                        last_audit_date DATE,
                        next_audit_date DATE,
                        last_review_date DATE
                    )
                """))
                print("✓ Created institutions table")
            except Exception as e:
                print(f"Error creating institutions table: {e}")
                raise
            
            # Create subsidiaries table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS subsidiaries (
                        subsidiary_id TEXT PRIMARY KEY,
                        parent_institution_id TEXT REFERENCES institutions(institution_id),
                        legal_name TEXT NOT NULL,
                        business_type TEXT NOT NULL,
                        incorporation_country TEXT NOT NULL,
                        incorporation_date DATE NOT NULL,
                        acquisition_date DATE,
                        operational_status TEXT NOT NULL,
                        regulatory_status TEXT,
                        local_licenses JSONB,
                        financial_metrics JSONB,
                        industry_codes JSONB
                    )
                """))
                print("✓ Created subsidiaries table")
            except Exception as e:
                print(f"Error creating subsidiaries table: {e}")
                raise
            
            # Create accounts table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        account_type TEXT NOT NULL,
                        account_number TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        status TEXT NOT NULL,
                        opening_date DATE NOT NULL,
                        last_activity_date DATE NOT NULL,
                        balance DECIMAL NOT NULL,
                        risk_rating TEXT NOT NULL,
                        purpose TEXT,
                        average_monthly_balance DECIMAL,
                        custodian_bank TEXT,
                        account_officer TEXT,
                        custodian_country TEXT
                    )
                """))
                print("✓ Created accounts table")
            except Exception as e:
                print(f"Error creating accounts table: {e}")
                raise
            
            # Create risk_assessments table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS risk_assessments (
                        assessment_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        assessment_date DATE NOT NULL,
                        risk_rating TEXT NOT NULL,
                        risk_score FLOAT NOT NULL,
                        assessment_type TEXT NOT NULL,
                        risk_factors JSONB,
                        conducted_by TEXT,
                        approved_by TEXT,
                        findings TEXT,
                        assessor TEXT,
                        next_review_date DATE,
                        notes TEXT
                    )
                """))
                print("✓ Created risk_assessments table")
            except Exception as e:
                print(f"Error creating risk_assessments table: {e}")
                raise

            # Create authorized_persons table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS authorized_persons (
                        person_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        name TEXT NOT NULL,
                        title TEXT NOT NULL,
                        authorization_level TEXT NOT NULL,
                        authorization_type TEXT NOT NULL,
                        authorization_start DATE NOT NULL,
                        authorization_end DATE,
                        contact_info JSONB,
                        is_active BOOLEAN NOT NULL DEFAULT true,
                        last_verification_date DATE,
                        notes TEXT
                    )
                """))
                print("✓ Created authorized_persons table")
            except Exception as e:
                print(f"Error creating authorized_persons table: {e}")
                raise

            # Create documents table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS documents (
                        document_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        document_type TEXT NOT NULL,
                        document_category TEXT NOT NULL,
                        issue_date DATE,
                        expiry_date DATE,
                        issuing_authority TEXT,
                        document_number TEXT,
                        verification_status TEXT,
                        last_verified DATE,
                        notes TEXT
                    )
                """))
                print("✓ Created documents table")
            except Exception as e:
                print(f"Error creating documents table: {e}")
                raise

            # Create transactions table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id TEXT PRIMARY KEY,
                        transaction_type TEXT NOT NULL,
                        transaction_date DATE NOT NULL,
                        amount DECIMAL NOT NULL,
                        currency TEXT NOT NULL,
                        transaction_status TEXT NOT NULL,
                        is_debit BOOLEAN NOT NULL,
                        account_id TEXT NOT NULL,
                        counterparty_account TEXT NOT NULL,
                        counterparty_name TEXT NOT NULL,
                        counterparty_bank TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        counterparty_entity_name TEXT NOT NULL,
                        originating_country TEXT NOT NULL,
                        destination_country TEXT NOT NULL,
                        purpose TEXT NOT NULL,
                        reference_number TEXT NOT NULL,
                        screening_alert BOOLEAN NOT NULL DEFAULT FALSE,
                        alert_details TEXT,
                        risk_score INTEGER,
                        processing_fee DECIMAL,
                        exchange_rate DECIMAL,
                        value_date DATE,
                        batch_id TEXT,
                        check_number TEXT,
                        wire_reference TEXT
                    )
                """))
                print("✓ Created transactions table")
            except Exception as e:
                print(f"Error creating transactions table: {e}")
                raise

            # Create addresses table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS addresses (
                        address_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        address_type TEXT NOT NULL,
                        address_line1 TEXT NOT NULL,
                        address_line2 TEXT,
                        city TEXT NOT NULL,
                        state_province TEXT,
                        postal_code TEXT,
                        country TEXT NOT NULL,
                        status TEXT NOT NULL,
                        effective_from DATE NOT NULL,
                        effective_to DATE,
                        primary_address BOOLEAN NOT NULL,
                        validation_status TEXT NOT NULL,
                        last_verified DATE,
                        geo_coordinates JSONB,
                        timezone TEXT
                    )
                """))
                print("✓ Created addresses table")
            except Exception as e:
                print(f"Error creating addresses table: {e}")
                raise

            # Create beneficial owners table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS beneficial_owners (
                        owner_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        name TEXT NOT NULL,
                        nationality TEXT NOT NULL,
                        country_of_residence TEXT NOT NULL,
                        ownership_percentage DECIMAL NOT NULL,
                        dob DATE NOT NULL,
                        verification_date DATE NOT NULL,
                        pep_status BOOLEAN NOT NULL,
                        sanctions_status BOOLEAN NOT NULL,
                        adverse_media_status BOOLEAN NOT NULL,
                        verification_source TEXT NOT NULL,
                        notes TEXT
                    )
                """))
                print("✓ Created beneficial owners table")
            except Exception as e:
                print(f"Error creating beneficial owners table: {e}")
            
            # Create compliance events table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS compliance_events (
                        event_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        event_date DATE,
                        event_type TEXT NOT NULL,
                        event_description TEXT NOT NULL,
                        old_state TEXT,
                        new_state TEXT NOT NULL,
                        decision TEXT,
                        decision_date DATE,
                        decision_maker TEXT,
                        next_review_date DATE,
                        related_account_id TEXT NOT NULL,
                        notes TEXT
                    )
                """))
                print("✓ Created compliance events table")
            except Exception as e:
                print(f"Error creating compliance events table: {e}")
                raise

            # Create jurisdiction presence table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS jurisdiction_presence (
                        presence_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        jurisdiction TEXT NOT NULL,
                        registration_date DATE,
                        effective_from DATE,
                        effective_to DATE,
                        status TEXT NOT NULL,
                        local_registration_id TEXT NOT NULL,
                        local_registration_date DATE,
                        local_registration_authority TEXT NOT NULL,
                        notes TEXT
                    )
                """))
                print("✓ Created jurisdiction presence table")
            except Exception as e:
                print(f"Error creating jurisdiction presence table: {e}")
                raise

            # Create customers table
            try:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS customers (
                        customer_id TEXT PRIMARY KEY,
                        entity_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        customer_type TEXT NOT NULL,
                        onboarding_date DATE NOT NULL,
                        risk_rating TEXT NOT NULL,
                        status TEXT NOT NULL,
                        kyc_status TEXT NOT NULL,
                        last_review_date DATE NOT NULL,
                        next_review_date DATE NOT NULL,
                        relationship_manager TEXT NOT NULL,
                        segment TEXT NOT NULL,
                        products_used TEXT[] NOT NULL,
                        credit_rating TEXT,
                        credit_limit DECIMAL,
                        total_exposure DECIMAL,
                        annual_revenue DECIMAL,
                        industry_sector TEXT NOT NULL,
                        employee_count INTEGER,
                        incorporation_date DATE,
                        business_description TEXT,
                        website TEXT
                    )
                """))
                print("✓ Created customers table")
            except Exception as e:
                print(f"Error creating customers table: {e}")
                raise

            print("\n✓ All tables created successfully")
    
    async def cleanup_postgres(self):
        """Clean up PostgreSQL tables."""
        async with self.engine.begin() as conn:
            print("Cleaning up PostgreSQL tables...")
            await conn.execute(text("""
                DROP TABLE IF EXISTS transactions CASCADE;
                DROP TABLE IF EXISTS institutions CASCADE;
                DROP TABLE IF EXISTS subsidiaries CASCADE;
                DROP TABLE IF EXISTS addresses CASCADE;
                DROP TABLE IF EXISTS risk_assessments CASCADE;
                DROP TABLE IF EXISTS beneficial_owners CASCADE;
                DROP TABLE IF EXISTS authorized_persons CASCADE;
                DROP TABLE IF EXISTS documents CASCADE;
                DROP TABLE IF EXISTS jurisdiction_presence CASCADE;
                DROP TABLE IF EXISTS accounts CASCADE;
                DROP TABLE IF EXISTS subsidiary_relationships CASCADE;
                DROP TABLE IF EXISTS compliance_events CASCADE;
                DROP TABLE IF EXISTS customers CASCADE;
            """))
            print("✓ All tables dropped successfully")

    async def save_transactions(self, transactions: List[Dict[str, Any]]):
        """Save transactions to PostgreSQL."""
        if not transactions:
            return

        # Ensure we have a connection
        if not self.engine:
            await self.initialize()

        try:
            # Process transactions in chunks
            chunk_size = 1000
            for i in range(0, len(transactions), chunk_size):
                chunk = transactions[i:i + chunk_size]
                
                # Process each transaction in the chunk
                processed_chunk = []
                for txn in chunk:
                    processed_txn = {}
                    print(f"\nProcessing transaction {txn.get('transaction_id')}:")
                    print(f"Original transaction fields: {list(txn.keys())}")
                    
                    for key, value in txn.items():
                        if key == 'transaction_date' or key == 'value_date':
                            # Convert ISO format string to datetime object
                            if isinstance(value, str):
                                processed_txn[key] = datetime.fromisoformat(value.replace('Z', '+00:00')).date()
                            else:
                                processed_txn[key] = value
                        elif isinstance(value, (pd.Timestamp, datetime)):
                            processed_txn[key] = value.date()
                        elif isinstance(value, uuid.UUID):
                            processed_txn[key] = str(value)
                        elif isinstance(value, dict) and 0 in value:
                            processed_txn[key] = value[0]
                        else:
                            processed_txn[key] = value
                    
                    print(f"Processed transaction fields: {list(processed_txn.keys())}")
                    if 'transaction_status' not in processed_txn:
                        print(f"WARNING: transaction_status not found in processed transaction!")
                    
                    processed_chunk.append(processed_txn)
                
                # Insert chunk using executemany
                async with self.engine.begin() as conn:
                    # Check if table exists
                    exists = await conn.scalar(
                        text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'transactions')")
                    )
                    
                    if not exists:
                        print("Table 'transactions' does not exist. Reinitializing...")
                        await self.initialize()
                    
                    # Insert chunk using executemany
                    await conn.execute(
                        text("""
                            INSERT INTO transactions (
                                transaction_id, entity_id, entity_type, account_id,
                                transaction_type, transaction_date, amount, currency,
                                transaction_status, is_debit, counterparty_account,
                                counterparty_name, counterparty_bank, counterparty_entity_name,
                                originating_country, destination_country, purpose,
                                reference_number, screening_alert, alert_details,
                                risk_score, processing_fee, exchange_rate, value_date,
                                batch_id, check_number, wire_reference
                            ) VALUES (
                                :transaction_id, :entity_id, :entity_type, :account_id,
                                :transaction_type, :transaction_date, :amount, :currency,
                                :transaction_status, :is_debit, :counterparty_account,
                                :counterparty_name, :counterparty_bank, :counterparty_entity_name,
                                :originating_country, :destination_country, :purpose,
                                :reference_number, :screening_alert, :alert_details,
                                :risk_score, :processing_fee, :exchange_rate, :value_date,
                                :batch_id, :check_number, :wire_reference
                            )
                        """),
                        processed_chunk
                    )

        except Exception as e:
            print(f"Error saving transactions to PostgreSQL: {e}")
            raise
    
    async def close(self):
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()

class Neo4jHandler:
    def __init__(self):
        """Initialize Neo4j handler."""
        self.driver = None
    
    async def initialize(self):
        """Initialize database connection."""
        host = os.getenv('NEO4J_HOST', 'neo4j')  # Use service name from docker-compose
        port = os.getenv('NEO4J_PORT', '7687')
        user = os.getenv('NEO4J_USER', 'neo4j')
        password = os.getenv('NEO4J_PASSWORD', 'aml_password')
        
        uri = f'neo4j://{host}:{port}'
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        
        # Create constraints
        async with self.driver.session() as session:
            await session.run("""
                CREATE CONSTRAINT business_date_id IF NOT EXISTS
                FOR (d:BusinessDate) REQUIRE d.date IS UNIQUE
            """)
            
            await session.run("""
                CREATE CONSTRAINT institution_id IF NOT EXISTS
                FOR (i:Institution) REQUIRE i.entity_id IS UNIQUE
            """)
    
    async def save_transactions_rollup(self, transactions: List[Dict[str, Any]]):
        """Save rolled-up transaction data to Neo4j."""
        if not transactions:
            return
            
        # Convert transactions to DataFrame for aggregation
        df = pd.DataFrame(transactions)
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        # Group by date and calculate metrics
        daily_stats = df.groupby(['transaction_date', 'account_id']).agg({
            'amount': ['sum', 'count'],
            'screening_alert': ['sum'],  # Count of alerts
            'risk_score': ['mean']  # Average risk score
        }).reset_index()
        
        # Create Neo4j nodes and relationships
        total_stats = len(daily_stats)
        success_count = 0
        
        async with self.driver.session() as session:
            for _, row in daily_stats.iterrows():
                try:
                    # Create BusinessDate node and relationship
                    await session.run("""
                        MERGE (d:BusinessDate {date: $date})
                        WITH d
                        MATCH (a:Account {account_id: $account_id})
                        MERGE (a)-[r:TRANSACTED_ON]->(d)
                        SET r.total_amount = $total_amount,
                            r.transaction_count = $transaction_count,
                            r.alert_count = $alert_count,
                            r.avg_risk_score = $avg_risk_score
                    """, {
                        'date': str(row['transaction_date']),
                        'account_id': row['account_id'],
                        'total_amount': float(row[('amount', 'sum')]),
                        'transaction_count': int(row[('amount', 'count')]),
                        'alert_count': int(row[('screening_alert', 'sum')]),
                        'avg_risk_score': float(row[('risk_score', 'mean')])
                    })
                    success_count += 1
                except Exception as e:
                    print(f"Error saving transaction rollup for account {row['account_id']} on {row['transaction_date']}: {str(e)}")
            
            print(f"✓ Saved {success_count}/{total_stats} transaction rollups")
    
    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j."""
        print("\n=== Saving Data to Neo4j ===")
        async with self.driver.session() as neo4j_session:
            # Clean up existing data
            await neo4j_session.run("MATCH (n) DETACH DELETE n")
            await neo4j_session.run("CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.institution_id IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT subsidiary_id IF NOT EXISTS FOR (s:Subsidiary) REQUIRE s.subsidiary_id IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT country_code IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT business_date IF NOT EXISTS FOR (bd:BusinessDate) REQUIRE bd.date IS UNIQUE")
            print("✓ Cleaned up Neo4j database")
            
            # Save institutions
            if 'institutions' in data and not data['institutions'].empty:
                print("Saving institutions to Neo4j...")
                for _, row in data['institutions'].iterrows():
                    try:
                        await neo4j_session.run("""
                            CREATE (i:Institution {
                                institution_id: $id,
                                legal_name: $name,
                                business_type: $business_type,
                                incorporation_country: $country,
                                incorporation_date: $inc_date,
                                operational_status: $status,
                                regulatory_status: $reg_status,
                                licenses: $licenses,
                                industry_codes: $industry_codes,
                                public_company: $public,
                                stock_symbol: $symbol,
                                stock_exchange: $exchange
                            })
                            WITH i
                            MERGE (c:Country {code: $country})
                            MERGE (i)-[:INCORPORATED_IN]->(c)
                            WITH i
                            MERGE (bd:BusinessDate {date: $inc_date})
                            MERGE (i)-[:INCORPORATED_ON]->(bd)
                        """, {
                            'id': str(row['institution_id']),
                            'name': row['legal_name'],
                            'business_type': row['business_type'],
                            'country': row['incorporation_country'],
                            'inc_date': row['incorporation_date'],
                            'status': row['operational_status'],
                            'reg_status': row['regulatory_status'],
                            'licenses': json.dumps(row['licenses']),
                            'industry_codes': json.dumps(row['industry_codes']),
                            'public': row['public_company'],
                            'symbol': row['stock_symbol'],
                            'exchange': row['stock_exchange']
                        })
                    except Exception as e:
                        print(f"Error saving institution {row['legal_name']}: {e}")
                print("✓ Saved institutions")
            
            # Save subsidiaries
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Saving subsidiaries to Neo4j...")
                for _, row in data['subsidiaries'].iterrows():
                    try:
                        # Convert data to native Python types
                        subsidiary_data = {
                            'id': str(row['subsidiary_id']),
                            'parent_id': str(row['parent_institution_id']),
                            'name': str(row['legal_name']),
                            'business_type': str(row['business_type']) if pd.notna(row['business_type']) else None,
                            'country': str(row['incorporation_country']) if pd.notna(row['incorporation_country']) else None,
                            'inc_date': str(row['incorporation_date']) if pd.notna(row['incorporation_date']) else None,
                            'status': str(row['operational_status']) if pd.notna(row['operational_status']) else None,
                            'reg_status': str(row['regulatory_status']) if pd.notna(row['regulatory_status']) else None,
                            'licenses': row['local_licenses'] if isinstance(row['local_licenses'], list) else [],
                            'financial_metrics': json.dumps(row['financial_metrics']) if isinstance(row['financial_metrics'], dict) else "{}",
                            'is_regulated': bool(row['is_regulated']) if pd.notna(row['is_regulated']) else False,
                            'is_customer': bool(row['is_customer']) if pd.notna(row['is_customer']) else False,
                            'customer_id': str(row['customer_id']) if pd.notna(row['customer_id']) else None,
                            'onboarding_date': str(row['customer_onboarding_date']) if pd.notna(row['customer_onboarding_date']) else None,
                            'risk_rating': str(row['customer_risk_rating']) if pd.notna(row['customer_risk_rating']) else None,
                            'customer_status': str(row['customer_status']) if pd.notna(row['customer_status']) else None,
                            'industry_codes': row['industry_codes'] if isinstance(row['industry_codes'], list) else []
                        }
                        
                        await neo4j_session.run("""
                            MATCH (p:Institution {institution_id: $parent_id})
                            CREATE (s:Subsidiary {subsidiary_id: $id})
                            SET s.legal_name = $name,
                                s.business_type = $business_type,
                                s.incorporation_country = $country,
                                s.incorporation_date = $inc_date,
                                s.operational_status = $status,
                                s.regulatory_status = $reg_status,
                                s.local_licenses = $licenses,
                                s.financial_metrics = $financial_metrics,
                                s.industry_codes = $industry_codes,
                                s.is_regulated = $is_regulated,
                                s.is_customer = $is_customer,
                                s.customer_id = $customer_id,
                                s.customer_onboarding_date = $onboarding_date,
                                s.customer_risk_rating = $risk_rating,
                                s.customer_status = $customer_status
                            WITH p, s
                            CREATE (p)-[:OWNS]->(s)
                            WITH s
                            MERGE (c:Country {code: $country})
                            MERGE (s)-[:INCORPORATED_IN]->(c)
                            WITH s
                            MERGE (bd:BusinessDate {date: $inc_date})
                            MERGE (s)-[:INCORPORATED_ON]->(bd)
                        """, subsidiary_data)
                    except Exception as e:
                        print(f"Error saving subsidiary {row['legal_name']}: {e}")
                print("✓ Saved subsidiaries")
            
            # Save risk assessments
            if 'risk_assessments' in data and not data['risk_assessments'].empty:
                print("Saving risk assessments to Neo4j...")
                total_assessments = len(data['risk_assessments'])
                success_count = 0
                
                for idx, row in data['risk_assessments'].iterrows():
                    try:
                        # Convert dates to string format
                        assessment_date = pd.to_datetime(row['assessment_date']).strftime('%Y-%m-%d') if pd.notnull(row['assessment_date']) else None
                        next_review_date = pd.to_datetime(row['next_review_date']).strftime('%Y-%m-%d') if pd.notnull(row['next_review_date']) else None
                        
                        # Convert risk_factors to JSON string
                        risk_factors = json.dumps(row['risk_factors']) if isinstance(row['risk_factors'], (list, dict)) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (ra:RiskAssessment {
                                assessment_id: $id,
                                assessment_date: $assessment_date,
                                risk_rating: $risk_rating,
                                risk_score: $risk_score,
                                assessment_type: $assessment_type,
                                risk_factors: $risk_factors,
                                conducted_by: $conducted_by,
                                approved_by: $approved_by,
                                findings: $findings,
                                assessor: $assessor,
                                next_review_date: $next_review_date,
                                notes: $notes
                            })
                            CREATE (owner)-[:HAS_RISK_ASSESSMENT]->(ra)
                            WITH ra, $assessment_date as adate
                            WHERE adate IS NOT NULL
                            MERGE (bd:BusinessDate {date: adate})
                            CREATE (ra)-[:ASSESSED_ON]->(bd)
                            WITH ra, $next_review_date as rdate
                            WHERE rdate IS NOT NULL
                            MERGE (bd2:BusinessDate {date: rdate})
                            CREATE (ra)-[:NEXT_REVIEW_ON]->(bd2)
                        """, {
                            'id': str(row['assessment_id']),
                            'entity_id': str(row['entity_id']),
                            'assessment_date': assessment_date,
                            'risk_rating': str(row['risk_rating']),
                            'risk_score': str(row['risk_score']),
                            'assessment_type': str(row['assessment_type']),
                            'risk_factors': risk_factors,
                            'conducted_by': str(row['conducted_by']) if pd.notnull(row['conducted_by']) else None,
                            'approved_by': str(row['approved_by']) if pd.notnull(row['approved_by']) else None,
                            'findings': str(row['findings']) if pd.notnull(row['findings']) else None,
                            'assessor': str(row['assessor']) if pd.notnull(row['assessor']) else None,
                            'next_review_date': next_review_date,
                            'notes': str(row['notes']) if pd.notnull(row['notes']) else None
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving risk assessment {row['assessment_id']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_assessments} risk assessments")
            
            # Save authorized persons
            if 'authorized_persons' in data and not data['authorized_persons'].empty:
                print("Saving authorized persons to Neo4j...")
                total_persons = len(data['authorized_persons'])
                success_count = 0
                
                for idx, row in data['authorized_persons'].iterrows():
                    try:
                        # Convert dates to string format
                        auth_start = pd.to_datetime(row['authorization_start']).strftime('%Y-%m-%d') if pd.notnull(row['authorization_start']) else None
                        auth_end = pd.to_datetime(row['authorization_end']).strftime('%Y-%m-%d') if pd.notnull(row['authorization_end']) else None
                        verification_date = pd.to_datetime(row['last_verification_date']).strftime('%Y-%m-%d') if pd.notnull(row['last_verification_date']) else None
                        
                        # Convert contact_info to JSON string
                        contact_info = json.dumps(row['contact_info']) if isinstance(row['contact_info'], (dict, str)) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (ap:AuthorizedPerson {
                                person_id: $id,
                                name: $name,
                                title: $title,
                                authorization_level: $auth_level,
                                authorization_type: $auth_type,
                                authorization_start: $auth_start,
                                authorization_end: $auth_end,
                                contact_info: $contact_info,
                                is_active: $is_active,
                                last_verification_date: $verification_date
                            })
                            CREATE (owner)-[:HAS_AUTHORIZED_PERSON]->(ap)
                            WITH ap, $auth_start as adate
                            WHERE adate IS NOT NULL
                            MERGE (bd:BusinessDate {date: adate})
                            CREATE (ap)-[:AUTHORIZED_ON]->(bd)
                        """, {
                            'id': str(row['person_id']),
                            'entity_id': str(row['entity_id']),
                            'name': str(row['name']),
                            'title': str(row['title']) if pd.notnull(row['title']) else None,
                            'auth_level': str(row['authorization_level']),
                            'auth_type': str(row['authorization_type']),
                            'auth_start': auth_start,
                            'auth_end': auth_end,
                            'contact_info': contact_info,
                            'is_active': bool(row['is_active']),
                            'verification_date': verification_date
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving authorized person {row['name']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_persons} authorized persons")
            
            # Save documents
            if 'documents' in data and not data['documents'].empty:
                print("Saving documents to Neo4j...")
                total_documents = len(data['documents'])
                success_count = 0
                
                for idx, row in data['documents'].iterrows():
                    try:
                        # Convert dates to string format
                        issue_date = pd.to_datetime(row['issue_date']).strftime('%Y-%m-%d') if pd.notnull(row['issue_date']) else None
                        expiry_date = pd.to_datetime(row['expiry_date']).strftime('%Y-%m-%d') if pd.notnull(row['expiry_date']) else None
                        verification_date = pd.to_datetime(row['verification_date']).strftime('%Y-%m-%d') if pd.notnull(row['verification_date']) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (doc:Document {
                                document_id: $id,
                                document_type: $type,
                                document_number: $number,
                                issuing_authority: $authority,
                                issue_date: $issue_date,
                                expiry_date: $expiry_date,
                                document_category: $category,
                                verification_status: $verification_status,
                                verification_date: $verification_date,
                                notes: $notes
                            })
                            CREATE (owner)-[:HAS_DOCUMENT]->(doc)
                            WITH doc, $issue_date as idate
                            WHERE idate IS NOT NULL
                            MERGE (bd:BusinessDate {date: idate})
                            CREATE (doc)-[:ISSUED_ON]->(bd)
                        """, {
                            'id': str(row['document_id']),
                            'entity_id': str(row['entity_id']),
                            'type': str(row['document_type']),
                            'number': str(row['document_number']) if pd.notnull(row['document_number']) else None,
                            'authority': str(row['issuing_authority']) if pd.notnull(row['issuing_authority']) else None,
                            'issue_date': issue_date,
                            'expiry_date': expiry_date,
                            'category': str(row['document_category']),
                            'verification_status': str(row['verification_status']) if pd.notnull(row['verification_status']) else None,
                            'verification_date': verification_date,
                            'notes': str(row['notes']) if pd.notnull(row['notes']) else None
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving document {row['document_id']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_documents} documents")

            # Save accounts
            if 'accounts' in data and not data['accounts'].empty:
                print("Saving accounts to Neo4j...")
                total_accounts = len(data['accounts'])
                success_count = 0
                
                async with self.driver.session() as neo4j_session:
                    for idx, row in data['accounts'].iterrows():
                        try:
                            account_data = {
                                'entity_id': str(row['entity_id']),
                                'id': str(row['account_id']),
                                'type': str(row['account_type']),
                                'number': str(row['account_number']),
                                'currency': str(row['currency']),
                                'status': str(row['status']),
                                'opening_date': str(row['opening_date']),
                                'last_activity': str(row['last_activity_date']),
                                'balance': float(row['balance']),
                                'risk_rating': str(row['risk_rating']),
                                'purpose': str(row['purpose']) if pd.notna(row['purpose']) else None
                            }
                            
                            await neo4j_session.run("""
                                MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                                CREATE (a:Account {
                                    account_id: $id,
                                    account_type: $type,
                                    account_number: $number,
                                    currency: $currency,
                                    status: $status,
                                    opening_date: $opening_date,
                                    last_activity_date: $last_activity,
                                    balance: $balance,
                                    risk_rating: $risk_rating,
                                    purpose: $purpose
                                })
                                WITH e, a
                                CREATE (e)-[:HAS_ACCOUNT]->(a)
                                WITH a, $opening_date as odate
                                MERGE (bd:BusinessDate {date: odate})
                                CREATE (a)-[:OPENED_ON]->(bd)
                            """, account_data)
                            success_count += 1
                        except Exception as e:
                            print(f"\nError saving account {idx}: {e}")
                    print(f"✓ Saved {success_count}/{total_accounts} accounts")
            
            # Save beneficial owners
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                total_owners = len(data['beneficial_owners'])
                success_count = 0
                
                for idx, row in data['beneficial_owners'].iterrows():
                    try:
                        owner_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['owner_id']),
                            'name': str(row['name']),
                            'nationality': str(row['nationality']),
                            'residence': str(row['country_of_residence']),
                            'ownership': float(row['ownership_percentage']),
                            'dob': str(row['dob']),
                            'verification_date': str(row['verification_date']),
                            'pep': bool(row['pep_status']),
                            'sanctions': bool(row['sanctions_status']),
                            'adverse_media': bool(row['adverse_media_status'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (bo:BeneficialOwner {
                                owner_id: $id,
                                name: $name,
                                nationality: $nationality,
                                country_of_residence: $residence,
                                ownership_percentage: $ownership,
                                dob: $dob,
                                verification_date: $verification_date,
                                pep_status: $pep,
                                sanctions_status: $sanctions,
                                adverse_media_status: $adverse_media
                            })
                            WITH e, bo
                            CREATE (e)-[:HAS_BENEFICIAL_OWNER]->(bo)
                            WITH bo, $nationality as nat
                            MERGE (c:Country {code: nat})
                            CREATE (bo)-[:CITIZEN_OF]->(c)
                        """, owner_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving beneficial owner {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_owners} beneficial owners")

            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                
                for idx, row in data['addresses'].iterrows():
                    try:
                        address_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['address_id']),
                            'type': str(row['address_type']),
                            'line1': str(row['address_line1']),
                            'line2': str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                            'city': str(row['city']),
                            'state': str(row['state_province']),
                            'postal': str(row['postal_code']),
                            'country': str(row['country']),
                            'status': str(row['status']),
                            'from_date': str(row['effective_from']),
                            'to_date': str(row['effective_to']) if pd.notna(row['effective_to']) else None,
                            'primary': bool(row['primary_address'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Address {
                                address_id: $id,
                                address_type: $type,
                                address_line1: $line1,
                                address_line2: $line2,
                                city: $city,
                                state_province: $state,
                                postal_code: $postal,
                                country: $country,
                                status: $status,
                                effective_from: $from_date,
                                effective_to: $to_date,
                                primary_address: $primary
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ADDRESS]->(a)
                            WITH a, $country as c
                            MERGE (country:Country {code: c})
                            CREATE (a)-[:LOCATED_IN]->(country)
                        """, address_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving address {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_addresses} addresses")

            # Save compliance events
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Saving compliance events to Neo4j...")
                total_events = len(data['compliance_events'])
                success_count = 0
                
                for idx, row in data['compliance_events'].iterrows():
                    try:
                        event_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['event_id']),
                            'date': str(row['event_date']),
                            'type': str(row['event_type']),
                            'description': str(row['event_description']),
                            'old_state': str(row['old_state']) if pd.notna(row['old_state']) else None,
                            'new_state': str(row['new_state']),
                            'decision': str(row['decision']) if pd.notna(row['decision']) else None,
                            'decision_date': str(row['decision_date']) if pd.notna(row['decision_date']) else None,
                            'decision_maker': str(row['decision_maker']) if pd.notna(row['decision_maker']) else None,
                            'next_review': str(row['next_review_date']) if pd.notna(row['next_review_date']) else None
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (ce:ComplianceEvent {
                                event_id: $id,
                                event_date: $date,
                                event_type: $type,
                                event_description: $description,
                                old_state: $old_state,
                                new_state: $new_state,
                                decision: $decision,
                                decision_date: $decision_date,
                                decision_maker: $decision_maker,
                                next_review_date: $next_review
                            })
                            WITH e, ce
                            CREATE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                            WITH ce, $date as edate
                            MERGE (bd:BusinessDate {date: edate})
                            CREATE (ce)-[:OCCURRED_ON]->(bd)
                        """, event_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving compliance event {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_events} compliance events")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                total_transactions = len(data['transactions'])
                success_count = 0
                batch_size = 100  # Process transactions in batches
                
                for batch_start in range(0, total_transactions, batch_size):
                    batch_end = min(batch_start + batch_size, total_transactions)
                    batch = data['transactions'].iloc[batch_start:batch_end]
                    
                    async with neo4j_session.begin_transaction() as tx:
                        for idx, row in batch.iterrows():
                            try:
                                # Convert transaction data for Neo4j
                                transaction_data = {
                                    'id': str(row['transaction_id']),
                                    'account_id': str(row['account_id']),
                                    'type': str(row.get('transaction_type', 'unknown')),
                                    'date': str(row['transaction_date']),
                                    'amount': float(row['amount']),
                                    'currency': str(row.get('currency', 'USD')),
                                    'status': str(row.get('transaction_status', 'pending')),
                                    'is_debit': bool(row.get('is_debit', False)),
                                    'counterparty_account': str(row.get('counterparty_account', '')),
                                    'counterparty_name': str(row.get('counterparty_name', '')),
                                    'counterparty_bank': str(row.get('counterparty_bank', '')),
                                    'counterparty_entity_name': str(row.get('counterparty_entity_name', '')),
                                    'originating_country': str(row.get('originating_country', '')),
                                    'destination_country': str(row.get('destination_country', '')),
                                    'purpose': str(row.get('purpose', '')),
                                    'reference': str(row.get('reference_number', ''))
                                }
                                
                                await tx.run("""
                                    MATCH (a:Account {account_id: $account_id})
                                    CREATE (t:Transaction {
                                        transaction_id: $id,
                                        transaction_type: $type,
                                        transaction_date: $date,
                                        amount: $amount,
                                        currency: $currency,
                                        status: $status,
                                        is_debit: $is_debit,
                                        counterparty_account: $counterparty_account,
                                        counterparty_name: $counterparty_name,
                                        counterparty_bank: $counterparty_bank,
                                        counterparty_entity_name: $counterparty_entity_name,
                                        originating_country: $originating_country,
                                        destination_country: $destination_country,
                                        purpose: $purpose,
                                        reference_number: $reference
                                    })-[:BELONGS_TO]->(a)
                                    WITH t, $date as tdate
                                    MERGE (bd:BusinessDate {date: tdate})
                                    CREATE (t)-[:OCCURRED_ON]->(bd)
                                """, transaction_data)
                                success_count += 1
                            except Exception as e:
                                print(f"Error saving transaction {row['transaction_id']} to Neo4j: {str(e)}")
                                # Continue with the next transaction in case of error
                                continue
                        
                        # Commit the batch
                        await tx.commit()
                    
                    print(f"Progress: Saved {success_count}/{total_transactions} transactions ({(batch_end/total_transactions)*100:.1f}%)")
                
                print(f"✓ Completed saving {success_count}/{total_transactions} transactions to Neo4j")

    async def close(self):
        """Close database connection."""
        if self.driver:
            await self.driver.close()

class DatabaseManager:
    def __init__(self):
        """Initialize database connections."""
        self.engine = None
        self.driver = None
        
        # Load environment variables
        self.pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_port = os.getenv('POSTGRES_PORT', '5432')
        self.pg_db = os.getenv('POSTGRES_DB', 'aml_monitoring')
        self.pg_user = os.getenv('POSTGRES_USER', 'postgres')
        self.pg_password = os.getenv('POSTGRES_PASSWORD', '')

        self.neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
        self.neo4j_password = os.getenv('NEO4J_PASSWORD', '')

    async def initialize(self):
        """Initialize database connections."""
        # Initialize PostgreSQL
        db_url = f'postgresql+asyncpg://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}'
        self.engine = create_async_engine(db_url, echo=True)
        
        # Initialize Neo4j
        self.driver = AsyncGraphDatabase.driver(
            self.neo4j_uri,
            auth=(self.neo4j_user, self.neo4j_password)
        )

    async def cleanup_postgres(self):
        """Clean up PostgreSQL tables."""
        if not self.engine:
            await self.initialize()
            
        tables = [
            'institutions',
            'subsidiaries',
            'accounts',
            'transactions',
            'beneficial_owners',
            'addresses',
            'compliance_events',
            'jurisdiction_presence',
            'customers',
            'risk_assessments',
            'authorized_persons',
            'documents'
        ]
        
        async with self.engine.begin() as conn:
            print("\nDropping tables in PostgreSQL...")
            for table in tables:
                try:
                    await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    print(f"✓ Dropped table {table}")
                except Exception as e:
                    print(f"Error dropping table {table}: {str(e)}")
            
            print("\nCreating tables...")
            # Create tables in order of dependencies
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS institutions (
                    institution_id UUID PRIMARY KEY,
                    legal_name VARCHAR(255) NOT NULL,
                    business_type VARCHAR(50) NOT NULL,
                    incorporation_country VARCHAR(2) NOT NULL,
                    incorporation_date DATE NOT NULL,
                    onboarding_date DATE NOT NULL,
                    operational_status VARCHAR(20) NOT NULL,
                    regulatory_status VARCHAR(50),
                    licenses JSONB,
                    industry_codes JSONB,
                    public_company BOOLEAN,
                    stock_symbol VARCHAR(10),
                    stock_exchange VARCHAR(50),
                    kyc_refresh_date DATE,
                    last_audit_date DATE
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS subsidiaries (
                    subsidiary_id UUID PRIMARY KEY,
                    institution_id UUID NOT NULL REFERENCES institutions(institution_id),
                    legal_name VARCHAR(255) NOT NULL,
                    business_type VARCHAR(50) NOT NULL,
                    incorporation_country VARCHAR(2) NOT NULL,
                    incorporation_date DATE NOT NULL,
                    operational_status VARCHAR(20) NOT NULL,
                    parent_ownership_percentage DECIMAL NOT NULL,
                    local_licenses JSONB,
                    financial_metrics JSONB,
                    industry_codes JSONB
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    account_type VARCHAR(50) NOT NULL,
                    account_status VARCHAR(20) NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    opening_date DATE NOT NULL,
                    closing_date DATE,
                    last_transaction_date DATE,
                    risk_rating VARCHAR(20) NOT NULL,
                    purpose VARCHAR(255) NOT NULL
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    account_id UUID NOT NULL,
                    transaction_type VARCHAR(50) NOT NULL,
                    transaction_date DATE NOT NULL,
                    amount DECIMAL NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    transaction_status VARCHAR(20) NOT NULL,
                    is_debit BOOLEAN NOT NULL,
                    counterparty_account VARCHAR(50),
                    counterparty_name VARCHAR(255),
                    counterparty_bank VARCHAR(255),
                    counterparty_entity_name VARCHAR(255),
                    originating_country VARCHAR(2),
                    destination_country VARCHAR(2),
                    purpose VARCHAR(255),
                    reference_number VARCHAR(50)
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS beneficial_owners (
                    owner_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    nationality VARCHAR(2) NOT NULL,
                    country_of_residence VARCHAR(2) NOT NULL,
                    ownership_percentage DECIMAL NOT NULL,
                    dob DATE NOT NULL,
                    verification_date DATE NOT NULL,
                    pep_status BOOLEAN NOT NULL,
                    sanctions_status BOOLEAN NOT NULL,
                    adverse_media_status BOOLEAN NOT NULL,
                    verification_source VARCHAR(50) NOT NULL,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS addresses (
                    address_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    address_type VARCHAR(50) NOT NULL,
                    street_address TEXT NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    state VARCHAR(100),
                    postal_code VARCHAR(20),
                    country VARCHAR(2) NOT NULL,
                    verification_date DATE,
                    verification_source VARCHAR(50)
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS compliance_events (
                    event_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    event_date DATE NOT NULL,
                    event_description TEXT NOT NULL,
                    old_state VARCHAR(50),
                    new_state VARCHAR(50),
                    decision VARCHAR(50) NOT NULL,
                    decision_date DATE NOT NULL,
                    decision_maker VARCHAR(100) NOT NULL,
                    next_review_date DATE
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS jurisdiction_presence (
                    presence_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    country VARCHAR(2) NOT NULL,
                    presence_type VARCHAR(50) NOT NULL,
                    registration_number VARCHAR(100),
                    registration_date DATE,
                    expiry_date DATE,
                    regulatory_status VARCHAR(50),
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS risk_assessments (
                    assessment_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    assessment_date DATE NOT NULL,
                    risk_rating VARCHAR(50) NOT NULL,
                    risk_score VARCHAR(20) NOT NULL,
                    assessment_type VARCHAR(50) NOT NULL,
                    risk_factors JSONB NOT NULL,
                    conducted_by VARCHAR(100) NOT NULL,
                    approved_by VARCHAR(100) NOT NULL,
                    findings TEXT NOT NULL,
                    assessor VARCHAR(100) NOT NULL,
                    next_review_date DATE NOT NULL,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS authorized_persons (
                    person_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    authorization_level VARCHAR(50) NOT NULL,
                    authorization_type VARCHAR(50) NOT NULL,
                    authorization_start DATE NOT NULL,
                    authorization_end DATE,
                    contact_info JSONB,
                    is_active BOOLEAN NOT NULL DEFAULT true,
                    last_verification_date DATE,
                    notes TEXT
                )
            """))
            
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    document_type VARCHAR(50) NOT NULL,
                    document_number VARCHAR(100) NOT NULL,
                    issuing_country VARCHAR(2) NOT NULL,
                    issuing_authority VARCHAR(100) NOT NULL,
                    issue_date DATE NOT NULL,
                    expiry_date DATE,
                    verification_status VARCHAR(50) NOT NULL,
                    verification_date DATE NOT NULL,
                    document_category VARCHAR(50) NOT NULL,
                    notes TEXT
                )
            """))
            
            print("✓ All tables created successfully")
    
    async def cleanup_neo4j(self):
        """Clean up Neo4j database."""
        async with self.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
            await session.run("CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.institution_id IS UNIQUE")
            await session.run("CREATE CONSTRAINT subsidiary_id IF NOT EXISTS FOR (s:Subsidiary) REQUIRE s.subsidiary_id IS UNIQUE")
            await session.run("CREATE CONSTRAINT country_code IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE")
            await session.run("CREATE CONSTRAINT business_date IF NOT EXISTS FOR (bd:BusinessDate) REQUIRE bd.date IS UNIQUE")
            print("✓ Cleaned up Neo4j database and created constraints")
            
            # Create constraints
            constraints = [
                "CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.entity_id IS UNIQUE",
                "CREATE CONSTRAINT subsidiary_id IF NOT EXISTS FOR (s:Subsidiary) REQUIRE s.subsidiary_id IS UNIQUE",
                "CREATE CONSTRAINT risk_assessment_id IF NOT EXISTS FOR (ra:RiskAssessment) REQUIRE ra.assessment_id IS UNIQUE",
                "CREATE CONSTRAINT authorized_person_id IF NOT EXISTS FOR (ap:AuthorizedPerson) REQUIRE ap.person_id IS UNIQUE",
                "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.document_id IS UNIQUE",
                "CREATE CONSTRAINT country_code IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    await session.run(constraint)
                except Exception as e:
                    print(f"Error creating constraint: {e}")

    async def save_to_postgres(self, data: Dict[str, pd.DataFrame]):
        """Save data to PostgreSQL database."""
        print("\n=== Saving Data to PostgreSQL ===")
        async with self.engine.begin() as conn:
            # Helper function to convert dates
            def convert_dates(df, date_columns):
                df = df.copy()
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                return df

            # Helper function to process JSON columns
            def process_json_columns(df, json_columns):
                for col in json_columns:
                    if col in df.columns:
                        df[col] = df[col].map(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                return df

            # Save institutions
            if 'institutions' in data and not data['institutions'].empty:
                print("Processing institutions...")
                try:
                    df = data['institutions'].copy()
                    df = convert_dates(df, ['incorporation_date'])
                    df = process_json_columns(df, ['licenses', 'industry_codes'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO institutions
                        SELECT * FROM jsonb_populate_recordset(null::institutions, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print("✓ Saved institutions")
                except Exception as e:
                    print(f"Error saving to institutions: {str(e)}")
                    raise
            
            # Save subsidiaries
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Processing subsidiaries...")
                try:
                    df = data['subsidiaries'].copy()
                    df = convert_dates(df, ['incorporation_date', 'customer_onboarding_date'])
                    df = process_json_columns(df, ['local_licenses', 'financial_metrics', 'industry_codes'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    # Rename parent_institution_id to institution_id to match our schema
                    df_dict = [{**item, 'institution_id': item.pop('parent_institution_id')} for item in df_dict]
                    
                    await conn.execute(text("""
                        INSERT INTO subsidiaries
                        SELECT * FROM jsonb_populate_recordset(null::subsidiaries, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} subsidiaries")
                except Exception as e:
                    print(f"Error saving to subsidiaries: {str(e)}")
                    raise

            # Save risk assessments
            if 'risk_assessments' in data and not data['risk_assessments'].empty:
                print("Processing risk_assessments...")
                try:
                    df = data['risk_assessments'].copy()
                    df = convert_dates(df, ['assessment_date', 'next_review_date'])
                    df = process_json_columns(df, ['risk_factors', 'assessment_details'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO risk_assessments
                        SELECT * FROM jsonb_populate_recordset(null::risk_assessments, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print("✓ Saved risk assessments")
                except Exception as e:
                    print(f"Error saving to risk_assessments: {str(e)}")
                    raise

            # Save authorized persons
            if 'authorized_persons' in data and not data['authorized_persons'].empty:
                print("Processing authorized_persons...")
                try:
                    df = data['authorized_persons'].copy()
                    
                    # Ensure required fields have default values
                    df['title'] = df['title'].fillna('Default Title')  # Set default title
                    df['authorization_level'] = df['authorization_level'].fillna('view_only')  # Set default auth level
                    df['authorization_type'] = df['authorization_type'].fillna('standard')  # Set default auth type
                    df['is_active'] = df['is_active'].fillna(True)  # Set default to active
                    
                    # Convert dates
                    df = convert_dates(df, ['authorization_start', 'authorization_end', 'last_verification_date'])
                    
                    # Process JSON columns
                    df = process_json_columns(df, ['contact_info'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO authorized_persons
                        SELECT * FROM jsonb_populate_recordset(null::authorized_persons, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} authorized persons")
                except Exception as e:
                    print(f"Error saving to authorized_persons: {str(e)}")
                    raise

            # Save documents
            if 'documents' in data and not data['documents'].empty:
                print("Processing documents...")
                try:
                    df = data['documents'].copy()
                    
                    # Ensure required fields have default values
                    df['verification_status'] = df['verification_status'].fillna('pending')  # Set default verification status
                    df['document_category'] = df['document_category'].fillna('general')  # Set default document category
                    
                    # Convert dates
                    df = convert_dates(df, ['issue_date', 'expiry_date', 'verification_date'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO documents
                        SELECT * FROM jsonb_populate_recordset(null::documents, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} documents")
                except Exception as e:
                    print(f"Error saving to documents: {str(e)}")
                    raise

            # Save accounts
            if 'accounts' in data and not data['accounts'].empty:
                print("Saving accounts to Neo4j...")
                total_accounts = len(data['accounts'])
                success_count = 0
                
                for idx, row in data['accounts'].iterrows():
                    try:
                        account_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['account_id']),
                            'type': str(row['account_type']),
                            'number': str(row['account_number']),
                            'currency': str(row['currency']),
                            'status': str(row['status']),
                            'opening_date': str(row['opening_date']),
                            'last_activity': str(row['last_activity_date']),
                            'balance': float(row['balance']),
                            'risk_rating': str(row['risk_rating']),
                            'purpose': str(row['purpose']) if pd.notna(row['purpose']) else None
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Account {
                                account_id: $id,
                                account_type: $type,
                                account_number: $number,
                                currency: $currency,
                                status: $status,
                                opening_date: $opening_date,
                                last_activity_date: $last_activity,
                                balance: $balance,
                                risk_rating: $risk_rating,
                                purpose: $purpose
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ACCOUNT]->(a)
                            WITH a, $opening_date as odate
                            MERGE (bd:BusinessDate {date: odate})
                            CREATE (a)-[:OPENED_ON]->(bd)
                        """, account_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving account {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_accounts} accounts")

            # Save beneficial owners
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                total_owners = len(data['beneficial_owners'])
                success_count = 0
                
                for idx, row in data['beneficial_owners'].iterrows():
                    try:
                        owner_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['owner_id']),
                            'name': str(row['name']),
                            'nationality': str(row['nationality']),
                            'residence': str(row['country_of_residence']),
                            'ownership': float(row['ownership_percentage']),
                            'dob': str(row['dob']),
                            'verification_date': str(row['verification_date']),
                            'pep': bool(row['pep_status']),
                            'sanctions': bool(row['sanctions_status']),
                            'adverse_media': bool(row['adverse_media_status'])
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (bo:BeneficialOwner {
                                owner_id: $id,
                                name: $name,
                                nationality: $nationality,
                                country_of_residence: $residence,
                                ownership_percentage: $ownership,
                                dob: $dob,
                                verification_date: $verification_date,
                                pep_status: $pep,
                                sanctions_status: $sanctions,
                                adverse_media_status: $adverse_media
                            })
                            WITH e, bo
                            CREATE (e)-[:HAS_BENEFICIAL_OWNER]->(bo)
                            WITH bo, $nationality as nat
                            MERGE (c:Country {code: nat})
                            CREATE (bo)-[:CITIZEN_OF]->(c)
                        """, owner_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving beneficial owner {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_owners} beneficial owners")

            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                
                for idx, row in data['addresses'].iterrows():
                    try:
                        address_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['address_id']),
                            'type': str(row['address_type']),
                            'line1': str(row['address_line1']),
                            'line2': str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                            'city': str(row['city']),
                            'state': str(row['state_province']),
                            'postal': str(row['postal_code']),
                            'country': str(row['country']),
                            'status': str(row['status']),
                            'from_date': str(row['effective_from']),
                            'to_date': str(row['effective_to']) if pd.notna(row['effective_to']) else None,
                            'primary': bool(row['primary_address'])
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Address {
                                address_id: $id,
                                address_type: $type,
                                address_line1: $line1,
                                address_line2: $line2,
                                city: $city,
                                state_province: $state,
                                postal_code: $postal,
                                country: $country,
                                status: $status,
                                effective_from: $from_date,
                                effective_to: $to_date,
                                primary_address: $primary
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ADDRESS]->(a)
                            WITH a, $country as c
                            MERGE (country:Country {code: c})
                            CREATE (a)-[:LOCATED_IN]->(country)
                        """, address_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving address {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_addresses} addresses")

            # Save compliance events
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Saving compliance events to Neo4j...")
                total_events = len(data['compliance_events'])
                success_count = 0
                
                for idx, row in data['compliance_events'].iterrows():
                    try:
                        event_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['event_id']),
                            'date': str(row['event_date']),
                            'type': str(row['event_type']),
                            'description': str(row['event_description']),
                            'old_state': str(row['old_state']) if pd.notna(row['old_state']) else None,
                            'new_state': str(row['new_state']),
                            'decision': str(row['decision']) if pd.notna(row['decision']) else None,
                            'decision_date': str(row['decision_date']) if pd.notna(row['decision_date']) else None,
                            'decision_maker': str(row['decision_maker']) if pd.notna(row['decision_maker']) else None,
                            'next_review': str(row['next_review_date']) if pd.notna(row['next_review_date']) else None
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (ce:ComplianceEvent {
                                event_id: $id,
                                event_date: $date,
                                event_type: $type,
                                event_description: $description,
                                old_state: $old_state,
                                new_state: $new_state,
                                decision: $decision,
                                decision_date: $decision_date,
                                decision_maker: $decision_maker,
                                next_review_date: $next_review
                            })
                            WITH e, ce
                            CREATE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                            WITH ce, $date as edate
                            MERGE (bd:BusinessDate {date: edate})
                            CREATE (ce)-[:OCCURRED_ON]->(bd)
                        """, event_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving compliance event {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_events} compliance events")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                total_transactions = len(data['transactions'])
                success_count = 0
                
                for idx, row in data['transactions'].iterrows():
                    try:
                        # Convert transaction data for Neo4j
                        transaction_data = {
                            'id': str(row['transaction_id']),
                            'account_id': str(row['account_id']),
                            'type': str(row.get('transaction_type', 'unknown')),
                            'date': str(row['transaction_date']),
                            'amount': float(row['amount']),
                            'currency': str(row.get('currency', 'USD')),
                            'status': str(row.get('transaction_status', 'pending')),
                            'is_debit': bool(row.get('is_debit', False)),
                            'counterparty_account': str(row.get('counterparty_account', '')),
                            'counterparty_name': str(row.get('counterparty_name', '')),
                            'counterparty_bank': str(row.get('counterparty_bank', '')),
                            'counterparty_entity_name': str(row.get('counterparty_entity_name', '')),
                            'originating_country': str(row.get('originating_country', '')),
                            'destination_country': str(row.get('destination_country', '')),
                            'purpose': str(row.get('purpose', '')),
                            'reference': str(row.get('reference_number', ''))
                        }
                        
                        await session.run("""
                            MATCH (a:Account {account_id: $account_id})
                            CREATE (t:Transaction {
                                transaction_id: $id,
                                transaction_type: $type,
                                transaction_date: $date,
                                amount: $amount,
                                currency: $currency,
                                status: $status,
                                is_debit: $is_debit,
                                counterparty_account: $counterparty_account,
                                counterparty_name: $counterparty_name,
                                counterparty_bank: $counterparty_bank,
                                counterparty_entity_name: $counterparty_entity_name,
                                originating_country: $originating_country,
                                destination_country: $destination_country,
                                purpose: $purpose,
                                reference_number: $reference
                            })-[:BELONGS_TO]->(a)
                            WITH t, $date as tdate
                            MERGE (bd:BusinessDate {date: tdate})
                            CREATE (t)-[:OCCURRED_ON]->(bd)
                        """, transaction_data)
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving transaction {row['transaction_id']} to Neo4j: {str(e)}")
                print(f"✓ Saved {success_count}/{total_transactions} transactions to Neo4j")

    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j database."""
        print("\n=== Saving Data to Neo4j ===")
        async with self.driver.session() as neo4j_session:
            # Clean up existing data
            await neo4j_session.run("MATCH (n) DETACH DELETE n")
            await neo4j_session.run("CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.institution_id IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT subsidiary_id IF NOT EXISTS FOR (s:Subsidiary) REQUIRE s.subsidiary_id IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT country_code IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE")
            await neo4j_session.run("CREATE CONSTRAINT business_date IF NOT EXISTS FOR (bd:BusinessDate) REQUIRE bd.date IS UNIQUE")
            print("✓ Cleaned up Neo4j database")
            
            # Save institutions
            if 'institutions' in data and not data['institutions'].empty:
                print("Saving institutions to Neo4j...")
                for _, row in data['institutions'].iterrows():
                    try:
                        await neo4j_session.run("""
                            CREATE (i:Institution {
                                institution_id: $id,
                                legal_name: $name,
                                business_type: $business_type,
                                incorporation_country: $country,
                                incorporation_date: $inc_date,
                                operational_status: $status,
                                regulatory_status: $reg_status,
                                licenses: $licenses,
                                industry_codes: $industry_codes,
                                public_company: $public,
                                stock_symbol: $symbol,
                                stock_exchange: $exchange
                            })
                            WITH i
                            MERGE (c:Country {code: $country})
                            MERGE (i)-[:INCORPORATED_IN]->(c)
                            WITH i
                            MERGE (bd:BusinessDate {date: $inc_date})
                            MERGE (i)-[:INCORPORATED_ON]->(bd)
                        """, {
                            'id': str(row['institution_id']),
                            'name': row['legal_name'],
                            'business_type': row['business_type'],
                            'country': row['incorporation_country'],
                            'inc_date': row['incorporation_date'],
                            'status': row['operational_status'],
                            'reg_status': row['regulatory_status'],
                            'licenses': json.dumps(row['licenses']),
                            'industry_codes': json.dumps(row['industry_codes']),
                            'public': row['public_company'],
                            'symbol': row['stock_symbol'],
                            'exchange': row['stock_exchange']
                        })
                    except Exception as e:
                        print(f"Error saving institution {row['legal_name']}: {e}")
                print("✓ Saved institutions")
            
            # Save subsidiaries
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Saving subsidiaries to Neo4j...")
                for _, row in data['subsidiaries'].iterrows():
                    try:
                        # Convert data to native Python types
                        subsidiary_data = {
                            'id': str(row['subsidiary_id']),
                            'parent_id': str(row['parent_institution_id']),
                            'name': str(row['legal_name']),
                            'business_type': str(row['business_type']) if pd.notna(row['business_type']) else None,
                            'country': str(row['incorporation_country']) if pd.notna(row['incorporation_country']) else None,
                            'inc_date': str(row['incorporation_date']) if pd.notna(row['incorporation_date']) else None,
                            'status': str(row['operational_status']) if pd.notna(row['operational_status']) else None,
                            'reg_status': str(row['regulatory_status']) if pd.notna(row['regulatory_status']) else None,
                            'licenses': row['local_licenses'] if isinstance(row['local_licenses'], list) else [],
                            'financial_metrics': json.dumps(row['financial_metrics']) if isinstance(row['financial_metrics'], dict) else "{}",
                            'is_regulated': bool(row['is_regulated']) if pd.notna(row['is_regulated']) else False,
                            'is_customer': bool(row['is_customer']) if pd.notna(row['is_customer']) else False,
                            'customer_id': str(row['customer_id']) if pd.notna(row['customer_id']) else None,
                            'onboarding_date': str(row['customer_onboarding_date']) if pd.notna(row['customer_onboarding_date']) else None,
                            'risk_rating': str(row['customer_risk_rating']) if pd.notna(row['customer_risk_rating']) else None,
                            'customer_status': str(row['customer_status']) if pd.notna(row['customer_status']) else None,
                            'industry_codes': row['industry_codes'] if isinstance(row['industry_codes'], list) else []
                        }
                        
                        await neo4j_session.run("""
                            MATCH (p:Institution {institution_id: $parent_id})
                            CREATE (s:Subsidiary {subsidiary_id: $id})
                            SET s.legal_name = $name,
                                s.business_type = $business_type,
                                s.incorporation_country = $country,
                                s.incorporation_date = $inc_date,
                                s.operational_status = $status,
                                s.regulatory_status = $reg_status,
                                s.local_licenses = $licenses,
                                s.financial_metrics = $financial_metrics,
                                s.industry_codes = $industry_codes,
                                s.is_regulated = $is_regulated,
                                s.is_customer = $is_customer,
                                s.customer_id = $customer_id,
                                s.customer_onboarding_date = $onboarding_date,
                                s.customer_risk_rating = $risk_rating,
                                s.customer_status = $customer_status
                            WITH p, s
                            CREATE (p)-[:OWNS]->(s)
                            WITH s
                            MERGE (c:Country {code: $country})
                            MERGE (s)-[:INCORPORATED_IN]->(c)
                            WITH s
                            MERGE (bd:BusinessDate {date: $inc_date})
                            MERGE (s)-[:INCORPORATED_ON]->(bd)
                        """, subsidiary_data)
                    except Exception as e:
                        print(f"Error saving subsidiary {row['legal_name']}: {e}")
                print("✓ Saved subsidiaries")
            
            # Save risk assessments
            if 'risk_assessments' in data and not data['risk_assessments'].empty:
                print("Saving risk assessments to Neo4j...")
                total_assessments = len(data['risk_assessments'])
                success_count = 0
                
                for idx, row in data['risk_assessments'].iterrows():
                    try:
                        # Convert dates to string format
                        assessment_date = pd.to_datetime(row['assessment_date']).strftime('%Y-%m-%d') if pd.notnull(row['assessment_date']) else None
                        next_review_date = pd.to_datetime(row['next_review_date']).strftime('%Y-%m-%d') if pd.notnull(row['next_review_date']) else None
                        
                        # Convert risk_factors to JSON string
                        risk_factors = json.dumps(row['risk_factors']) if isinstance(row['risk_factors'], (list, dict)) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (ra:RiskAssessment {
                                assessment_id: $id,
                                assessment_date: $assessment_date,
                                risk_rating: $risk_rating,
                                risk_score: $risk_score,
                                assessment_type: $assessment_type,
                                risk_factors: $risk_factors,
                                conducted_by: $conducted_by,
                                approved_by: $approved_by,
                                findings: $findings,
                                assessor: $assessor,
                                next_review_date: $next_review_date,
                                notes: $notes
                            })
                            CREATE (owner)-[:HAS_RISK_ASSESSMENT]->(ra)
                            WITH ra, $assessment_date as adate
                            WHERE adate IS NOT NULL
                            MERGE (bd:BusinessDate {date: adate})
                            CREATE (ra)-[:ASSESSED_ON]->(bd)
                            WITH ra, $next_review_date as rdate
                            WHERE rdate IS NOT NULL
                            MERGE (bd2:BusinessDate {date: rdate})
                            CREATE (ra)-[:NEXT_REVIEW_ON]->(bd2)
                        """, {
                            'id': str(row['assessment_id']),
                            'entity_id': str(row['entity_id']),
                            'assessment_date': assessment_date,
                            'risk_rating': str(row['risk_rating']),
                            'risk_score': str(row['risk_score']),
                            'assessment_type': str(row['assessment_type']),
                            'risk_factors': risk_factors,
                            'conducted_by': str(row['conducted_by']) if pd.notnull(row['conducted_by']) else None,
                            'approved_by': str(row['approved_by']) if pd.notnull(row['approved_by']) else None,
                            'findings': str(row['findings']) if pd.notnull(row['findings']) else None,
                            'assessor': str(row['assessor']) if pd.notnull(row['assessor']) else None,
                            'next_review_date': next_review_date,
                            'notes': str(row['notes']) if pd.notnull(row['notes']) else None
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving risk assessment {row['assessment_id']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_assessments} risk assessments")
            
            # Save authorized persons
            if 'authorized_persons' in data and not data['authorized_persons'].empty:
                print("Saving authorized persons to Neo4j...")
                total_persons = len(data['authorized_persons'])
                success_count = 0
                
                for idx, row in data['authorized_persons'].iterrows():
                    try:
                        # Convert dates to string format
                        auth_start = pd.to_datetime(row['authorization_start']).strftime('%Y-%m-%d') if pd.notnull(row['authorization_start']) else None
                        auth_end = pd.to_datetime(row['authorization_end']).strftime('%Y-%m-%d') if pd.notnull(row['authorization_end']) else None
                        verification_date = pd.to_datetime(row['last_verification_date']).strftime('%Y-%m-%d') if pd.notnull(row['last_verification_date']) else None
                        
                        # Convert contact_info to JSON string
                        contact_info = json.dumps(row['contact_info']) if isinstance(row['contact_info'], (dict, str)) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (ap:AuthorizedPerson {
                                person_id: $id,
                                name: $name,
                                title: $title,
                                authorization_level: $auth_level,
                                authorization_type: $auth_type,
                                authorization_start: $auth_start,
                                authorization_end: $auth_end,
                                contact_info: $contact_info,
                                is_active: $is_active,
                                last_verification_date: $verification_date
                            })
                            CREATE (owner)-[:HAS_AUTHORIZED_PERSON]->(ap)
                            WITH ap, $auth_start as adate
                            WHERE adate IS NOT NULL
                            MERGE (bd:BusinessDate {date: adate})
                            CREATE (ap)-[:AUTHORIZED_ON]->(bd)
                        """, {
                            'id': str(row['person_id']),
                            'entity_id': str(row['entity_id']),
                            'name': str(row['name']),
                            'title': str(row['title']) if pd.notnull(row['title']) else None,
                            'auth_level': str(row['authorization_level']),
                            'auth_type': str(row['authorization_type']),
                            'auth_start': auth_start,
                            'auth_end': auth_end,
                            'contact_info': contact_info,
                            'is_active': bool(row['is_active']),
                            'verification_date': verification_date
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving authorized person {row['name']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_persons} authorized persons")
            
            # Save documents
            if 'documents' in data and not data['documents'].empty:
                print("Saving documents to Neo4j...")
                total_documents = len(data['documents'])
                success_count = 0
                
                for idx, row in data['documents'].iterrows():
                    try:
                        # Convert dates to string format
                        issue_date = pd.to_datetime(row['issue_date']).strftime('%Y-%m-%d') if pd.notnull(row['issue_date']) else None
                        expiry_date = pd.to_datetime(row['expiry_date']).strftime('%Y-%m-%d') if pd.notnull(row['expiry_date']) else None
                        verification_date = pd.to_datetime(row['verification_date']).strftime('%Y-%m-%d') if pd.notnull(row['verification_date']) else None
                        
                        await neo4j_session.run("""
                            MATCH (owner) 
                            WHERE (owner:Institution OR owner:Subsidiary) AND 
                                  (owner.institution_id = $entity_id OR owner.subsidiary_id = $entity_id)
                            CREATE (doc:Document {
                                document_id: $id,
                                document_type: $type,
                                document_number: $number,
                                issuing_authority: $authority,
                                issue_date: $issue_date,
                                expiry_date: $expiry_date,
                                document_category: $category,
                                verification_status: $verification_status,
                                verification_date: $verification_date,
                                notes: $notes
                            })
                            CREATE (owner)-[:HAS_DOCUMENT]->(doc)
                            WITH doc, $issue_date as idate
                            WHERE idate IS NOT NULL
                            MERGE (bd:BusinessDate {date: idate})
                            CREATE (doc)-[:ISSUED_ON]->(bd)
                        """, {
                            'id': str(row['document_id']),
                            'entity_id': str(row['entity_id']),
                            'type': str(row['document_type']),
                            'number': str(row['document_number']) if pd.notnull(row['document_number']) else None,
                            'authority': str(row['issuing_authority']) if pd.notnull(row['issuing_authority']) else None,
                            'issue_date': issue_date,
                            'expiry_date': expiry_date,
                            'category': str(row['document_category']),
                            'verification_status': str(row['verification_status']) if pd.notnull(row['verification_status']) else None,
                            'verification_date': verification_date,
                            'notes': str(row['notes']) if pd.notnull(row['notes']) else None
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving document {row['document_id']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_documents} documents")

            # Save accounts
            if 'accounts' in data and not data['accounts'].empty:
                print("Saving accounts to Neo4j...")
                total_accounts = len(data['accounts'])
                success_count = 0
                
                async with self.driver.session() as neo4j_session:
                    for idx, row in data['accounts'].iterrows():
                        try:
                            account_data = {
                                'entity_id': str(row['entity_id']),
                                'id': str(row['account_id']),
                                'type': str(row['account_type']),
                                'number': str(row['account_number']),
                                'currency': str(row['currency']),
                                'status': str(row['status']),
                                'opening_date': str(row['opening_date']),
                                'last_activity': str(row['last_activity_date']),
                                'balance': float(row['balance']),
                                'risk_rating': str(row['risk_rating']),
                                'purpose': str(row['purpose']) if pd.notna(row['purpose']) else None
                            }
                            
                            await neo4j_session.run("""
                                MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                                CREATE (a:Account {
                                    account_id: $id,
                                    account_type: $type,
                                    account_number: $number,
                                    currency: $currency,
                                    status: $status,
                                    opening_date: $opening_date,
                                    last_activity_date: $last_activity,
                                    balance: $balance,
                                    risk_rating: $risk_rating,
                                    purpose: $purpose
                                })
                                WITH e, a
                                CREATE (e)-[:HAS_ACCOUNT]->(a)
                                WITH a, $opening_date as odate
                                MERGE (bd:BusinessDate {date: odate})
                                CREATE (a)-[:OPENED_ON]->(bd)
                            """, account_data)
                            success_count += 1
                        except Exception as e:
                            print(f"\nError saving account {idx}: {e}")
                    print(f"✓ Saved {success_count}/{total_accounts} accounts")
            
            # Save beneficial owners
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                total_owners = len(data['beneficial_owners'])
                success_count = 0
                
                for idx, row in data['beneficial_owners'].iterrows():
                    try:
                        owner_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['owner_id']),
                            'name': str(row['name']),
                            'nationality': str(row['nationality']),
                            'residence': str(row['country_of_residence']),
                            'ownership': float(row['ownership_percentage']),
                            'dob': str(row['dob']),
                            'verification_date': str(row['verification_date']),
                            'pep': bool(row['pep_status']),
                            'sanctions': bool(row['sanctions_status']),
                            'adverse_media': bool(row['adverse_media_status'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (bo:BeneficialOwner {
                                owner_id: $id,
                                name: $name,
                                nationality: $nationality,
                                country_of_residence: $residence,
                                ownership_percentage: $ownership,
                                dob: $dob,
                                verification_date: $verification_date,
                                pep_status: $pep,
                                sanctions_status: $sanctions,
                                adverse_media_status: $adverse_media
                            })
                            WITH e, bo
                            CREATE (e)-[:HAS_BENEFICIAL_OWNER]->(bo)
                            WITH bo, $nationality as nat
                            MERGE (c:Country {code: nat})
                            CREATE (bo)-[:CITIZEN_OF]->(c)
                        """, owner_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving beneficial owner {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_owners} beneficial owners")

            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                
                for idx, row in data['addresses'].iterrows():
                    try:
                        address_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['address_id']),
                            'type': str(row['address_type']),
                            'line1': str(row['address_line1']),
                            'line2': str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                            'city': str(row['city']),
                            'state': str(row['state_province']),
                            'postal': str(row['postal_code']),
                            'country': str(row['country']),
                            'status': str(row['status']),
                            'from_date': str(row['effective_from']),
                            'to_date': str(row['effective_to']) if pd.notna(row['effective_to']) else None,
                            'primary': bool(row['primary_address'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Address {
                                address_id: $id,
                                address_type: $type,
                                address_line1: $line1,
                                address_line2: $line2,
                                city: $city,
                                state_province: $state,
                                postal_code: $postal,
                                country: $country,
                                status: $status,
                                effective_from: $from_date,
                                effective_to: $to_date,
                                primary_address: $primary
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ADDRESS]->(a)
                            WITH a, $country as c
                            MERGE (country:Country {code: c})
                            CREATE (a)-[:LOCATED_IN]->(country)
                        """, address_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving address {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_addresses} addresses")

            # Save compliance events
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Saving compliance events to Neo4j...")
                total_events = len(data['compliance_events'])
                success_count = 0
                
                for idx, row in data['compliance_events'].iterrows():
                    try:
                        event_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['event_id']),
                            'date': str(row['event_date']),
                            'type': str(row['event_type']),
                            'description': str(row['event_description']),
                            'old_state': str(row['old_state']) if pd.notna(row['old_state']) else None,
                            'new_state': str(row['new_state']),
                            'decision': str(row['decision']) if pd.notna(row['decision']) else None,
                            'decision_date': str(row['decision_date']) if pd.notna(row['decision_date']) else None,
                            'decision_maker': str(row['decision_maker']) if pd.notna(row['decision_maker']) else None,
                            'next_review': str(row['next_review_date']) if pd.notna(row['next_review_date']) else None
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (ce:ComplianceEvent {
                                event_id: $id,
                                event_date: $date,
                                event_type: $type,
                                event_description: $description,
                                old_state: $old_state,
                                new_state: $new_state,
                                decision: $decision,
                                decision_date: $decision_date,
                                decision_maker: $decision_maker,
                                next_review_date: $next_review
                            })
                            WITH e, ce
                            CREATE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                            WITH ce, $date as edate
                            MERGE (bd:BusinessDate {date: edate})
                            CREATE (ce)-[:OCCURRED_ON]->(bd)
                        """, event_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving compliance event {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_events} compliance events")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                total_transactions = len(data['transactions'])
                success_count = 0
                batch_size = 100  # Process transactions in batches
                
                for batch_start in range(0, total_transactions, batch_size):
                    batch_end = min(batch_start + batch_size, total_transactions)
                    batch = data['transactions'].iloc[batch_start:batch_end]
                    
                    async with neo4j_session.begin_transaction() as tx:
                        for idx, row in batch.iterrows():
                            try:
                                # Convert transaction data for Neo4j
                                transaction_data = {
                                    'id': str(row['transaction_id']),
                                    'account_id': str(row['account_id']),
                                    'type': str(row.get('transaction_type', 'unknown')),
                                    'date': str(row['transaction_date']),
                                    'amount': float(row['amount']),
                                    'currency': str(row.get('currency', 'USD')),
                                    'status': str(row.get('transaction_status', 'pending')),
                                    'is_debit': bool(row.get('is_debit', False)),
                                    'counterparty_account': str(row.get('counterparty_account', '')),
                                    'counterparty_name': str(row.get('counterparty_name', '')),
                                    'counterparty_bank': str(row.get('counterparty_bank', '')),
                                    'counterparty_entity_name': str(row.get('counterparty_entity_name', '')),
                                    'originating_country': str(row.get('originating_country', '')),
                                    'destination_country': str(row.get('destination_country', '')),
                                    'purpose': str(row.get('purpose', '')),
                                    'reference': str(row.get('reference_number', ''))
                                }
                                
                                await tx.run("""
                                    MATCH (a:Account {account_id: $account_id})
                                    CREATE (t:Transaction {
                                        transaction_id: $id,
                                        transaction_type: $type,
                                        transaction_date: $date,
                                        amount: $amount,
                                        currency: $currency,
                                        status: $status,
                                        is_debit: $is_debit,
                                        counterparty_account: $counterparty_account,
                                        counterparty_name: $counterparty_name,
                                        counterparty_bank: $counterparty_bank,
                                        counterparty_entity_name: $counterparty_entity_name,
                                        originating_country: $originating_country,
                                        destination_country: $destination_country,
                                        purpose: $purpose,
                                        reference_number: $reference
                                    })-[:BELONGS_TO]->(a)
                                    WITH t, $date as tdate
                                    MERGE (bd:BusinessDate {date: tdate})
                                    CREATE (t)-[:OCCURRED_ON]->(bd)
                                """, transaction_data)
                                success_count += 1
                            except Exception as e:
                                print(f"Error saving transaction {row['transaction_id']} to Neo4j: {str(e)}")
                                # Continue with the next transaction in case of error
                                continue
                        
                        # Commit the batch
                        await tx.commit()
                    
                    print(f"Progress: Saved {success_count}/{total_transactions} transactions ({(batch_end/total_transactions)*100:.1f}%)")
                
                print(f"✓ Completed saving {success_count}/{total_transactions} transactions to Neo4j")

    async def close(self):
        """Close database connections."""
        if self.driver:
            await self.driver.close()
        if self.engine:
            await self.engine.dispose()

class PostgresDatabaseManager(DatabaseManager):
    async def save_to_postgres(self, data: Dict[str, pd.DataFrame]):
        """Save data to PostgreSQL."""
        if not self.engine:
            await self.initialize()

        async with self.engine.begin() as conn:
            # Helper function to convert dates
            def convert_dates(df, date_columns):
                df = df.copy()
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                return df

            # Helper function to process JSON columns
            def process_json_columns(df, json_columns):
                for col in json_columns:
                    if col in df.columns:
                        df[col] = df[col].map(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                return df

            # Save institutions
            if 'institutions' in data and not data['institutions'].empty:
                print("Processing institutions...")
                try:
                    df = data['institutions'].copy()
                    df = convert_dates(df, ['incorporation_date'])
                    df = process_json_columns(df, ['licenses', 'industry_codes'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO institutions
                        SELECT * FROM jsonb_populate_recordset(null::institutions, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print("✓ Saved institutions")
                except Exception as e:
                    print(f"Error saving to institutions: {str(e)}")
                    raise
            
            # Save subsidiaries
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Processing subsidiaries...")
                try:
                    df = data['subsidiaries'].copy()
                    df = convert_dates(df, ['incorporation_date', 'customer_onboarding_date'])
                    df = process_json_columns(df, ['local_licenses', 'financial_metrics', 'industry_codes'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    # Rename parent_institution_id to institution_id to match our schema
                    df_dict = [{**item, 'institution_id': item.pop('parent_institution_id')} for item in df_dict]
                    
                    await conn.execute(text("""
                        INSERT INTO subsidiaries
                        SELECT * FROM jsonb_populate_recordset(null::subsidiaries, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} subsidiaries")
                except Exception as e:
                    print(f"Error saving to subsidiaries: {str(e)}")
                    raise

            # Save risk assessments
            if 'risk_assessments' in data and not data['risk_assessments'].empty:
                print("Processing risk_assessments...")
                try:
                    df = data['risk_assessments'].copy()
                    df = convert_dates(df, ['assessment_date', 'next_review_date'])
                    df = process_json_columns(df, ['risk_factors', 'assessment_details'])
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO risk_assessments
                        SELECT * FROM jsonb_populate_recordset(null::risk_assessments, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print("✓ Saved risk assessments")
                except Exception as e:
                    print(f"Error saving to risk_assessments: {str(e)}")
                    raise

            # Save authorized persons
            if 'authorized_persons' in data and not data['authorized_persons'].empty:
                print("Processing authorized_persons...")
                try:
                    df = data['authorized_persons'].copy()
                    
                    # Ensure required fields have default values
                    df['title'] = df['title'].fillna('Default Title')  # Set default title
                    df['authorization_level'] = df['authorization_level'].fillna('view_only')  # Set default auth level
                    df['authorization_type'] = df['authorization_type'].fillna('standard')  # Set default auth type
                    df['is_active'] = df['is_active'].fillna(True)  # Set default to active
                    
                    # Convert dates
                    df = convert_dates(df, ['authorization_start', 'authorization_end', 'last_verification_date'])
                    
                    # Process JSON columns
                    df = process_json_columns(df, ['contact_info'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO authorized_persons
                        SELECT * FROM jsonb_populate_recordset(null::authorized_persons, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} authorized persons")
                except Exception as e:
                    print(f"Error saving to authorized_persons: {str(e)}")
                    raise

            # Save documents
            if 'documents' in data and not data['documents'].empty:
                print("Processing documents...")
                try:
                    df = data['documents'].copy()
                    
                    # Ensure required fields have default values
                    df['verification_status'] = df['verification_status'].fillna('pending')  # Set default verification status
                    df['document_category'] = df['document_category'].fillna('general')  # Set default document category
                    
                    # Convert dates
                    df = convert_dates(df, ['issue_date', 'expiry_date', 'verification_date'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO documents
                        SELECT * FROM jsonb_populate_recordset(null::documents, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} documents")
                except Exception as e:
                    print(f"Error saving to documents: {str(e)}")
                    raise

            # Save accounts
            if 'accounts' in data and not data['accounts'].empty:
                print("Saving accounts to Neo4j...")
                total_accounts = len(data['accounts'])
                success_count = 0
                
                for idx, row in data['accounts'].iterrows():
                    try:
                        account_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['account_id']),
                            'type': str(row['account_type']),
                            'number': str(row['account_number']),
                            'currency': str(row['currency']),
                            'status': str(row['status']),
                            'opening_date': str(row['opening_date']),
                            'last_activity': str(row['last_activity_date']),
                            'balance': float(row['balance']),
                            'risk_rating': str(row['risk_rating']),
                            'purpose': str(row['purpose']) if pd.notna(row['purpose']) else None
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Account {
                                account_id: $id,
                                account_type: $type,
                                account_number: $number,
                                currency: $currency,
                                status: $status,
                                opening_date: $opening_date,
                                last_activity_date: $last_activity,
                                balance: $balance,
                                risk_rating: $risk_rating,
                                purpose: $purpose
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ACCOUNT]->(a)
                            WITH a, $opening_date as odate
                            MERGE (bd:BusinessDate {date: odate})
                            CREATE (a)-[:OPENED_ON]->(bd)
                        """, account_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving account {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_accounts} accounts")

            # Save beneficial owners
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                total_owners = len(data['beneficial_owners'])
                success_count = 0
                
                for idx, row in data['beneficial_owners'].iterrows():
                    try:
                        owner_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['owner_id']),
                            'name': str(row['name']),
                            'nationality': str(row['nationality']),
                            'residence': str(row['country_of_residence']),
                            'ownership': float(row['ownership_percentage']),
                            'dob': str(row['dob']),
                            'verification_date': str(row['verification_date']),
                            'pep': bool(row['pep_status']),
                            'sanctions': bool(row['sanctions_status']),
                            'adverse_media': bool(row['adverse_media_status'])
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (bo:BeneficialOwner {
                                owner_id: $id,
                                name: $name,
                                nationality: $nationality,
                                country_of_residence: $residence,
                                ownership_percentage: $ownership,
                                dob: $dob,
                                verification_date: $verification_date,
                                pep_status: $pep,
                                sanctions_status: $sanctions,
                                adverse_media_status: $adverse_media
                            })
                            WITH e, bo
                            CREATE (e)-[:HAS_BENEFICIAL_OWNER]->(bo)
                            WITH bo, $nationality as nat
                            MERGE (c:Country {code: nat})
                            CREATE (bo)-[:CITIZEN_OF]->(c)
                        """, owner_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving beneficial owner {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_owners} beneficial owners")

            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                
                for idx, row in data['addresses'].iterrows():
                    try:
                        address_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['address_id']),
                            'type': str(row['address_type']),
                            'line1': str(row['address_line1']),
                            'line2': str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                            'city': str(row['city']),
                            'state': str(row['state_province']),
                            'postal': str(row['postal_code']),
                            'country': str(row['country']),
                            'status': str(row['status']),
                            'from_date': str(row['effective_from']),
                            'to_date': str(row['effective_to']) if pd.notna(row['effective_to']) else None,
                            'primary': bool(row['primary_address'])
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Address {
                                address_id: $id,
                                address_type: $type,
                                address_line1: $line1,
                                address_line2: $line2,
                                city: $city,
                                state_province: $state,
                                postal_code: $postal,
                                country: $country,
                                status: $status,
                                effective_from: $from_date,
                                effective_to: $to_date,
                                primary_address: $primary
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ADDRESS]->(a)
                            WITH a, $country as c
                            MERGE (country:Country {code: c})
                            CREATE (a)-[:LOCATED_IN]->(country)
                        """, address_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving address {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_addresses} addresses")

            # Save compliance events
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Saving compliance events to Neo4j...")
                total_events = len(data['compliance_events'])
                success_count = 0
                
                for idx, row in data['compliance_events'].iterrows():
                    try:
                        event_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['event_id']),
                            'date': str(row['event_date']),
                            'type': str(row['event_type']),
                            'description': str(row['event_description']),
                            'old_state': str(row['old_state']) if pd.notna(row['old_state']) else None,
                            'new_state': str(row['new_state']),
                            'decision': str(row['decision']) if pd.notna(row['decision']) else None,
                            'decision_date': str(row['decision_date']) if pd.notna(row['decision_date']) else None,
                            'decision_maker': str(row['decision_maker']) if pd.notna(row['decision_maker']) else None,
                            'next_review': str(row['next_review_date']) if pd.notna(row['next_review_date']) else None
                        }
                        
                        await session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (ce:ComplianceEvent {
                                event_id: $id,
                                event_date: $date,
                                event_type: $type,
                                event_description: $description,
                                old_state: $old_state,
                                new_state: $new_state,
                                decision: $decision,
                                decision_date: $decision_date,
                                decision_maker: $decision_maker,
                                next_review_date: $next_review
                            })
                            WITH e, ce
                            CREATE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                            WITH ce, $date as edate
                            MERGE (bd:BusinessDate {date: edate})
                            CREATE (ce)-[:OCCURRED_ON]->(bd)
                        """, event_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving compliance event {idx}: {e}")
                print(f"✓ Saved {success_count}/{total_events} compliance events")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                total_transactions = len(data['transactions'])
                success_count = 0
                
                for idx, row in data['transactions'].iterrows():
                    try:
                        # Convert transaction data for Neo4j
                        transaction_data = {
                            'id': str(row['transaction_id']),
                            'account_id': str(row['account_id']),
                            'type': str(row.get('transaction_type', 'unknown')),
                            'date': str(row['transaction_date']),
                            'amount': float(row['amount']),
                            'currency': str(row.get('currency', 'USD')),
                            'status': str(row.get('transaction_status', 'pending')),
                            'is_debit': bool(row.get('is_debit', False)),
                            'counterparty_account': str(row.get('counterparty_account', '')),
                            'counterparty_name': str(row.get('counterparty_name', '')),
                            'counterparty_bank': str(row.get('counterparty_bank', '')),
                            'counterparty_entity_name': str(row.get('counterparty_entity_name', '')),
                            'originating_country': str(row.get('originating_country', '')),
                            'destination_country': str(row.get('destination_country', '')),
                            'purpose': str(row.get('purpose', '')),
                            'reference': str(row.get('reference_number', ''))
                        }
                        
                        await session.run("""
                            MATCH (a:Account {account_id: $account_id})
                            CREATE (t:Transaction {
                                transaction_id: $id,
                                transaction_type: $type,
                                transaction_date: $date,
                                amount: $amount,
                                currency: $currency,
                                status: $status,
                                is_debit: $is_debit,
                                counterparty_account: $counterparty_account,
                                counterparty_name: $counterparty_name,
                                counterparty_bank: $counterparty_bank,
                                counterparty_entity_name: $counterparty_entity_name,
                                originating_country: $originating_country,
                                destination_country: $destination_country,
                                purpose: $purpose,
                                reference_number: $reference
                            })-[:BELONGS_TO]->(a)
                            WITH t, $date as tdate
                            MERGE (bd:BusinessDate {date: tdate})
                            CREATE (t)-[:OCCURRED_ON]->(bd)
                        """, transaction_data)
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving transaction {row['transaction_id']}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_transactions} transactions")

    async def close(self):
        """Close database connections."""
        if self.driver:
            await self.driver.close()
            self.driver = None
            # Save addresses to PostgreSQL
            if 'addresses' in data and not data['addresses'].empty:
                print("Processing addresses...")
                try:
                    df = data['addresses'].copy()
                    
                    # Convert dates
                    df = convert_dates(df, ['effective_from', 'effective_to'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO addresses
                        SELECT * FROM jsonb_populate_recordset(null::addresses, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} addresses to PostgreSQL")
                except Exception as e:
                    print(f"Error saving addresses to PostgreSQL: {str(e)}")
                    raise

            # Save beneficial owners to PostgreSQL
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Processing beneficial owners...")
                try:
                    df = data['beneficial_owners'].copy()
                    
                    # Convert dates
                    df = convert_dates(df, ['dob', 'verification_date'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO beneficial_owners
                        SELECT * FROM jsonb_populate_recordset(null::beneficial_owners, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} beneficial owners to PostgreSQL")
                except Exception as e:
                    print(f"Error saving beneficial owners to PostgreSQL: {str(e)}")
                    raise

            # Save compliance events to PostgreSQL
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Processing compliance events...")
                try:
                    df = data['compliance_events'].copy()
                    
                    # Convert dates
                    df = convert_dates(df, ['event_date', 'decision_date', 'next_review_date'])
                    
                    # Convert to dict and handle nulls
                    df_dict = df.replace({np.nan: None}).to_dict(orient='records')
                    
                    await conn.execute(text("""
                        INSERT INTO compliance_events
                        SELECT * FROM jsonb_populate_recordset(null::compliance_events, cast(:data as jsonb))
                    """), {'data': json.dumps(df_dict)})
                    print(f"✓ Saved {len(df)} compliance events to PostgreSQL")
                except Exception as e:
                    print(f"Error saving compliance events to PostgreSQL: {str(e)}")
                    raise

        # Now save to Neo4j
        async with self.driver.session() as neo4j_session:
            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                
                for idx, row in data['addresses'].iterrows():
                    try:
                        address_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['address_id']),
                            'type': str(row['address_type']),
                            'line1': str(row['address_line1']),
                            'line2': str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                            'city': str(row['city']),
                            'state': str(row['state_province']),
                            'postal': str(row['postal_code']),
                            'country': str(row['country']),
                            'status': str(row['status']),
                            'from_date': str(row['effective_from']),
                            'to_date': str(row['effective_to']) if pd.notna(row['effective_to']) else None,
                            'primary': bool(row['primary_address'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (a:Address {
                                address_id: $id,
                                address_type: $type,
                                address_line1: $line1,
                                address_line2: $line2,
                                city: $city,
                                state_province: $state,
                                postal_code: $postal,
                                country: $country,
                                status: $status,
                                effective_from: $from_date,
                                effective_to: $to_date,
                                primary_address: $primary
                            })
                            WITH e, a
                            CREATE (e)-[:HAS_ADDRESS]->(a)
                            WITH a, $country as c
                            MERGE (country:Country {code: c})
                            CREATE (a)-[:LOCATED_IN]->(country)
                        """, address_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving address {idx}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_addresses} addresses")

            # Save beneficial owners
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                total_owners = len(data['beneficial_owners'])
                success_count = 0
                
                for idx, row in data['beneficial_owners'].iterrows():
                    try:
                        owner_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['owner_id']),
                            'name': str(row['name']),
                            'nationality': str(row['nationality']),
                            'residence': str(row['country_of_residence']),
                            'ownership': float(row['ownership_percentage']),
                            'dob': str(row['dob']),
                            'verification_date': str(row['verification_date']),
                            'pep': bool(row['pep_status']),
                            'sanctions': bool(row['sanctions_status']),
                            'adverse_media': bool(row['adverse_media_status'])
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (bo:BeneficialOwner {
                                owner_id: $id,
                                name: $name,
                                nationality: $nationality,
                                country_of_residence: $residence,
                                ownership_percentage: $ownership,
                                dob: $dob,
                                verification_date: $verification_date,
                                pep_status: $pep,
                                sanctions_status: $sanctions,
                                adverse_media_status: $adverse_media
                            })
                            WITH e, bo
                            CREATE (e)-[:HAS_BENEFICIAL_OWNER]->(bo)
                            WITH bo, $nationality as nat
                            MERGE (c:Country {code: nat})
                            CREATE (bo)-[:CITIZEN_OF]->(c)
                        """, owner_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving beneficial owner {idx}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_owners} beneficial owners")

            # Save compliance events
            if 'compliance_events' in data and not data['compliance_events'].empty:
                print("Saving compliance events to Neo4j...")
                total_events = len(data['compliance_events'])
                success_count = 0
                
                for idx, row in data['compliance_events'].iterrows():
                    try:
                        event_data = {
                            'entity_id': str(row['entity_id']),
                            'id': str(row['event_id']),
                            'date': str(row['event_date']),
                            'type': str(row['event_type']),
                            'description': str(row['event_description']),
                            'old_state': str(row['old_state']) if pd.notna(row['old_state']) else None,
                            'new_state': str(row['new_state']),
                            'decision': str(row['decision']) if pd.notna(row['decision']) else None,
                            'decision_date': str(row['decision_date']) if pd.notna(row['decision_date']) else None,
                            'decision_maker': str(row['decision_maker']) if pd.notna(row['decision_maker']) else None,
                            'next_review': str(row['next_review_date']) if pd.notna(row['next_review_date']) else None
                        }
                        
                        await neo4j_session.run("""
                            MATCH (e) WHERE e.institution_id = $entity_id OR e.subsidiary_id = $entity_id
                            CREATE (ce:ComplianceEvent {
                                event_id: $id,
                                event_date: $date,
                                event_type: $type,
                                event_description: $description,
                                old_state: $old_state,
                                new_state: $new_state,
                                decision: $decision,
                                decision_date: $decision_date,
                                decision_maker: $decision_maker,
                                next_review_date: $next_review
                            })
                            WITH e, ce
                            CREATE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                            WITH ce, $date as edate
                            MERGE (bd:BusinessDate {date: edate})
                            CREATE (ce)-[:OCCURRED_ON]->(bd)
                        """, event_data)
                        success_count += 1
                    except Exception as e:
                        print(f"\nError saving compliance event {idx}: {str(e)}")
                print(f"✓ Saved {success_count}/{total_events} compliance events")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                total_transactions = len(data['transactions'])
                success_count = 0
                
                for idx, row in data['transactions'].iterrows():
                    try:
                        # Convert transaction data for Neo4j
                        transaction_data = {
                            'id': str(row['transaction_id']),
                            'account_id': str(row['account_id']),
                            'type': str(row.get('transaction_type', 'unknown')),
                            'date': str(row['transaction_date']),
                            'amount': float(row['amount']),
                            'currency': str(row.get('currency', 'USD')),
                            'status': str(row.get('transaction_status', 'pending')),
                            'is_debit': bool(row.get('is_debit', False)),
                            'counterparty_account': str(row.get('counterparty_account', '')),
                            'counterparty_name': str(row.get('counterparty_name', '')),
                            'counterparty_bank': str(row.get('counterparty_bank', '')),
                            'counterparty_entity_name': str(row.get('counterparty_entity_name', '')),
                            'originating_country': str(row.get('originating_country', '')),
                            'destination_country': str(row.get('destination_country', '')),
                            'purpose': str(row.get('purpose', '')),
                            'reference': str(row.get('reference_number', ''))
                        }
                        
                        await neo4j_session.run("""
                            MATCH (a:Account {account_id: $account_id})
                            CREATE (t:Transaction {
                                transaction_id: $id,
                                transaction_type: $type,
                                transaction_date: $date,
                                amount: $amount,
                                currency: $currency,
                                status: $status,
                                is_debit: $is_debit,
                                counterparty_account: $counterparty_account,
                                counterparty_name: $counterparty_name,
                                counterparty_bank: $counterparty_bank,
                                counterparty_entity_name: $counterparty_entity_name,
                                originating_country: $originating_country,
                                destination_country: $destination_country,
                                purpose: $purpose,
                                reference_number: $reference
                            })-[:BELONGS_TO]->(a)
                            WITH t, $date as tdate
                            MERGE (bd:BusinessDate {date: tdate})
                            CREATE (t)-[:OCCURRED_ON]->(bd)
                        """, transaction_data)
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving transaction {row['transaction_id']} to Neo4j: {str(e)}")
                print(f"✓ Saved {success_count}/{total_transactions} transactions to Neo4j")