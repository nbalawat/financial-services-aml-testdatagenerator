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
            # First drop the table if it exists to ensure a clean state
            await conn.execute(text('DROP TABLE IF EXISTS transactions'))
            
            # Then create the table
            await conn.execute(text("""
                CREATE TABLE transactions (
                    transaction_id UUID PRIMARY KEY,
                    entity_id UUID NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    account_id UUID NOT NULL,
                    transaction_type VARCHAR(50) NOT NULL,
                    transaction_date TIMESTAMP NOT NULL,
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
                    reference_number VARCHAR(100),
                    screening_alert BOOLEAN DEFAULT FALSE,
                    alert_details TEXT,
                    risk_score INTEGER,
                    processing_fee DECIMAL,
                    exchange_rate DECIMAL,
                    value_date DATE,
                    batch_id UUID,
                    check_number VARCHAR(50),
                    wire_reference VARCHAR(100)
                )
            """))
            
            # Commit the transaction to ensure table is created
            await conn.execute(text('COMMIT'))
    
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
                        risk_rating TEXT NOT NULL,
                        operational_status TEXT NOT NULL,
                        primary_currency TEXT NOT NULL,
                        regulatory_status TEXT NOT NULL,
                        primary_business_activity TEXT NOT NULL,
                        primary_regulator TEXT NOT NULL,
                        licenses JSONB NOT NULL,
                        aml_program_status TEXT NOT NULL,
                        kyc_refresh_date DATE NOT NULL,
                        last_audit_date DATE NOT NULL,
                        next_audit_date DATE NOT NULL,
                        relationship_manager TEXT NOT NULL,
                        relationship_status TEXT NOT NULL,
                        swift_code TEXT NOT NULL,
                        lei_code TEXT NOT NULL,
                        tax_id TEXT NOT NULL,
                        website TEXT NOT NULL,
                        primary_contact_name TEXT NOT NULL,
                        primary_contact_email TEXT NOT NULL,
                        primary_contact_phone TEXT NOT NULL,
                        annual_revenue DECIMAL NOT NULL,
                        employee_count INTEGER NOT NULL,
                        year_established INTEGER NOT NULL,
                        customer_status TEXT NOT NULL,
                        last_review_date DATE NOT NULL,
                        industry_codes JSONB NOT NULL,
                        public_company BOOLEAN NOT NULL,
                        stock_symbol TEXT,
                        stock_exchange TEXT
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
                        parent_institution_id TEXT NOT NULL,
                        legal_name TEXT NOT NULL,
                        tax_id TEXT NOT NULL,
                        incorporation_country TEXT NOT NULL,
                        incorporation_date DATE NOT NULL,
                        acquisition_date DATE NOT NULL,
                        business_type TEXT NOT NULL,
                        operational_status TEXT NOT NULL,
                        parent_ownership_percentage FLOAT NOT NULL,
                        consolidation_status TEXT NOT NULL,
                        capital_investment FLOAT NOT NULL,
                        functional_currency TEXT NOT NULL,
                        material_subsidiary BOOLEAN NOT NULL,
                        risk_classification TEXT NOT NULL,
                        regulatory_status TEXT NOT NULL,
                        local_licenses JSONB NOT NULL,
                        integration_status TEXT NOT NULL,
                        financial_metrics JSONB NOT NULL,
                        reporting_frequency TEXT NOT NULL,
                        requires_local_audit BOOLEAN NOT NULL,
                        corporate_governance_model TEXT NOT NULL,
                        is_regulated BOOLEAN NOT NULL,
                        is_customer BOOLEAN NOT NULL,
                        customer_id TEXT,
                        customer_onboarding_date DATE,
                        customer_risk_rating TEXT,
                        customer_status TEXT
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
                raise
            
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

    async def cleanup_postgres(self):
        """Clean up PostgreSQL database."""
        if not self.postgres_handler.engine:
            await self.postgres_handler.initialize()

        tables = [
            'institutions',
            'subsidiaries',
            'accounts',
            'transactions',
            'beneficial_owners',
            'addresses',
            'compliance_events',
            'jurisdiction_presence',
            'customers'
        ]

        async with self.postgres_handler.engine.begin() as conn:
            print("\nDropping tables in PostgreSQL...")
            for table in tables:
                try:
                    await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    print(f"✓ Dropped table {table}")
                except Exception as e:
                    print(f"Error dropping table {table}: {e}")

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
        async with self.driver.session() as session:
            for _, row in daily_stats.iterrows():
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
    
    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j."""
        # Initialize Neo4j connection if not already initialized
        if not self.driver:
            await self.initialize()
            
        # First create all nodes
        async with self.driver.session() as session:
            # Save institutions
            if 'institutions' in data and not data['institutions'].empty:
                print("Saving institutions to Neo4j...")
                for _, row in data['institutions'].iterrows():
                    try:
                        await session.run("""
                            MERGE (i:Institution {entity_id: $id})
                            SET i.name = $name,
                                i.country = $country,
                                i.business_type = $business_type,
                                i.risk_rating = $risk_rating,
                                i.onboarding_date = $onboarding_date,
                                i.incorporation_date = $incorporation_date,
                                i.operational_status = $operational_status
                            MERGE (c:Country {code: $country})
                            MERGE (i)-[:INCORPORATED_IN]->(c)
                            WITH i
                            MATCH (a:Account) WHERE a.entity_id = $id
                            MERGE (i)-[:HAS_ACCOUNT]->(a)
                            WITH i
                            MATCH (addr:Address) WHERE addr.entity_id = $id
                            MERGE (i)-[:HAS_ADDRESS]->(addr)
                            WITH i
                            MATCH (doc:Document) WHERE doc.entity_id = $id
                            MERGE (i)-[:HAS_DOCUMENT]->(doc)
                            WITH i
                            MATCH (jp:JurisdictionPresence) WHERE jp.entity_id = $id
                            MERGE (i)-[:HAS_JURISDICTION_PRESENCE]->(jp)
                        """, {
                            'id': str(row['institution_id']),
                            'name': row['legal_name'],
                            'country': row['incorporation_country'],
                            'business_type': row['business_type'],
                            'risk_rating': row['risk_rating'],
                            'onboarding_date': row['onboarding_date'],
                            'incorporation_date': row['incorporation_date'],
                            'operational_status': row['operational_status']
                        })
                    except Exception as e:
                        print(f"Error saving institution {row['legal_name']}: {e}")
                print(f"✓ Saved {len(data['institutions'])} institutions")

            # Save subsidiaries
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Saving subsidiaries to Neo4j...")
                success_count = 0
                for _, row in data['subsidiaries'].iterrows():
                    try:
                        await session.run("""
                            MERGE (s:Subsidiary {subsidiary_id: $id})
                            SET s.name = $name,
                                s.business_type = $business_type,
                                s.incorporation_date = $incorporation_date,
                                s.acquisition_date = $acquisition_date,
                                s.operational_status = $operational_status,
                                s.parent_ownership_percentage = $ownership_percentage,
                                s.risk_classification = $risk_classification,
                                s.is_regulated = $is_regulated
                            WITH s
                            MATCH (i:Institution {entity_id: $parent_id})
                            MERGE (i)-[:OWNS {ownership_percentage: $ownership_percentage}]->(s)
                            MERGE (c:Country {code: $country})
                            MERGE (s)-[:INCORPORATED_IN]->(c)
                        """, {
                            'id': str(row['subsidiary_id']),
                            'name': row['legal_name'],
                            'business_type': row['business_type'],
                            'incorporation_date': row['incorporation_date'],
                            'acquisition_date': row['acquisition_date'],
                            'operational_status': row['operational_status'],
                            'ownership_percentage': float(row['parent_ownership_percentage']),
                            'risk_classification': row['risk_classification'],
                            'is_regulated': bool(row['is_regulated']),
                            'parent_id': str(row['parent_institution_id']),
                            'country': row['incorporation_country']
                        })
                        success_count += 1
                    except Exception as e:
                        print(f"Error saving subsidiary {row['legal_name']}: {e}")

            # Save addresses
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                total_addresses = len(data['addresses'])
                success_count = 0
                for idx, row in enumerate(data['addresses'].iterrows(), 1):
                    try:
                        _, row = row  # Unpack the row
                        geo_coords = row['geo_coordinates']
                        if isinstance(geo_coords, str):
                            try:
                                geo = json.loads(geo_coords)
                            except:
                                geo = {}
                        elif isinstance(geo_coords, dict):
                            geo = geo_coords
                        else:
                            geo = {}
                            
                        lat = geo.get('latitude', 0.0)
                        lon = geo.get('longitude', 0.0)
                        
                        await session.run("""
                            MERGE (addr:Address {address_id: $id})
                            SET addr.address_line1 = $line1,
                                addr.address_line2 = $line2,
                                addr.city = $city,
                                addr.state_province = $state,
                                addr.postal_code = $postal_code,
                                addr.country = $country,
                                addr.address_type = $type,
                                addr.status = $status,
                                addr.effective_from = $effective_from,
                                addr.effective_to = $effective_to,
                                addr.primary_address = $primary,
                                addr.validation_status = $validation_status,
                                addr.last_verified = $last_verified,
                                addr.latitude = $lat,
                                addr.longitude = $lon,
                                addr.timezone = $timezone,
                                addr.entity_id = $entity_id,
                                addr.entity_type = $entity_type
                            WITH addr
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (owner)-[:HAS_ADDRESS]->(addr)
                            MERGE (c:Country {code: $country})
                            MERGE (addr)-[:LOCATED_IN]->(c)
                        """, {
                            'id': str(row['address_id']),
                            'line1': row['address_line1'],
                            'line2': row.get('address_line2'),
                            'city': row['city'],
                            'state': row['state_province'],
                            'postal_code': row['postal_code'],
                            'country': row['country'],
                            'type': row['address_type'],
                            'status': row['status'],
                            'effective_from': row['effective_from'],
                            'effective_to': row.get('effective_to'),
                            'primary': bool(row['primary_address']),
                            'validation_status': row['validation_status'],
                            'last_verified': row['last_verified'],
                            'lat': lat,
                            'lon': lon,
                            'timezone': row['timezone'],
                            'entity_id': str(row['entity_id']),
                            'entity_type': row['entity_type']
                        })
                        success_count += 1
                        print(f"Progress: {success_count}/{total_addresses} addresses", end='\r')
                    except Exception as e:
                        print(f"\nError saving address {idx}: {e}")
                print(f"\n✓ Saved {success_count}/{total_addresses} addresses")

            # Save transactions
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                batch_size = 100
                transactions = data['transactions'].to_dict('records')
                total_transactions = len(transactions)
                success_count = 0
                
                for i in range(0, total_transactions, batch_size):
                    batch = transactions[i:i + batch_size]
                    batch_success = 0
                    try:
                        for row in batch:
                            await session.run("""
                                MATCH (a:Account {account_id: $account_id})
                                MERGE (t:Transaction {transaction_id: $id})
                                SET t.type = $type,
                                    t.amount = $amount,
                                    t.currency = $currency,
                                    t.transaction_status = $transaction_status,
                                    t.date = $date
                                MERGE (a)-[:HAS_TRANSACTION {date: $date}]->(t)
                                MERGE (t)-[:BELONGS_TO {type: 'originating'}]->(a)
                                MERGE (owner)-[:TRANSACTED {via: a.account_id}]->(t)
                                WITH t
                                MERGE (oc:Country {code: $originating_country})
                                MERGE (dc:Country {code: $destination_country})
                                MERGE (t)-[:ORIGINATED_FROM]->(oc)
                                MERGE (t)-[:DESTINED_TO]->(dc)
                                WITH t
                                MERGE (bd:BusinessDate {date: $date})
                                MERGE (t)-[:OCCURRED_ON]->(bd)
                            """, {
                                'id': str(row['transaction_id']),
                                'account_id': str(row['account_id']),
                                'type': row['transaction_type'],
                                'amount': float(row['amount']),
                                'currency': row['currency'],
                                'transaction_status': row['transaction_status'],
                                'date': row['transaction_date'],
                                'originating_country': row['originating_country'],
                                'destination_country': row['destination_country']
                            })
                            batch_success += 1
                            success_count += 1
                    except Exception as e:
                        print(f"\nError in batch {i//batch_size + 1}: {e}")
                    
                    # Update progress after each batch
                    progress = min(success_count, total_transactions)
                    batch_num = i//batch_size + 1
                    total_batches = (total_transactions + batch_size - 1)//batch_size
                    print(f"Progress: Batch {batch_num}/{total_batches} - {progress}/{total_transactions} transactions", end='\r')
                
                print(f"\n✓ Saved {success_count}/{total_transactions} transactions")

            # Save subsidiaries and link to institutions
            if 'subsidiaries' in data and not data['subsidiaries'].empty:
                print("Saving subsidiaries to Neo4j...")
                for _, row in data['subsidiaries'].iterrows():
                    try:
                        await session.run("""
                            MERGE (s:Subsidiary {subsidiary_id: $id})
                            SET s.name = $name,
                                s.business_type = $business_type,
                                s.incorporation_date = $incorporation_date,
                                s.acquisition_date = $acquisition_date,
                                s.operational_status = $operational_status,
                                s.parent_ownership_percentage = $ownership_percentage,
                                s.risk_classification = $risk_classification,
                                s.is_regulated = $is_regulated
                            WITH s
                            MATCH (i:Institution {entity_id: $parent_id})
                            MERGE (i)-[:OWNS {ownership_percentage: $ownership_percentage}]->(s)
                            MERGE (c:Country {code: $country})
                            MERGE (s)-[:INCORPORATED_IN]->(c)
                        """, {
                            'id': str(row['subsidiary_id']),
                            'name': row['legal_name'],
                            'business_type': row['business_type'],
                            'incorporation_date': row['incorporation_date'],
                            'acquisition_date': row['acquisition_date'],
                            'operational_status': row['operational_status'],
                            'ownership_percentage': float(row['parent_ownership_percentage']),
                            'risk_classification': row['risk_classification'],
                            'is_regulated': bool(row['is_regulated']),
                            'parent_id': str(row['parent_institution_id']),
                            'country': row['incorporation_country']
                        })
                    except Exception as e:
                        print(f"Error saving subsidiary {row['legal_name']}: {e}")

            # Save accounts and link to entities and currencies
            if 'accounts' in data and not data['accounts'].empty:
                print("Saving accounts to Neo4j...")
                total_accounts = len(data['accounts'])
                success_count = 0
                for idx, row in enumerate(data['accounts'].iterrows(), 1):
                    try:
                        _, row = row  # Unpack the row
                        await session.run("""
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (acc:Account {account_id: $id})
                            SET acc.account_type = $type,
                                acc.account_number = $number,
                                acc.status = $status,
                                acc.opening_date = $opening_date,
                                acc.balance = $balance,
                                acc.entity_id = $entity_id,
                                acc.entity_type = $entity_type
                            MERGE (owner)-[:HAS_ACCOUNT]->(acc)
                            MERGE (curr:Currency {code: $currency})
                            MERGE (acc)-[:DENOMINATED_IN]->(curr)
                            WITH acc, $custodian_country as cc
                            WHERE cc IS NOT NULL
                            MERGE (c:Country {code: cc})
                            MERGE (acc)-[:CUSTODIED_IN]->(c)
                        """, {
                            'id': str(row['account_id']),
                            'type': row['account_type'],
                            'number': row['account_number'],
                            'status': row['status'],
                            'opening_date': row['opening_date'],
                            'balance': float(row['balance']),
                            'entity_id': str(row['entity_id']),
                            'entity_type': row['entity_type'],
                            'currency': row['currency'],
                            'custodian_country': row.get('custodian_country')
                        })
                        success_count += 1
                        print(f"Progress: {success_count}/{total_accounts} accounts", end='\r')
                    except Exception as e:
                        print(f"\nError saving account {idx}: {e}")
                print(f"\n✓ Saved {success_count}/{total_accounts} accounts")

            # Save beneficial owners and link to entities
            if 'beneficial_owners' in data and not data['beneficial_owners'].empty:
                print("Saving beneficial owners to Neo4j...")
                for _, row in data['beneficial_owners'].iterrows():
                    try:
                        await session.run("""
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (bo:BeneficialOwner {owner_id: $id})
                            SET bo.name = $name,
                                bo.country = $country,
                                bo.ownership_percentage = $percentage,
                                bo.verification_date = $verification_date
                            MERGE (bo)-[:OWNS {percentage: $percentage}]->(owner)
                            MERGE (bo)-[:CONTROLS {type: 'beneficial_ownership', percentage: $percentage}]->(owner)
                            MERGE (owner)-[:OWNED_BY {verified_on: $verification_date}]->(bo)
                            MERGE (bo)-[:HAS_NATIONALITY {country: $country}]->(c:Country {code: $country})
                        """, {
                            'id': str(row['owner_id']),
                            'entity_id': str(row['entity_id']),
                            'name': row['name'],  
                            'country': row['nationality'],
                            'percentage': float(row['ownership_percentage']),
                            'verification_date': row['verification_date']
                        })
                    except Exception as e:
                        print(f"Error saving beneficial owner {row['name']}: {e}")

            # Save transactions (using existing rollup logic)
            if 'transactions' in data and not data['transactions'].empty:
                print("Saving transactions to Neo4j...")
                for _, row in data['transactions'].iterrows():
                    try:
                        await session.run("""
                            MATCH (a:Account {account_id: $account_id})
                            MERGE (t:Transaction {transaction_id: $id})
                            SET t.type = $type,
                                t.amount = $amount,
                                t.currency = $currency,
                                t.transaction_status = $transaction_status,
                                t.date = $date
                            MERGE (a)-[:HAS_TRANSACTION {date: $date}]->(t)
                            MERGE (t)-[:BELONGS_TO {type: 'originating'}]->(a)
                            MERGE (owner)-[:TRANSACTED {via: a.account_id}]->(t)
                            WITH t
                            MERGE (oc:Country {code: $originating_country})
                            MERGE (dc:Country {code: $destination_country})
                            MERGE (t)-[:ORIGINATED_FROM]->(oc)
                            MERGE (t)-[:DESTINED_TO]->(dc)
                            WITH t
                            MERGE (bd:BusinessDate {date: $date})
                            MERGE (t)-[:OCCURRED_ON]->(bd)
                        """, {
                            'id': str(row['transaction_id']),
                            'account_id': str(row['account_id']),
                            'type': row['transaction_type'],
                            'amount': float(row['amount']),
                            'currency': row['currency'],
                            'transaction_status': row['transaction_status'],
                            'date': row['transaction_date'],
                            'originating_country': row['originating_country'],
                            'destination_country': row['destination_country']
                        })
                    except Exception as e:
                        print(f"Error saving transaction {row['transaction_id']}: {e}")

            # Save documents and link to entities
            if 'documents' in data and not data['documents'].empty:
                print("Saving documents to Neo4j...")
                for _, row in data['documents'].iterrows():
                    try:
                        await session.run("""
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (doc:Document {document_id: $id})
                            SET doc.document_type = $type,
                                doc.document_number = $number,
                                doc.issuing_authority = $authority,
                                doc.issuing_country = $country,
                                doc.issue_date = $issue_date,
                                doc.expiry_date = $expiry_date,
                                doc.verification_status = $status
                            MERGE (c:Country {code: $country})
                            MERGE (doc)-[:ISSUED_IN]->(c)
                            MERGE (owner)-[:HAS_DOCUMENT]->(doc)
                        """, {
                            'id': str(row['document_id']),
                            'entity_id': str(row['entity_id']),
                            'type': row['document_type'],
                            'number': row['document_number'],
                            'authority': row['issuing_authority'],
                            'country': row['issuing_country'],
                            'issue_date': row['issue_date'],
                            'expiry_date': row['expiry_date'],
                            'status': row['verification_status']
                        })
                    except Exception as e:
                        print(f"Error saving document {row['document_id']}: {e}")

            # Save jurisdiction presences and link to entities
            if 'jurisdiction_presences' in data and not data['jurisdiction_presences'].empty:
                print("Saving jurisdiction presences to Neo4j...")
                for _, row in data['jurisdiction_presences'].iterrows():
                    try:
                        await session.run("""
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (jp:JurisdictionPresence {presence_id: $id})
                            SET jp.presence_type = $type,
                                jp.regulatory_status = $status,
                                jp.establishment_date = $date
                            MERGE (c:Country {code: $country})
                            MERGE (jp)-[:PRESENT_IN]->(c)
                            MERGE (owner)-[:HAS_JURISDICTION_PRESENCE]->(jp)
                        """, {
                            'id': str(row['presence_id']),
                            'entity_id': str(row['entity_id']),
                            'type': row['presence_type'],
                            'status': row['regulatory_status'],
                            'date': row['establishment_date'],
                            'country': row['jurisdiction']
                        })
                    except Exception as e:
                        print(f"Error saving jurisdiction presence {row['presence_id']}: {e}")

            # Save addresses and link to entities
            if 'addresses' in data and not data['addresses'].empty:
                print("Saving addresses to Neo4j...")
                for _, row in data['addresses'].iterrows():
                    try:
                        geo_coords = row['geo_coordinates']
                        if isinstance(geo_coords, str):
                            try:
                                geo = json.loads(geo_coords)
                            except:
                                geo = {}
                        elif isinstance(geo_coords, dict):
                            geo = geo_coords
                        else:
                            geo = {}
                            
                        lat = geo.get('latitude', 0.0)
                        lon = geo.get('longitude', 0.0)
                        
                        await session.run("""
                            MATCH (owner) WHERE owner.entity_id = $entity_id
                            MERGE (addr:Address {address_id: $id})
                            SET addr.line1 = $line1,
                                addr.line2 = $line2,
                                addr.city = $city,
                                addr.state = $state,
                                addr.postal_code = $postal_code,
                                addr.latitude = $latitude,
                                addr.longitude = $longitude
                            MERGE (c:Country {code: $country})
                            MERGE (addr)-[:LOCATED_IN]->(c)
                            MERGE (owner)-[:HAS_ADDRESS]->(addr)
                            WITH addr
                            MATCH (other:Address)
                            WHERE other.address_id <> $id 
                              AND abs(other.latitude - addr.latitude) < 0.1
                              AND abs(other.longitude - addr.longitude) < 0.1
                            MERGE (addr)-[:NEAR]->(other)
                        """, {
                            'id': str(row['address_id']),
                            'entity_id': str(row['entity_id']),
                            'line1': row['address_line1'],
                            'line2': row.get('address_line2'),
                            'city': row['city'],
                            'state': row.get('state_province'),
                            'postal_code': row['postal_code'],
                            'country': row['country'],
                            'latitude': lat,
                            'longitude': lon
                        })
                    except Exception as e:
                        print(f"Error saving address {row['address_id']}: {e}")
    
    async def close(self):
        """Close database connection."""
        if self.driver:
            await self.driver.close()


class DatabaseManager:
    def __init__(self):
        """Initialize database handlers."""
        self.postgres_handler = PostgresHandler()
        self.neo4j_handler = Neo4jHandler()

    async def initialize_postgres(self):
        """Initialize PostgreSQL database tables."""
        await self.postgres_handler.initialize()

    async def cleanup_postgres(self):
        """Clean up PostgreSQL database."""
        tables = [
            'institutions',
            'subsidiaries',
            'accounts',
            'transactions',
            'beneficial_owners',
            'addresses',
            'compliance_events',
            'jurisdiction_presence',
            'customers'
        ]

        async with self.postgres_handler.engine.begin() as conn:
            print("\nDropping tables in PostgreSQL...")
            for table in tables:
                try:
                    await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    print(f"✓ Dropped table {table}")
                except Exception as e:
                    print(f"Error dropping table {table}: {e}")

    async def cleanup_neo4j(self):
        """Clean up Neo4j database."""
        # Initialize Neo4j connection if not already initialized
        if not self.neo4j_handler.driver:
            await self.neo4j_handler.initialize()
            
        async with self.neo4j_handler.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")
            print("✓ Cleaned up Neo4j database")

    async def save_to_postgres(self, data: Dict[str, pd.DataFrame]):
        """Save data to PostgreSQL."""
        if not self.postgres_handler.engine:
            await self.postgres_handler.initialize()

        # First initialize tables
        await self.postgres_handler.initialize_tables()

        date_terms = ['date', 'dob', 'verification_date', 'registration_date', 'expiry_date', 'review_date', 'onboarding_date', 'incorporation_date', 'acquisition_date', 'last_activity_date', 'effective_from', 'effective_to', 'last_verified', 'resolution_date', 'next_review_date', 'last_audit_date', 'next_audit_date', 'kyc_refresh_date', 'last_review_date', 'value_date']

        async with self.postgres_handler.engine.begin() as conn:
            for table_name, df in data.items():
                if df.empty:
                    continue

                print(f"Processing {table_name}...")
                
                # Convert date columns and handle NaT values
                for col in df.columns:
                    if any(term in col.lower() for term in date_terms):
                        try:
                            # Convert to datetime first
                            df[col] = pd.to_datetime(df[col])
                            # Replace NaT with None
                            df[col] = df[col].where(pd.notna(df[col]), None)
                            # Convert valid dates to date objects
                            df[col] = df[col].apply(lambda x: x.date() if pd.notna(x) else None)
                        except Exception as e:
                            print(f"Error converting column {col}: {e}")
                            continue
                    
                    # Handle JSON/JSONB fields
                    if col in ['geo_coordinates', 'financial_metrics', 'local_licenses', 'licenses', 'industry_codes', 'service_agreements', 'risk_factors', 'reporting_requirements']:
                        df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

                # Get table columns
                try:
                    result = await conn.execute(text(f"SELECT * FROM {table_name} LIMIT 0"))
                    columns = result.keys()
                except Exception as e:
                    print(f"Skipping {table_name}: table not found")
                    continue

                records = df.to_dict('records')
                batch_size = 1000
                total_batches = (len(records)-1)//batch_size + 1

                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    placeholders = ', '.join([f':{col}' for col in columns])
                    
                    try:
                        await conn.execute(
                            text(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"),
                            batch
                        )
                        print(f"✓ {table_name} {i//batch_size + 1}/{total_batches}", end='\r')
                    except Exception as e:
                        print(f"\nError in {table_name} batch {i//batch_size + 1}: {e}")
                        continue
                print(f"\n✓ {table_name} complete")
    
    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j."""
        # Initialize Neo4j connection if not already initialized
        if not self.neo4j_handler.driver:
            await self.neo4j_handler.initialize()
            
        # First clean up existing data
        await self.cleanup_neo4j()
        
        # Save data
        await self.neo4j_handler.save_to_neo4j(data)

class TransactionGenerator:
    def __init__(self):
        """Initialize transaction generator."""
        self.transactions = []

    def generate_transactions(self, num_transactions: int) -> pd.DataFrame:
        """Generate synthetic transaction data."""
        failed = 0
        reversed = 0
        pending = 0
        completed = 0
        
        print(f"Generating {num_transactions} transactions...")
        
        for i in range(num_transactions):
            transaction = self._generate_single_transaction()
            self.transactions.append(transaction)
            
            # Update status counts
            if transaction['status'] == 'failed':
                failed += 1
            elif transaction['status'] == 'reversed':
                reversed += 1
            elif transaction['status'] == 'pending':
                pending += 1
            else:
                completed += 1
                
            # Show progress every 1000 transactions
            if (i + 1) % 1000 == 0:
                print(f"Progress: {i + 1}/{num_transactions} (✓: {completed}, ⚠: {pending}, ✗: {failed}, ↺: {reversed})", end='\r')
        
        print(f"\nCompleted generating {num_transactions} transactions")
        print(f"Status Summary: {completed} completed, {pending} pending, {failed} failed, {reversed} reversed")
        
        return pd.DataFrame(self.transactions)

    def _generate_single_transaction(self) -> Dict[str, Any]:
        """Generate a single synthetic transaction."""
        # Implement logic to generate a single transaction
        pass
