"""PostgreSQL database handler."""

import os
from typing import Dict, List, Any, Optional
import pandas as pd
import asyncpg
import json
from datetime import datetime
import numpy as np
import logging
from asyncpg import create_pool

from .base import DatabaseHandler, DatabaseError
from .exceptions import ConnectionError, ValidationError, SchemaError, BatchError, DatabaseInitializationError
from ..models import (
    Institution, Address, Account, BeneficialOwner, Transaction,
    BusinessType, OperationalStatus, RiskRating
)

class PostgresHandler(DatabaseHandler):
    """Handler for PostgreSQL database operations."""
    
    # Table schemas with their required columns and types
    TABLE_SCHEMAS = {
        'institutions': {
            # Critical fields
            'institution_id': 'UUID PRIMARY KEY',
            'legal_name': 'TEXT NOT NULL',
            'business_type': 'business_type NOT NULL',
            'incorporation_country': 'TEXT NOT NULL',
            'incorporation_date': 'DATE NOT NULL',
            'onboarding_date': 'DATE NOT NULL',
            'risk_rating': 'risk_rating NOT NULL',
            'operational_status': 'operational_status NOT NULL',
            
            # Optional fields
            'primary_currency': 'TEXT',
            'regulatory_status': 'TEXT',
            'primary_business_activity': 'TEXT',
            'primary_regulator': 'TEXT',
            'licenses': 'JSONB',
            'aml_program_status': 'TEXT',
            'kyc_refresh_date': 'DATE',
            'last_audit_date': 'DATE',
            'next_audit_date': 'DATE',
            'relationship_manager': 'TEXT',
            'relationship_status': 'TEXT',
            'swift_code': 'TEXT',
            'lei_code': 'TEXT',
            'tax_id': 'TEXT',
            'website': 'TEXT',
            'primary_contact_name': 'TEXT',
            'primary_contact_email': 'TEXT',
            'primary_contact_phone': 'TEXT',
            'annual_revenue': 'DECIMAL',
            'employee_count': 'INTEGER',
            'year_established': 'INTEGER',
            'customer_status': 'TEXT',
            'last_review_date': 'DATE',
            'industry_codes': 'JSONB',
            'public_company': 'BOOLEAN',
            'stock_symbol': 'TEXT',
            'stock_exchange': 'TEXT'
        },
        'addresses': {
            'address_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'address_type': 'TEXT NOT NULL',
            'address_line1': 'TEXT NOT NULL',
            'address_line2': 'TEXT',
            'city': 'TEXT NOT NULL',
            'state_province': 'TEXT',
            'postal_code': 'TEXT',
            'country': 'TEXT NOT NULL',
            'status': 'TEXT NOT NULL',
            'effective_from': 'DATE NOT NULL',
            'effective_to': 'DATE',
            'primary_address': 'BOOLEAN NOT NULL',
            'validation_status': 'TEXT NOT NULL',
            'last_verified': 'DATE NOT NULL',
            'geo_coordinates': 'JSONB NOT NULL',
            'timezone': 'TEXT NOT NULL'
        },
        'risk_assessments': {
            'assessment_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'assessment_date': 'DATE NOT NULL',
            'risk_rating': 'risk_rating NOT NULL',
            'risk_score': 'TEXT NOT NULL',
            'assessment_type': 'TEXT NOT NULL',
            'risk_factors': 'JSONB NOT NULL',
            'conducted_by': 'TEXT',
            'approved_by': 'TEXT',
            'findings': 'TEXT',
            'assessor': 'TEXT',
            'next_review_date': 'DATE',
            'notes': 'TEXT'
        },
        'authorized_persons': {
            'person_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'name': 'TEXT NOT NULL',
            'title': 'TEXT NOT NULL',
            'authorization_level': 'TEXT NOT NULL',
            'authorization_type': 'TEXT NOT NULL',
            'authorization_start': 'DATE NOT NULL',
            'authorization_end': 'DATE',
            'contact_info': 'JSONB NOT NULL',
            'is_active': 'BOOLEAN NOT NULL',
            'last_verification_date': 'DATE'
        },
        'documents': {
            'document_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'document_type': 'TEXT NOT NULL',
            'document_number': 'TEXT NOT NULL',
            'issuing_authority': 'TEXT NOT NULL',
            'issuing_country': 'TEXT NOT NULL',
            'issue_date': 'DATE NOT NULL',
            'expiry_date': 'DATE NOT NULL',
            'verification_status': 'TEXT',
            'verification_date': 'DATE',
            'document_category': 'TEXT',
            'notes': 'TEXT'
        },
        'jurisdiction_presences': {
            'presence_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'jurisdiction': 'TEXT NOT NULL',
            'registration_date': 'DATE NOT NULL',
            'effective_from': 'DATE NOT NULL',
            'status': 'TEXT NOT NULL',
            'local_registration_id': 'TEXT NOT NULL',
            'effective_to': 'DATE',
            'local_registration_date': 'DATE',
            'local_registration_authority': 'TEXT',
            'notes': 'TEXT'
        },
        'accounts': {
            'account_id': 'UUID PRIMARY KEY',
            'account_number': 'TEXT NOT NULL',
            'account_type': 'TEXT NOT NULL',
            'balance': 'DECIMAL NOT NULL',
            'currency': 'TEXT NOT NULL',
            'risk_rating': 'risk_rating NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'status': 'TEXT NOT NULL',
            'opening_date': 'DATE NOT NULL',
            'entity_id': 'UUID NOT NULL',
            'last_activity_date': 'DATE',
            'purpose': 'TEXT',
            'average_monthly_balance': 'DECIMAL',
            'custodian_bank': 'TEXT',
            'account_officer': 'TEXT',
            'custodian_country': 'TEXT'
        },
        'beneficial_owners': {
            'owner_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'name': 'TEXT NOT NULL',
            'nationality': 'TEXT NOT NULL',
            'country_of_residence': 'TEXT NOT NULL',
            'ownership_percentage': 'DECIMAL NOT NULL',
            'dob': 'DATE NOT NULL',
            'verification_date': 'DATE NOT NULL',
            'pep_status': 'BOOLEAN NOT NULL',
            'sanctions_status': 'BOOLEAN NOT NULL',
            'adverse_media_status': 'BOOLEAN NOT NULL',
            'verification_source': 'TEXT NOT NULL',
            'notes': 'TEXT'
        },
        'transactions': {
            'transaction_id': 'UUID PRIMARY KEY',
            'transaction_type': 'TEXT NOT NULL',
            'transaction_date': 'DATE NOT NULL',
            'value_date': 'DATE',
            'amount': 'DECIMAL NOT NULL',
            'currency': 'TEXT NOT NULL',
            'transaction_status': 'TEXT NOT NULL',
            'is_debit': 'BOOLEAN NOT NULL',
            'account_id': 'UUID NOT NULL',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'counterparty_account': 'TEXT',
            'counterparty_name': 'TEXT',
            'counterparty_bank': 'TEXT',
            'counterparty_entity_name': 'TEXT',
            'originating_country': 'TEXT',
            'destination_country': 'TEXT',
            'purpose': 'TEXT',
            'reference_number': 'TEXT',
            'screening_alert': 'BOOLEAN',
            'alert_details': 'TEXT',
            'risk_score': 'INTEGER',
            'processing_fee': 'DECIMAL',
            'exchange_rate': 'DECIMAL',
            'batch_id': 'TEXT',
            'check_number': 'TEXT',
            'wire_reference': 'TEXT'
        },
        'compliance_events': {
            'event_id': 'UUID PRIMARY KEY',
            'entity_id': 'UUID NOT NULL',
            'entity_type': 'TEXT NOT NULL',
            'event_date': 'DATE NOT NULL',
            'event_type': 'TEXT NOT NULL',
            'event_description': 'TEXT NOT NULL',
            'old_state': 'TEXT',
            'new_state': 'TEXT NOT NULL',
            'decision': 'TEXT',
            'decision_date': 'DATE',
            'decision_maker': 'TEXT',
            'next_review_date': 'DATE',
            'related_account_id': 'TEXT NOT NULL',
            'notes': 'TEXT'
        }
    }
    
    # Foreign key constraints to be added after table creation
    FOREIGN_KEY_CONSTRAINTS = {
        'addresses': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'risk_assessments': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'authorized_persons': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'documents': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'jurisdiction_presences': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'accounts': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'beneficial_owners': [
            ('entity_id', 'institutions', 'institution_id')
        ],
        'transactions': [
            ('account_id', 'accounts', 'account_id'),
            ('entity_id', 'institutions', 'institution_id')
        ],
        'compliance_events': [
            ('entity_id', 'institutions', 'institution_id')
        ]
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize PostgreSQL handler."""
        super().__init__()
        self.config = config or {
            'host': 'localhost',
            'port': 5432,
            'user': 'aml_user',
            'password': 'aml_password',
            'database': 'aml_monitoring'
        }
        self.pool = None
        self.is_connected = False
        self.logger = logging.getLogger(__name__)


        
    async def connect(self) -> None:
        """Establish connection to PostgreSQL database."""
        try:
            if not self.is_connected:
                # Use config values with environment variable fallbacks
                host = os.getenv('POSTGRES_HOST', self.config.get('host', 'localhost'))
                port = os.getenv('POSTGRES_PORT', str(self.config.get('port', '5432')))
                db = os.getenv('POSTGRES_DB', self.config.get('database', 'aml_monitoring'))
                user = os.getenv('POSTGRES_USER', self.config.get('user', 'postgres'))
                password = os.getenv('POSTGRES_PASSWORD', self.config.get('password', ''))
                
                self.pool = await asyncpg.create_pool(
                    f'postgresql://{user}:{password}@{host}:{port}/{db}',
                    min_size=1,
                    max_size=5
                )
                
                # Test connection
                async with self.pool.acquire() as conn:
                    await conn.execute('SELECT 1')
                
                self.is_connected = True
                self._log_operation('connect', {'status': 'success'})
                
            else:
                self._log_operation('connect', {'status': 'skipped', 'message': 'Already connected'})
                
        except Exception as e:
            self._log_operation('connect', {'status': 'failed', 'error': str(e)})
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self.pool:
            await self.pool.close()
            self.is_connected = False
            self._log_operation('disconnect', {'status': 'success'})
    
    async def validate_schema(self) -> bool:
        """Validate database schema against models."""
        try:
            async with self.pool.acquire() as conn:
                for table_name, schema in self.TABLE_SCHEMAS.items():
                    # Check if table exists
                    result = await conn.fetch(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = '{table_name}'
                        );
                    """)
                    exists = result[0]['exists']

                    if not exists:
                        raise SchemaError(f"Table {table_name} does not exist")

                    # Check columns
                    result = await conn.fetch(f"""
                        SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}';
                    """)
                    columns = [(row['column_name'], row['data_type']) for row in result]

                    db_columns = {col[0]: col[1] for col in columns}
                    for col_name in schema.keys():
                        if col_name not in db_columns:
                            raise SchemaError(
                                f"Column {col_name} missing in table {table_name}"
                            )

            return True

        except Exception as e:
            self._log_operation('validate_schema',
                              {'status': 'failed', 'error': str(e)})
            raise SchemaError(f"Schema validation failed: {str(e)}")
    
    async def create_schema(self) -> None:
        """Create database schema."""
        try:
            async with self.pool.acquire() as conn:
                # Drop existing tables in reverse order to handle dependencies
                for table_name in reversed(list(self.TABLE_SCHEMAS.keys())):
                    await conn.execute(f"""
                        DROP TABLE IF EXISTS {table_name} CASCADE
                    """)

                # Create tables in order (base tables first, then dependent tables)
                table_order = [
                    'institutions',  # Base table
                    'addresses',
                    'risk_assessments',
                    'authorized_persons',
                    'documents',
                    'jurisdiction_presences',
                    'accounts',
                    'beneficial_owners',
                    'transactions',
                    'compliance_events'
                ]

                # Create tables
                for table_name in table_order:
                    if table_name in self.TABLE_SCHEMAS:
                        columns = [f"{col} {dtype}"
                                for col, dtype in self.TABLE_SCHEMAS[table_name].items()]
                        create_stmt = f"""
                            CREATE TABLE {table_name} (
                                {', '.join(columns)}
                            )
                        """
                        await conn.execute(create_stmt)

                # Add foreign key constraints
                for table_name, constraints in self.FOREIGN_KEY_CONSTRAINTS.items():
                    for column, ref_table, ref_column in constraints:
                        await conn.execute(f"""
                            ALTER TABLE {table_name}
                            ADD CONSTRAINT fk_{table_name}_{column}
                            FOREIGN KEY ({column})
                            REFERENCES {ref_table}({ref_column})
                            ON DELETE CASCADE
                        """)

            self._log_operation('create_schema', {'status': 'success'})

        except Exception as e:
            self._log_operation('create_schema',
                              {'status': 'failed', 'error': str(e)})
            raise SchemaError(f"Failed to create schema: {str(e)}")
    
    async def initialize_database(self) -> None:
        """Initialize the database with required tables and enums."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                # Drop existing enums and tables
                await conn.execute("""
                    DROP TABLE IF EXISTS transactions CASCADE;
                    DROP TABLE IF EXISTS accounts CASCADE;
                    DROP TABLE IF EXISTS beneficial_owners CASCADE;
                    DROP TABLE IF EXISTS risk_assessments CASCADE;
                    DROP TABLE IF EXISTS addresses CASCADE;
                    DROP TABLE IF EXISTS authorized_persons CASCADE;
                    DROP TABLE IF EXISTS documents CASCADE;
                    DROP TABLE IF EXISTS jurisdiction_presences CASCADE;
                    DROP TABLE IF EXISTS institutions CASCADE;
                    DROP TABLE IF EXISTS compliance_events CASCADE;
                    DROP TYPE IF EXISTS business_type CASCADE;
                    DROP TYPE IF EXISTS operational_status CASCADE;
                    DROP TYPE IF EXISTS risk_rating CASCADE;
                    DROP TYPE IF EXISTS transaction_type CASCADE;
                    DROP TYPE IF EXISTS transaction_status CASCADE;
                """)

                # Create enum types
                await conn.execute("""
                    CREATE TYPE business_type AS ENUM (
                        'hedge_fund', 'bank', 'broker_dealer', 'insurance',
                        'asset_manager', 'pension_fund', 'other'
                    );

                    CREATE TYPE operational_status AS ENUM (
                        'active', 'dormant', 'liquidating'
                    );

                    CREATE TYPE risk_rating AS ENUM (
                        'low', 'medium', 'high'
                    );

                    CREATE TYPE transaction_type AS ENUM (
                        'ach', 'wire', 'check', 'lockbox'
                    );

                    CREATE TYPE transaction_status AS ENUM (
                        'completed', 'pending', 'failed', 'reversed'
                    );
                """)

                # Create tables
                await self.create_schema()

            self._log_operation('initialize_database', {'status': 'success'})

        except Exception as e:
            self._log_operation('initialize_database',
                              {'status': 'failed', 'error': str(e)})
            raise DatabaseInitializationError(f"Failed to initialize database: {str(e)}")
    
    async def _validate_dataframe_schema(self, table_name: str, df: pd.DataFrame) -> None:
        """Validate DataFrame schema for a specific table."""
        if table_name not in self.TABLE_SCHEMAS:
            raise ValidationError(f"Unknown table: {table_name}")
        
        required_columns = set([col for col, dtype in self.TABLE_SCHEMAS[table_name].items() if 'NOT NULL' in dtype])
        df_columns = set(df.columns)
        
        # Check for missing required columns
        missing_columns = required_columns - df_columns
        if missing_columns:
            raise ValidationError(
                f"Missing required columns in {table_name}: {missing_columns}"
            )
    
    async def validate_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """Validate data before saving to PostgreSQL."""
        try:
            # Check if data is empty
            if not data:
                raise ValidationError("No data provided")

            # Validate each table's data
            for table_name, df in data.items():
                if not isinstance(df, pd.DataFrame):
                    raise ValidationError(f"Data for {table_name} must be a DataFrame")
                
                if df.empty:
                    continue  # Skip empty DataFrames
                
                # Validate schema
                await self._validate_dataframe_schema(table_name, df)
                
                # Check for required fields
                required_columns = {col for col, dtype in self.TABLE_SCHEMAS[table_name].items()
                                  if 'NOT NULL' in dtype}
                missing_required = required_columns - set(df.columns)
                if missing_required:
                    raise ValidationError(
                        f"Missing required columns in {table_name}: {missing_required}"
                    )

                # Check for null values in required fields
                for col in required_columns & set(df.columns):
                    null_mask = df[col].isna()
                    if isinstance(null_mask, pd.Series):
                        null_mask = null_mask.any()
                    if null_mask:
                        null_indices = df[df[col].isna()].index.tolist()
                        raise ValidationError(
                            f"NULL values found in required column {table_name}.{col} "
                            f"at indices: {null_indices}"
                        )

                # Validate enum values
                enum_columns = {
                    'business_type': {'hedge_fund', 'bank', 'broker_dealer', 'insurance',
                                    'asset_manager', 'pension_fund', 'other'},
                    'operational_status': {'active', 'dormant', 'liquidating'},
                    'risk_rating': {'low', 'medium', 'high'},
                    'transaction_type': {'ach', 'wire', 'check', 'lockbox'},
                    'transaction_status': {'completed', 'pending', 'failed', 'reversed'}
                }

                for col, valid_values in enum_columns.items():
                    if col in df.columns:
                        # Convert enum values to strings
                        values = df[col].dropna().apply(lambda x: x.value if hasattr(x, 'value') else str(x).lower())
                        invalid_values = set(values) - valid_values
                        if invalid_values:
                            raise ValidationError(
                                f"Invalid {col} values in {table_name}: {invalid_values}"
                            )

                # Validate date formats
                date_columns = [col for col, dtype in self.TABLE_SCHEMAS[table_name].items()
                              if 'DATE' in dtype]
                for col in date_columns:
                    if col in df.columns:
                        try:
                            pd.to_datetime(df[col].dropna())
                        except Exception as e:
                            raise ValidationError(
                                f"Invalid date format in {table_name}.{col}: {str(e)}"
                            )

                # Validate numeric fields
                numeric_columns = [col for col, dtype in self.TABLE_SCHEMAS[table_name].items()
                                 if any(t in dtype.upper() for t in ['DECIMAL', 'INTEGER'])]
                for col in numeric_columns:
                    if col in df.columns:
                        non_numeric_mask = df[col].dropna().apply(lambda x: not isinstance(x, (int, float)))
                        if isinstance(non_numeric_mask, pd.Series):
                            non_numeric_mask = non_numeric_mask.any()
                        if non_numeric_mask:
                            raise ValidationError(
                                f"Non-numeric values found in {table_name}.{col}"
                            )

                # Validate JSON fields
                json_columns = [col for col, dtype in self.TABLE_SCHEMAS[table_name].items()
                              if 'JSONB' in dtype]
                for col in json_columns:
                    if col in df.columns:
                        def convert_to_json(x):
                            if isinstance(x, pd.Series):
                                x = x.tolist()
                            if isinstance(x, (list, np.ndarray)):
                                return json.dumps(x.tolist() if isinstance(x, np.ndarray) else x)
                            if pd.isna(x) or x is None:
                                return None
                            if isinstance(x, dict):
                                return json.dumps(x)
                            if isinstance(x, str):
                                try:
                                    json.loads(x)  # Validate it's already valid JSON
                                    return x
                                except:
                                    return json.dumps(x)
                            return json.dumps(str(x))

                        df[col] = df[col].apply(convert_to_json)

                        for idx, value in df[col].items():
                            # Skip None/NaN values
                            if value is None or pd.isna(value):
                                continue

                            # Validate JSON serialization
                            try:
                                if isinstance(value, str):
                                    json.loads(value)
                            except Exception as e:
                                raise ValidationError(
                                    f"Invalid JSON in {table_name}.{col} at index {idx}: {str(e)}"
                                )

                # Convert enum columns
                for col in enum_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: x.value if hasattr(x, 'value') else str(x).lower())

                # Convert date columns
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).apply(lambda x: x.date() if pd.notna(x) else None)

                # Check for required fields
                required_columns = {col for col, dtype in self.TABLE_SCHEMAS[table_name].items()
                                  if 'NOT NULL' in dtype}
                missing_required = required_columns - set(df.columns)
                if missing_required:
                    raise ValidationError(
                        f"Missing required columns in {table_name}: {missing_required}"
                    )

                # Check for null values in required fields
                for col in required_columns & set(df.columns):
                    if df[col].isna().any():
                        null_indices = df[df[col].isna()].index.tolist()
                        raise ValidationError(
                            f"NULL values found in required column {table_name}.{col} "
                            f"at indices: {null_indices}"
                        )

        except Exception as e:
            if not isinstance(e, ValidationError):
                raise ValidationError(f"Data validation failed: {str(e)}")
            raise
    
    async def save_batch(self, data: Dict[str, pd.DataFrame], batch_size: int = 1000) -> None:
        """Save a batch of data to PostgreSQL."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")

        # Validate data first - let validation errors propagate up
        await self.validate_data(data)

        try:
            async with self.pool.acquire() as conn:
                # Process each table in the correct order (respecting foreign keys)
                table_order = [
                    'institutions',
                    'addresses',
                    'risk_assessments',
                    'authorized_persons',
                    'documents',
                    'jurisdiction_presences',
                    'accounts',
                    'beneficial_owners',
                    'transactions',
                    'compliance_events'
                ]

                for table in table_order:
                    if table in data and not data[table].empty:
                        df = data[table].copy()  # Make a copy to avoid modifying original

                        # Convert dates to proper format
                        date_columns = [col for col, dtype in self.TABLE_SCHEMAS[table].items()
                                     if 'DATE' in dtype]
                        for col in date_columns:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col]).apply(lambda x: x.date() if pd.notna(x) else None)

                        # Convert JSON columns
                        json_columns = [col for col, dtype in self.TABLE_SCHEMAS[table].items()
                                     if 'JSONB' in dtype]
                        for col in json_columns:
                            if col in df.columns:
                                def convert_to_json(x):
                                    if isinstance(x, pd.Series):
                                        x = x.tolist()
                                    if isinstance(x, (list, np.ndarray)):
                                        return json.dumps(x.tolist() if isinstance(x, np.ndarray) else x)
                                    if pd.isna(x) or x is None:
                                        return None
                                    if isinstance(x, dict):
                                        return json.dumps(x)
                                    if isinstance(x, str):
                                        try:
                                            json.loads(x)  # Validate it's already valid JSON
                                            return x
                                        except:
                                            return json.dumps(x)
                                    return json.dumps(str(x))

                                df[col] = df[col].apply(convert_to_json)

                        # Convert enum columns
                        enum_columns = {
                            'business_type': {'hedge_fund', 'bank', 'broker_dealer', 'insurance',
                                    'asset_manager', 'pension_fund', 'other'},
                            'operational_status': {'active', 'dormant', 'liquidating'},
                            'risk_rating': {'low', 'medium', 'high'},
                            'transaction_type': {'ach', 'wire', 'check', 'lockbox'},
                            'transaction_status': {'completed', 'pending', 'failed', 'reversed'}
                        }

                        for col in enum_columns:
                            if col in df.columns:
                                df[col] = df[col].apply(lambda x: x.value if hasattr(x, 'value') else x)

                        # Handle NULL values for optional columns
                        optional_columns = [col for col, dtype in self.TABLE_SCHEMAS[table].items()
                                         if 'NOT NULL' not in dtype]
                        for col in optional_columns:
                            if col in df.columns:
                                df[col] = df[col].replace({pd.NA: None, np.nan: None, '': None})

                        # Convert numeric columns
                        numeric_columns = [col for col, dtype in self.TABLE_SCHEMAS[table].items()
                                        if any(num_type in dtype.upper() for num_type in ['INTEGER', 'DECIMAL', 'NUMERIC'])]
                        for col in numeric_columns:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce').replace({pd.NA: None, np.nan: None})

                        # Generate SQL for batch insert with UPSERT
                        columns = list(df.columns)
                        placeholders = [f'${i+1}' for i in range(len(columns))]
                        primary_key = next(col for col, dtype in self.TABLE_SCHEMAS[table].items() 
                                        if 'PRIMARY KEY' in dtype)
                        
                        insert_sql = f"""
                            INSERT INTO {table} ({', '.join(columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT ({primary_key}) DO UPDATE 
                            SET {', '.join(f"{col} = EXCLUDED.{col}" 
                                         for col in columns if col != primary_key)}
                        """

                        # Insert in batches
                        for i in range(0, len(df), batch_size):
                            batch_df = df.iloc[i:i + batch_size]
                            values = [tuple(row) for _, row in batch_df.iterrows()]
                            await conn.executemany(insert_sql, values)

            self._log_operation('save_batch', {'status': 'success'})

        except Exception as e:
            self._log_operation('save_batch',
                              {'status': 'failed', 'error': str(e)})
            raise BatchError(f"Failed to save batch: {str(e)}", [])
    
    async def wipe_clean(self) -> None:
        """Wipe all data from the database while preserving the schema."""
        try:
            # Ensure connection
            if not self.is_connected:
                await self.connect()

            async with self.pool.acquire() as conn:
                # Disable foreign key checks temporarily
                await conn.execute('SET CONSTRAINTS ALL DEFERRED')
                
                # Delete data from all tables in reverse order
                for table_name in reversed(list(self.TABLE_SCHEMAS.keys())):
                    await conn.execute(f'TRUNCATE TABLE {table_name} CASCADE')
                
                # Re-enable foreign key checks
                await conn.execute('SET CONSTRAINTS ALL IMMEDIATE')
            
            self._log_operation('wipe_clean', {'status': 'success'})
            
        except Exception as e:
            self._log_operation('wipe_clean', {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to wipe database: {str(e)}")

    async def healthcheck(self) -> bool:
        """Check database health."""
        try:
            if not self.is_connected:
                return False
                
            async with self.pool.acquire() as conn:
                result = await conn.execute('SELECT 1')
                return result.scalar() == 1
                
        except Exception as e:
            self._log_operation('healthcheck', 
                              {'status': 'failed', 'error': str(e)})
            return False
    
    async def _convert_enum_to_str(self, value: Any) -> str:
        """Convert enum values to strings for PostgreSQL storage."""
        if hasattr(value, 'value'):  # Check if it's an enum
            return value.value
        return value

    async def _prepare_data(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for PostgreSQL insertion by converting enums to strings."""
        prepared_data = {}
        for key, value in data.items():
            if key in self.TABLE_SCHEMAS[table_name]:
                if 'enum' in self.TABLE_SCHEMAS[table_name][key].lower():
                    prepared_data[key] = await self._convert_enum_to_str(value)
                else:
                    prepared_data[key] = value
        return prepared_data

    async def insert_data(self, table_name: str, data: Dict[str, Any]) -> None:
        """Insert a single row of data into a table."""
        try:
            if table_name not in self.TABLE_SCHEMAS:
                raise ValidationError(f"Invalid table name: {table_name}")
            
            # Prepare data with enum conversions
            prepared_data = await self._prepare_data(table_name, data)
            
            # Create the INSERT query
            columns = list(prepared_data.keys())
            values = [prepared_data[col] for col in columns]
            placeholders = [f"${i+1}" for i in range(len(columns))]
            
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            async with self.pool.acquire() as conn:
                await conn.execute(query, *values)
            
            self._log_operation('insert_data', 
                              {'table': table_name, 'data': prepared_data})
            
        except Exception as e:
            self._log_operation('insert_data', 
                              {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to insert data: {str(e)}")

    def _log_operation(self, operation: str, details: Dict[str, Any]) -> None:
        """Log database operations."""
        self.logger.info(f"PostgreSQL operation: {operation}", extra=details)
