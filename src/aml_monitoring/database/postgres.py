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
from uuid import UUID

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
        'entities': {
            'entity_id': 'uuid PRIMARY KEY',
            'entity_type': 'text',  # 'institution' or 'subsidiary'
            'parent_entity_id': 'uuid',  # NULL for institutions, parent_institution_id for subsidiaries
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'institutions': {
            'institution_id': 'uuid PRIMARY KEY',
            'legal_name': 'text NOT NULL',
            'business_type': 'text NOT NULL',
            'incorporation_country': 'text NOT NULL',
            'incorporation_date': 'date NOT NULL',
            'onboarding_date': 'date NOT NULL',
            'risk_rating': 'text NOT NULL',
            'operational_status': 'text NOT NULL',
            'primary_currency': 'text',
            'regulatory_status': 'text',
            'primary_business_activity': 'text',
            'primary_regulator': 'text',
            'licenses': 'text[]',
            'aml_program_status': 'text',
            'kyc_refresh_date': 'date',
            'last_audit_date': 'date',
            'next_audit_date': 'date',
            'relationship_manager': 'text',
            'relationship_status': 'text',
            'swift_code': 'text',
            'lei_code': 'text',
            'tax_id': 'text',
            'website': 'text',
            'primary_contact_name': 'text',
            'primary_contact_email': 'text',
            'primary_contact_phone': 'text',
            'annual_revenue': 'numeric',
            'employee_count': 'integer',
            'year_established': 'integer',
            'customer_status': 'text',
            'last_review_date': 'date',
            'industry_codes': 'text[]',
            'public_company': 'boolean',
            'stock_symbol': 'text',
            'stock_exchange': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'subsidiaries': {
            'subsidiary_id': 'uuid PRIMARY KEY',
            'parent_institution_id': 'uuid',
            'legal_name': 'text',
            'tax_id': 'text',
            'incorporation_country': 'text',
            'incorporation_date': 'date',
            'acquisition_date': 'date',
            'business_type': 'text',
            'operational_status': 'text',
            'parent_ownership_percentage': 'numeric',
            'consolidation_status': 'text',
            'capital_investment': 'numeric',
            'functional_currency': 'text',
            'material_subsidiary': 'boolean',
            'risk_classification': 'text',
            'regulatory_status': 'text',
            'local_licenses': 'text[]',
            'integration_status': 'text',
            'revenue': 'numeric',
            'assets': 'numeric',
            'liabilities': 'numeric',
            'reporting_frequency': 'text',
            'requires_local_audit': 'boolean',
            'corporate_governance_model': 'text',
            'is_regulated': 'boolean',
            'is_customer': 'boolean',
            'industry_codes': 'text[]',
            'financial_metrics': 'jsonb',
            'customer_id': 'uuid',
            'customer_onboarding_date': 'date',
            'customer_risk_rating': 'text',
            'customer_status': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'addresses': {
            'address_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'address_type': 'text',
            'address_line1': 'text',
            'address_line2': 'text',
            'city': 'text',
            'state_province': 'text',
            'postal_code': 'text',
            'country': 'text',
            'status': 'text',
            'effective_from': 'date',
            'effective_to': 'date',
            'primary_address': 'boolean',
            'validation_status': 'text',
            'last_verified': 'date',
            'latitude': 'numeric',
            'longitude': 'numeric',
            'timezone': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'accounts': {
            'account_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'account_type': 'text',
            'account_number': 'text',
            'currency': 'text',
            'status': 'text',
            'opening_date': 'date',
            'balance': 'numeric',
            'risk_rating': 'text',
            'last_activity_date': 'date',
            'purpose': 'text',
            'average_monthly_balance': 'numeric',
            'custodian_bank': 'text',
            'account_officer': 'text',
            'custodian_country': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'transactions': {
            'transaction_id': 'uuid PRIMARY KEY',
            'transaction_type': 'text',
            'transaction_date': 'date',
            'amount': 'numeric',
            'currency': 'text',
            'transaction_status': 'text',
            'is_debit': 'boolean',
            'account_id': 'uuid',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'debit_account_id': 'uuid',
            'credit_account_id': 'uuid',
            'counterparty_account': 'text',
            'counterparty_name': 'text',
            'counterparty_bank': 'text',
            'counterparty_entity_name': 'text',
            'originating_country': 'text',
            'destination_country': 'text',
            'purpose': 'text',
            'reference_number': 'text',
            'screening_alert': 'boolean',
            'alert_details': 'text',
            'risk_score': 'integer',
            'processing_fee': 'numeric',
            'exchange_rate': 'numeric',
            'value_date': 'date',
            'batch_id': 'text',
            'check_number': 'text',
            'wire_reference': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'beneficial_owners': {
            'owner_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'name': 'text',
            'nationality': 'text',
            'country_of_residence': 'text',
            'ownership_percentage': 'numeric',
            'dob': 'date',
            'verification_date': 'date',
            'pep_status': 'boolean',
            'sanctions_status': 'boolean',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'risk_assessments': {
            'assessment_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'assessment_date': 'date',
            'risk_rating': 'text',
            'risk_score': 'text',
            'assessment_type': 'text',
            'risk_factors': 'jsonb',
            'conducted_by': 'text',
            'approved_by': 'text',
            'findings': 'text',
            'assessor': 'text',
            'next_review_date': 'date',
            'notes': 'text'
        },
        'authorized_persons': {
            'person_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'name': 'text',
            'title': 'text',
            'authorization_level': 'text',
            'authorization_type': 'text',
            'authorization_start': 'date',
            'authorization_end': 'date',
            'contact_info': 'jsonb',
            'is_active': 'boolean',
            'last_verification_date': 'date',
            'nationality': 'text'
        },
        'documents': {
            'document_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'document_type': 'text',
            'document_number': 'text',
            'issuing_authority': 'text',
            'issuing_country': 'text',
            'issue_date': 'date',
            'expiry_date': 'date',
            'verification_status': 'text',
            'verification_date': 'date',
            'document_category': 'text',
            'notes': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'jurisdiction_presences': {
            'presence_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'jurisdiction': 'text',
            'registration_date': 'date',
            'effective_from': 'date',
            'status': 'text',
            'local_registration_id': 'text',
            'effective_to': 'date',
            'local_registration_date': 'date',
            'local_registration_authority': 'text',
            'notes': 'text',
            'created_at': 'timestamp',
            'updated_at': 'timestamp',
            'deleted_at': 'timestamp'
        },
        'compliance_events': {
            'event_id': 'uuid PRIMARY KEY',
            'entity_id': 'uuid',
            'entity_type': 'text',
            'event_date': 'date',
            'event_type': 'text',
            'event_description': 'text',
            'old_state': 'text',
            'new_state': 'text',
            'decision': 'text',
            'decision_date': 'date',
            'decision_maker': 'text',
            'next_review_date': 'date',
            'related_account_id': 'text',
            'notes': 'text'
        }
    }
    
    # Foreign key constraints to be added after table creation
    FOREIGN_KEY_CONSTRAINTS = {
        'entities': [
            ('parent_entity_id', 'entities', 'entity_id')
        ],
        'institutions': [
            ('institution_id', 'entities', 'entity_id')
        ],
        'subsidiaries': [
            ('subsidiary_id', 'entities', 'entity_id'),
            ('parent_institution_id', 'institutions', 'institution_id')
        ],
        'addresses': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'risk_assessments': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'authorized_persons': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'documents': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'jurisdiction_presences': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'accounts': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'beneficial_owners': [
            ('entity_id', 'entities', 'entity_id')
        ],
        'transactions': [
            ('account_id', 'accounts', 'account_id'),
            ('entity_id', 'entities', 'entity_id')
        ],
        'compliance_events': [
            ('entity_id', 'entities', 'entity_id')
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
    
    async def close(self) -> None:
        """Close database connection."""
        if self.pool:
            await self.pool.close()
            self.is_connected = False
            self._log_operation('close', {'status': 'success'})
    
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
                    'entities',  # Base table
                    'institutions',
                    'subsidiaries',
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
                        columns = []
                        for col, dtype in self.TABLE_SCHEMAS[table_name].items():
                            columns.append(f"{col} {dtype}")

                        create_stmt = f"""
                            CREATE TABLE {table_name} (
                                {', '.join(columns)}
                            )
                        """
                        await conn.execute(create_stmt)

                # Add foreign key constraints
                for table_name, constraints in self.FOREIGN_KEY_CONSTRAINTS.items():
                    for i, (column, ref_table, ref_column) in enumerate(constraints):
                        constraint_name = f"fk_{table_name}_{column}_{ref_table}"
                        await conn.execute(f"""
                            ALTER TABLE {table_name}
                            ADD CONSTRAINT {constraint_name}
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
    
    async def initialize(self) -> None:
        """Initialize database connection and create tables if they don't exist."""
        try:
            await self.connect()
            await self.initialize_database()
            self._log_operation('initialize', {'status': 'success'})
        except Exception as e:
            self._log_operation('initialize', {'status': 'failed', 'error': str(e)})
            raise DatabaseInitializationError(f"Failed to initialize PostgreSQL database: {str(e)}")
    
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
                            if pd.isna(x) or x is None:
                                return None
                            if isinstance(x, (list, dict)):
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
                    'entities',
                    'institutions',
                    'subsidiaries',
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
                                     if 'DATE' in dtype.upper()]
                        for col in date_columns:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col]).dt.date
    
                        # Convert JSON columns
                        json_columns = [col for col, dtype in self.TABLE_SCHEMAS[table].items()
                                     if 'JSONB' in dtype]
                        for col in json_columns:
                            if col in df.columns:
                                def convert_to_json(x):
                                    if pd.isna(x) or x is None:
                                        return None
                                    if isinstance(x, (list, dict)):
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
                                df[col] = df[col].apply(lambda x: x.value if hasattr(x, 'value') else str(x).lower())

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

                        # Convert any remaining dictionary fields to JSON strings
                        for col in df.columns:
                            if col in df.columns and df[col].apply(lambda x: isinstance(x, dict)).any():
                                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    
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

    async def insert_data(self, table_name: str, df: pd.DataFrame) -> None:
        """Insert data into a table."""
        try:
            async with self.pool.acquire() as conn:
                # Convert DataFrame to list of tuples
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.to_records(index=False)]
                
                # Convert UUID strings to UUID objects and handle boolean values
                uuid_columns = ['institution_id', 'account_id', 'owner_id', 'transaction_id', 
                              'subsidiary_id', 'assessment_id', 'person_id', 'document_id', 
                              'presence_id', 'event_id', 'address_id', 'entity_id']
                
                # Get boolean columns from schema
                bool_columns = [col for col, type_ in self.TABLE_SCHEMAS[table_name].items() 
                              if type_.startswith('boolean')]
                
                # Get date columns from schema
                date_columns = [col for col, type_ in self.TABLE_SCHEMAS[table_name].items() 
                              if type_.startswith('date') or type_.startswith('timestamp')]
                
                # Get JSON columns from schema
                json_columns = [col for col, type_ in self.TABLE_SCHEMAS[table_name].items() 
                              if type_.startswith('json')]
                
                # Get numeric columns from schema
                numeric_columns = [col for col, type_ in self.TABLE_SCHEMAS[table_name].items() 
                                 if type_.startswith(('integer', 'numeric', 'decimal'))]
                
                for i, value in enumerate(values):
                    value_list = list(value)
                    for j, col in enumerate(columns):
                        # Handle UUIDs
                        if col in uuid_columns and isinstance(value_list[j], str):
                            value_list[j] = UUID(value_list[j])
                        # Handle booleans
                        elif col in bool_columns:
                            value_list[j] = bool(value_list[j])
                        # Handle dates
                        elif col in date_columns:
                            if isinstance(value_list[j], str):
                                value_list[j] = pd.to_datetime(value_list[j]).to_pydatetime()
                            elif isinstance(value_list[j], pd.Timestamp) or isinstance(value_list[j], np.datetime64):
                                value_list[j] = pd.to_datetime(value_list[j]).to_pydatetime()
                        # Handle JSON fields (convert dicts to JSON strings)
                        elif col in json_columns and isinstance(value_list[j], dict):
                            value_list[j] = json.dumps(value_list[j])
                        # Handle NaN in numeric columns
                        elif col in numeric_columns and pd.isna(value_list[j]):
                            value_list[j] = None
                    values[i] = tuple(value_list)

                # Create placeholders for the VALUES clause
                placeholders = ','.join(f'${i+1}' for i in range(len(columns)))
                
                # Construct and execute the INSERT query
                query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES ({placeholders})
                """
                
                # Execute the query for each row
                for value in values:
                    await conn.execute(query, *value)
                
                self._log_operation('insert_data', {'table': table_name})
        except Exception as e:
            self._log_operation('insert_data', {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to insert data: {str(e)}")

    async def save_batch(self, df_data: Dict[str, pd.DataFrame]) -> None:
        """Save a batch of data to the database."""
        try:
            # Save data in the correct order to respect foreign key constraints
            save_order = [
                'entities',
                'institutions',
                'subsidiaries',
                'addresses',
                'beneficial_owners',
                'accounts',
                'transactions',
                'risk_assessments',
                'compliance_events',
                'authorized_persons',
                'documents',
                'jurisdiction_presences'
            ]
            
            for table in save_order:
                if table in df_data and not df_data[table].empty:
                    await self.insert_data(table, df_data[table])
        except Exception as e:
            raise DatabaseError(f"Failed to save batch: {str(e)}")

    async def execute(self, query: str) -> None:
        """Execute a query without returning results."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query)
        except Exception as e:
            raise DatabaseError(f"Error executing query: {str(e)}")

    async def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        """Fetch all rows from the database."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch(query)
                return [dict(row) for row in result]
        except Exception as e:
            raise DatabaseError(f"Error fetching data: {str(e)}")

    def _log_operation(self, operation: str, details: Dict[str, Any]) -> None:
        """Log database operations."""
        self.logger.info(f"PostgreSQL operation: {operation}", extra=details)
