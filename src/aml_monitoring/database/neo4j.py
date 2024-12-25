"""Neo4j database handler."""

import os
from typing import Dict, List, Any, Optional, Set
import pandas as pd
from neo4j import AsyncGraphDatabase, AsyncDriver
import json
from datetime import datetime

from .base import DatabaseHandler
from .exceptions import ConnectionError, ValidationError, SchemaError, BatchError, DatabaseError

class Neo4jHandler(DatabaseHandler):
    """Handler for Neo4j database operations."""
    
    # Node schemas with their required and optional properties
    NODE_SCHEMAS = {
        'Institution': {
            'primary_key': ['institution_id'],
            'required': [
                'institution_id', 'legal_name', 'business_type', 'incorporation_country',
                'incorporation_date', 'onboarding_date', 'risk_rating', 'operational_status'
            ],
            'optional': [
                'primary_currency', 'regulatory_status', 'primary_business_activity',
                'primary_regulator', 'licenses', 'aml_program_status', 'kyc_refresh_date',
                'last_audit_date', 'next_audit_date', 'relationship_manager',
                'relationship_status', 'swift_code', 'lei_code', 'tax_id', 'website',
                'primary_contact_name', 'primary_contact_email', 'primary_contact_phone',
                'annual_revenue', 'employee_count', 'year_established', 'customer_status',
                'last_review_date', 'industry_codes', 'public_company', 'stock_symbol',
                'stock_exchange'
            ]
        },
        'RiskAssessment': {
            'primary_key': ['assessment_id'],
            'required': [
                'assessment_id', 'entity_id', 'entity_type', 'assessment_date',
                'risk_rating', 'risk_score', 'assessment_type', 'risk_factors'
            ],
            'optional': [
                'conducted_by', 'approved_by', 'findings', 'assessor',
                'next_review_date', 'notes'
            ]
        },
        'Document': {
            'primary_key': ['document_id'],
            'required': [
                'document_id', 'entity_id', 'entity_type', 'document_type',
                'document_number', 'issuing_authority', 'issuing_country',
                'issue_date', 'expiry_date'
            ],
            'optional': [
                'verification_status', 'verification_date', 'document_category', 'notes'
            ]
        },
        'JurisdictionPresence': {
            'primary_key': ['presence_id'],
            'required': [
                'presence_id', 'entity_id', 'entity_type', 'jurisdiction',
                'registration_date', 'effective_from', 'status', 'local_registration_id'
            ],
            'optional': [
                'effective_to', 'local_registration_date', 'local_registration_authority', 'notes'
            ]
        },
        'Account': {
            'primary_key': ['account_id'],
            'required': [
                'account_id', 'entity_id', 'entity_type', 'account_type',
                'account_number', 'currency', 'status', 'opening_date',
                'balance', 'risk_rating'
            ],
            'optional': [
                'last_activity_date', 'purpose', 'average_monthly_balance',
                'custodian_bank', 'account_officer', 'custodian_country'
            ]
        },
        'Transaction': {
            'primary_key': ['transaction_id'],
            'required': [
                'transaction_id', 'transaction_type', 'transaction_date',
                'amount', 'currency', 'transaction_status', 'is_debit',
                'account_id', 'entity_id', 'entity_type'
            ],
            'optional': [
                'counterparty_account', 'counterparty_name', 'counterparty_bank',
                'counterparty_entity_name', 'originating_country', 'destination_country',
                'purpose', 'reference_number', 'screening_alert', 'alert_details',
                'risk_score', 'processing_fee', 'exchange_rate', 'value_date',
                'batch_id', 'check_number', 'wire_reference'
            ]
        }
    }
    
    # Relationship types and their properties
    RELATIONSHIP_DEFINITIONS = {
        'HAS_ACCOUNT': {
            'from_label': 'Institution',
            'to_label': 'Account',
            'properties': ['relationship_type', 'start_date']
        },
        'TRANSACTED': {
            'from_label': 'Account',
            'to_label': 'Transaction',
            'properties': ['transaction_date']
        },
        'OWNED_BY': {
            'from_label': 'Institution',
            'to_label': 'BeneficialOwner',
            'properties': ['ownership_percentage', 'verification_date']
        },
        'HAS_SUBSIDIARY': {
            'from_label': 'Institution',
            'to_label': 'Subsidiary',
            'properties': ['ownership_percentage', 'relationship_start_date']
        },
        'HAS_DOCUMENT': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'Document',
            'properties': ['document_type']
        },
        'HAS_AUTHORIZED_PERSON': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'AuthorizedPerson',
            'properties': ['role', 'authorization_date']
        },
        'INCORPORATED_IN': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'Country',
            'properties': ['incorporation_date']
        },
        'CITIZEN_OF': {
            'from_label': ['BeneficialOwner', 'AuthorizedPerson'],
            'to_label': 'Country',
            'properties': []
        },
        'LOCATED_IN': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'Country',
            'properties': ['location_type', 'start_date']
        },
        'HAS_ADDRESS': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'Address',
            'properties': ['address_type', 'effective_from']
        },
        'TRANSACTED_ON': {
            'from_label': 'Account',
            'to_label': 'BusinessDate',
            'properties': [
                'total_amount', 'transaction_count',
                'alert_count', 'avg_risk_score'
            ]
        },
        'ISSUED_ON': {
            'from_label': 'Document',
            'to_label': 'BusinessDate',
            'properties': []
        },
        'OPENED_ON': {
            'from_label': 'Account',
            'to_label': 'BusinessDate',
            'properties': []
        },
        'BELONGS_TO': {
            'from_label': 'Transaction',
            'to_label': 'Account',
            'properties': []
        }
    }

    def __init__(self):
        """Initialize Neo4j handler."""
        super().__init__()
        self.driver: Optional[AsyncDriver] = None
    
    async def connect(self) -> None:
        """Establish database connection."""
        try:
            host = os.getenv('NEO4J_HOST', 'localhost')
            port = os.getenv('NEO4J_PORT', '7687')
            user = os.getenv('NEO4J_USER', 'neo4j')
            password = os.getenv('NEO4J_PASSWORD', '')
            
            uri = f'neo4j://{host}:{port}'
            self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
            
            # Test connection
            async with self.driver.session() as session:
                result = await session.run("RETURN 1")
                await result.consume()
            
            self.is_connected = True
            self._log_operation('connect', {'status': 'success'})
            
        except Exception as e:
            self._log_operation('connect', {'status': 'failed', 'error': str(e)})
            raise ConnectionError(f"Failed to connect to Neo4j: {str(e)}")
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self.driver:
            await self.driver.close()
            self.is_connected = False
            self._log_operation('disconnect', {'status': 'success'})
    
    async def validate_schema(self) -> bool:
        """Validate database schema (constraints and indexes)."""
        try:
            async with self.driver.session() as session:
                # Check indexes and constraints
                result = await session.run("""
                    SHOW INDEXES
                    YIELD name, labelsOrTypes, properties
                    RETURN collect({
                        name: name,
                        labels: labelsOrTypes,
                        properties: properties
                    }) as indexes
                """)
                indexes = await result.single()
                indexes = indexes['indexes'] if indexes else []

                # First create any missing indexes
                for label, definition in self.NODE_SCHEMAS.items():
                    for prop in definition['required']:
                        # Check if index exists for this property
                        index_exists = False
                        for index in indexes:
                            labels = index.get('labels', [])
                            properties = index.get('properties', [])
                            if (labels and properties and 
                                label in labels and 
                                prop in properties):
                                index_exists = True
                                break

                        if not index_exists:
                            # Create the missing index
                            await session.run(f"""
                                CREATE INDEX {label.lower()}_{prop}_idx
                                IF NOT EXISTS
                                FOR (n:{label})
                                ON (n.{prop})
                            """)

            return True

        except Exception as e:
            self._log_operation('validate_schema',
                              {'status': 'failed', 'error': str(e)})
            raise SchemaError(f"Schema validation failed: {str(e)}")
    
    async def create_schema(self) -> None:
        """Create database schema (constraints and indexes)."""
        try:
            async with self.driver.session() as session:
                # Create constraints and indexes for each node type
                for label, definition in self.NODE_SCHEMAS.items():
                    # Create unique constraints for primary keys
                    for prop in definition['primary_key']:
                        await session.run(f"""
                            CREATE CONSTRAINT {label.lower()}_{prop}_unique
                            IF NOT EXISTS
                            FOR (n:{label})
                            REQUIRE n.{prop} IS UNIQUE
                        """)

                    # Create indexes for required fields
                    for prop in definition['required']:
                        await session.run(f"""
                            CREATE INDEX {label.lower()}_{prop}_idx
                            IF NOT EXISTS
                            FOR (n:{label})
                            ON (n.{prop})
                        """)

            self._log_operation('create_schema', {'status': 'success'})

        except Exception as e:
            self._log_operation('create_schema',
                              {'status': 'failed', 'error': str(e)})
            raise SchemaError(f"Failed to create schema: {str(e)}")
    
    async def _validate_dataframe_schema(self, node_type: str, df: pd.DataFrame) -> None:
        """Validate DataFrame schema for a specific node type."""
        if node_type not in self.NODE_SCHEMAS:
            raise ValidationError(f"Invalid node type: {node_type}")

        required_fields = set(self.NODE_SCHEMAS[node_type]['required'])
        df_fields = set(df.columns)

        missing_fields = required_fields - df_fields
        if missing_fields:
            raise ValidationError(f"Missing required fields in {node_type}: {missing_fields}")

    async def save_batch(self, data: Dict[str, pd.DataFrame], batch_size: int = 1000) -> None:
        """Save a batch of data to Neo4j."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")

        # Normalize table names to lowercase
        normalized_data = {}
        for table_name, df in data.items():
            normalized_name = table_name.lower()
            if normalized_name.endswith('s'):
                normalized_data[normalized_name] = df
            else:
                normalized_data[f"{normalized_name}s"] = df

        # Validate data first - let validation errors propagate up
        await self.validate_data(normalized_data)

        try:
            async with self.driver.session() as session:
                # Save institutions first (they are the root nodes)
                if 'institutions' in normalized_data and not normalized_data['institutions'].empty:
                    for i in range(0, len(normalized_data['institutions']), batch_size):
                        batch = normalized_data['institutions'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MERGE (i:Institution {institution_id: $id})
                                ON CREATE SET
                                    i.legal_name = $name,
                                    i.business_type = $business_type,
                                    i.incorporation_country = $country,
                                    i.incorporation_date = $inc_date,
                                    i.operational_status = $status,
                                    i.regulatory_status = $reg_status,
                                    i.licenses = $licenses,
                                    i.industry_codes = $industry_codes,
                                    i.public_company = $public,
                                    i.stock_symbol = $symbol,
                                    i.stock_exchange = $exchange
                                ON MATCH SET
                                    i.legal_name = $name,
                                    i.business_type = $business_type,
                                    i.incorporation_country = $country,
                                    i.incorporation_date = $inc_date,
                                    i.operational_status = $status,
                                    i.regulatory_status = $reg_status,
                                    i.licenses = $licenses,
                                    i.industry_codes = $industry_codes,
                                    i.public_company = $public,
                                    i.stock_symbol = $symbol,
                                    i.stock_exchange = $exchange
                                WITH i
                                MERGE (c:Country {code: $country})
                                MERGE (i)-[:INCORPORATED_IN {date: $inc_date}]->(c)
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
                                'symbol': row.get('stock_symbol'),
                                'exchange': row.get('stock_exchange')
                            })
                
                # Save subsidiaries
                if 'subsidiaries' in normalized_data and not normalized_data['subsidiaries'].empty:
                    for i in range(0, len(normalized_data['subsidiaries']), batch_size):
                        batch = normalized_data['subsidiaries'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (p:Institution {institution_id: $parent_id})
                                MERGE (s:Subsidiary {subsidiary_id: $id})
                                ON CREATE SET
                                    s.legal_name = $name,
                                    s.business_type = $business_type,
                                    s.incorporation_country = $country,
                                    s.incorporation_date = $inc_date,
                                    s.operational_status = $status,
                                    s.regulatory_status = $reg_status,
                                    s.local_licenses = $licenses,
                                    s.industry_codes = $industry_codes,
                                    s.is_regulated = $is_regulated
                                ON MATCH SET
                                    s.legal_name = $name,
                                    s.business_type = $business_type,
                                    s.incorporation_country = $country,
                                    s.incorporation_date = $inc_date,
                                    s.operational_status = $status,
                                    s.regulatory_status = $reg_status,
                                    s.local_licenses = $licenses,
                                    s.industry_codes = $industry_codes,
                                    s.is_regulated = $is_regulated
                                WITH p, s
                                MERGE (p)-[:HAS_SUBSIDIARY {
                                    ownership_percentage: $ownership,
                                    relationship_start_date: $inc_date
                                }]->(s)
                                WITH s
                                MERGE (c:Country {code: $country})
                                MERGE (s)-[:INCORPORATED_IN {date: $inc_date}]->(c)
                            """, {
                                'id': str(row['subsidiary_id']),
                                'parent_id': str(row['parent_institution_id']),
                                'name': row['legal_name'],
                                'business_type': row['business_type'],
                                'country': row['incorporation_country'],
                                'inc_date': row['incorporation_date'],
                                'status': row['operational_status'],
                                'reg_status': row.get('regulatory_status'),
                                'licenses': json.dumps(row.get('local_licenses', [])),
                                'industry_codes': json.dumps(row.get('industry_codes', [])),
                                'is_regulated': bool(row.get('is_regulated', False)),
                                'ownership': float(row.get('ownership_percentage', 100.0))
                            })
                
                # Save accounts
                if 'accounts' in normalized_data and not normalized_data['accounts'].empty:
                    for i in range(0, len(normalized_data['accounts']), batch_size):
                        batch = normalized_data['accounts'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MERGE (a:Account {account_id: $id})
                                ON CREATE SET
                                    a.account_number = $number,
                                    a.account_type = $type,
                                    a.balance = $balance,
                                    a.currency = $currency,
                                    a.status = $status,
                                    a.open_date = $open_date,
                                    a.close_date = $close_date,
                                    a.risk_rating = $risk_rating
                                ON MATCH SET
                                    a.account_number = $number,
                                    a.account_type = $type,
                                    a.balance = $balance,
                                    a.currency = $currency,
                                    a.status = $status,
                                    a.open_date = $open_date,
                                    a.close_date = $close_date,
                                    a.risk_rating = $risk_rating
                                WITH a
                                MATCH (i:Institution {institution_id: $entity_id})
                                MERGE (a)-[:BELONGS_TO]->(i)
                            """, {
                                'id': str(row['account_id']),
                                'number': row['account_number'],
                                'type': row['account_type'],
                                'balance': float(row['balance']),
                                'currency': row['currency'],
                                'status': row.get('status'),
                                'open_date': row.get('open_date'),
                                'close_date': row.get('close_date'),
                                'risk_rating': row.get('risk_rating'),
                                'entity_id': str(row['entity_id'])
                            })

                # Save transactions
                if 'transactions' in normalized_data and not normalized_data['transactions'].empty:
                    for i in range(0, len(normalized_data['transactions']), batch_size):
                        batch = normalized_data['transactions'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                CREATE (t:Transaction {
                                    transaction_id: $id,
                                    amount: $amount,
                                    currency: $currency,
                                    transaction_type: $type,
                                    transaction_status: $status,
                                    transaction_date: $date,
                                    description: $desc
                                })
                                WITH t
                                MATCH (a:Account {account_id: $account_id})
                                MERGE (t)-[:BELONGS_TO]->(a)
                                WITH t
                                MATCH (bd:BusinessDate {date: $date})
                                MERGE (t)-[:OCCURRED_ON]->(bd)
                            """, {
                                'id': str(row['transaction_id']),
                                'amount': float(row['amount']),
                                'currency': row['currency'],
                                'type': row['transaction_type'],
                                'status': row['transaction_status'],
                                'date': row['transaction_date'],
                                'desc': row.get('description'),
                                'account_id': str(row['account_id'])
                            })

                # Save beneficial owners
                if 'beneficial_owners' in normalized_data and not normalized_data['beneficial_owners'].empty:
                    for i in range(0, len(normalized_data['beneficial_owners']), batch_size):
                        batch = normalized_data['beneficial_owners'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (bo:BeneficialOwner {owner_id: $id})
                                ON CREATE SET
                                    bo.name = $name,
                                    bo.nationality = $nationality,
                                    bo.country_of_residence = $residence,
                                    bo.ownership_percentage = $ownership,
                                    bo.dob = $dob,
                                    bo.verification_date = $verification_date,
                                    bo.pep_status = $pep,
                                    bo.sanctions_status = $sanctions,
                                    bo.adverse_media_status = $adverse_media
                                ON MATCH SET
                                    bo.name = $name,
                                    bo.nationality = $nationality,
                                    bo.country_of_residence = $residence,
                                    bo.ownership_percentage = $ownership,
                                    bo.dob = $dob,
                                    bo.verification_date = $verification_date,
                                    bo.pep_status = $pep,
                                    bo.sanctions_status = $sanctions,
                                    bo.adverse_media_status = $adverse_media
                                WITH e, bo
                                MERGE (e)-[:OWNED_BY {
                                    ownership_percentage: $ownership,
                                    verification_date: $verification_date
                                }]->(bo)
                                WITH bo
                                MERGE (c:Country {code: $nationality})
                                MERGE (bo)-[:CITIZEN_OF]->(c)
                            """, {
                                'id': str(row['owner_id']),
                                'entity_id': str(row['entity_id']),
                                'name': row['name'],
                                'nationality': row['nationality'],
                                'residence': row['country_of_residence'],
                                'ownership': float(row['ownership_percentage']),
                                'dob': row['dob'],
                                'verification_date': row['verification_date'],
                                'pep': bool(row['pep_status']),
                                'sanctions': bool(row['sanctions_status']),
                                'adverse_media': bool(row['adverse_media_status'])
                            })
                
                # Save addresses
                if 'addresses' in normalized_data and not normalized_data['addresses'].empty:
                    for i in range(0, len(normalized_data['addresses']), batch_size):
                        batch = normalized_data['addresses'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (a:Address {address_id: $id})
                                ON CREATE SET
                                    a.address_type = $type,
                                    a.address_line1 = $line1,
                                    a.address_line2 = $line2,
                                    a.city = $city,
                                    a.state_province = $state,
                                    a.postal_code = $postal,
                                    a.country = $country,
                                    a.status = $status,
                                    a.effective_from = $from_date,
                                    a.effective_to = $to_date,
                                    a.primary_address = $primary
                                ON MATCH SET
                                    a.address_type = $type,
                                    a.address_line1 = $line1,
                                    a.address_line2 = $line2,
                                    a.city = $city,
                                    a.state_province = $state,
                                    a.postal_code = $postal,
                                    a.country = $country,
                                    a.status = $status,
                                    a.effective_from = $from_date,
                                    a.effective_to = $to_date,
                                    a.primary_address = $primary
                                WITH e, a
                                MERGE (e)-[:HAS_ADDRESS {
                                    address_type: $type,
                                    effective_from: $from_date
                                }]->(a)
                                WITH a
                                MERGE (c:Country {code: $country})
                                MERGE (a)-[:LOCATED_IN]->(c)
                            """, {
                                'id': str(row['address_id']),
                                'entity_id': str(row['entity_id']),
                                'type': row['address_type'],
                                'line1': row['address_line1'],
                                'line2': row.get('address_line2'),
                                'city': row['city'],
                                'state': row.get('state_province'),
                                'postal': row.get('postal_code'),
                                'country': row['country'],
                                'status': row['status'],
                                'from_date': row['effective_from'],
                                'to_date': row.get('effective_to'),
                                'primary': bool(row.get('primary_address', False))
                            })
            
            self._log_operation('save_batch', {'status': 'success'})
            
        except Exception as e:
            self._log_operation('save_batch', 
                              {'status': 'failed', 'error': str(e)})
            raise BatchError(f"Failed to save batch: {str(e)}", [])
    
    async def wipe_clean(self) -> None:
        """Wipe all data from the database while preserving indexes and constraints."""
        try:
            if not self.is_connected:
                raise ConnectionError("Not connected to database")

            async with self.driver.session() as session:
                # Delete all nodes and relationships
                await session.run("""
                    MATCH (n)
                    DETACH DELETE n
                """)
            
            self._log_operation('wipe_clean', {'status': 'success'})
            
        except Exception as e:
            self._log_operation('wipe_clean', 
                              {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to wipe database: {str(e)}")
    
    async def healthcheck(self) -> bool:
        """Check database health."""
        try:
            if not self.is_connected or not self.driver:
                return False
                
            async with self.driver.session() as session:
                result = await session.run("RETURN 1")
                value = await result.single()
                return value[0] == 1
                
        except Exception as e:
            self._log_operation('healthcheck', 
                              {'status': 'failed', 'error': str(e)})
            return False
    
    def get_required_fields(self, table_name: str) -> Set[str]:
        """Get required fields for a table."""
        required_fields = {
            'institutions': {
                'institution_id', 'legal_name', 'business_type', 'incorporation_country',
                'incorporation_date', 'operational_status', 'regulatory_status',
                'licenses', 'industry_codes', 'public_company'
            },
            'accounts': {
                'account_id', 'account_number', 'account_type', 'balance',
                'currency', 'status', 'institution_id'
            },
            'transactions': {
                'transaction_id', 'account_id', 'transaction_type', 'transaction_date',
                'amount', 'currency', 'transaction_status', 'is_debit'
            },
            'beneficial_owners': {
                'owner_id', 'institution_id', 'name', 'ownership_percentage',
                'nationality', 'pep_status'
            },
            'addresses': {
                'address_id', 'institution_id', 'address_type', 'country',
                'city', 'postal_code'
            },
            'risk_assessments': {
                'assessment_id', 'institution_id', 'assessment_date',
                'risk_factors', 'risk_rating'
            },
            'authorized_persons': {
                'person_id', 'institution_id', 'name', 'title',
                'authorization_date', 'authorization_type'
            },
            'documents': {
                'document_id', 'institution_id', 'document_type',
                'issue_date', 'expiry_date', 'status'
            },
            'jurisdiction_presences': {
                'presence_id', 'institution_id', 'country',
                'presence_type', 'registration_date'
            },
            'compliance_events': {
                'event_id', 'institution_id', 'event_type',
                'event_date', 'severity', 'status'
            }
        }
        return required_fields.get(table_name, set())

    def _convert_enum_to_str(self, value: Any) -> str:
        """Convert enum values to strings for Neo4j storage."""
        if hasattr(value, 'value'):  # Check if it's an enum
            return value.value
        return value

    def _prepare_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare properties for Neo4j storage by converting enums to strings."""
        return {k: self._convert_enum_to_str(v) for k, v in properties.items()}

    async def create_node(self, label: str, properties: Dict[str, Any]) -> None:
        """Create a node with the given label and properties."""
        try:
            # Validate required properties
            if label not in self.NODE_SCHEMAS:
                raise ValidationError(f"Invalid node label: {label}")
            
            required_props = self.NODE_SCHEMAS[label]['required']
            for prop in required_props:
                if prop not in properties:
                    raise ValidationError(f"Missing required property: {prop}")
            
            # Convert enum values to strings
            prepared_properties = self._prepare_properties(properties)
            
            # Create node
            async with self.driver.session() as session:
                query = (
                    f"CREATE (n:{label}) "
                    f"SET n = $properties "
                    f"RETURN n"
                )
                await session.run(query, properties=prepared_properties)
            
            self._log_operation('create_node', 
                              {'label': label, 'properties': prepared_properties})
            
        except Exception as e:
            self._log_operation('create_node', 
                              {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to create node: {str(e)}")
