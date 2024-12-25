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
        'BeneficialOwner': {
            'primary_key': ['owner_id'],
            'required': [
                'owner_id', 'entity_id', 'entity_type', 'name', 'nationality',
                'country_of_residence', 'ownership_percentage', 'dob',
                'verification_date', 'pep_status', 'sanctions_status'
            ],
            'optional': []
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
        },
        'Subsidiary': {
            'primary_key': ['subsidiary_id'],
            'required': [
                'subsidiary_id', 'parent_institution_id', 'legal_name', 'tax_id',
                'incorporation_country', 'incorporation_date', 'acquisition_date',
                'business_type', 'operational_status', 'parent_ownership_percentage',
                'consolidation_status', 'capital_investment', 'functional_currency',
                'material_subsidiary', 'risk_classification', 'regulatory_status',
                'local_licenses', 'integration_status', 'financial_metrics',
                'reporting_frequency', 'requires_local_audit', 'corporate_governance_model',
                'is_regulated', 'is_customer', 'industry_codes', 'customer_id',
                'customer_onboarding_date', 'customer_risk_rating', 'customer_status'
            ],
            'optional': []
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
            'properties': ['title', 'authorization_date']
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
        },
        'HAS_RISK_ASSESSMENT': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'RiskAssessment',
            'properties': []
        },
        'HAS_AUTHORIZED_PERSON': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'AuthorizedPerson',
            'properties': ['title', 'authorization_date']
        },
        'HAS_DOCUMENT': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'Document',
            'properties': ['document_type']
        },
        'HAS_COMPLIANCE_EVENT': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'ComplianceEvent',
            'properties': []
        },
        'RELATED_TO': {
            'from_label': 'ComplianceEvent',
            'to_label': 'Account',
            'properties': []
        },
        'OWNS_SUBSIDIARY': {
            'from_label': 'Institution',
            'to_label': 'Subsidiary',
            'properties': ['ownership_percentage', 'acquisition_date']
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
            
            uri = f'bolt://{host}:{port}'
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
                                WITH i, $inc_date as inc_date
                                WHERE inc_date IS NOT NULL
                                MERGE (bd:BusinessDate {date: inc_date})
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
                                    s.tax_id = $tax_id,
                                    s.incorporation_country = $country,
                                    s.incorporation_date = $inc_date,
                                    s.acquisition_date = $acq_date,
                                    s.business_type = $type,
                                    s.operational_status = $status,
                                    s.parent_ownership_percentage = $ownership,
                                    s.consolidation_status = $consolidation,
                                    s.capital_investment = $capital,
                                    s.functional_currency = $currency,
                                    s.material_subsidiary = $material,
                                    s.risk_classification = $risk,
                                    s.regulatory_status = $reg_status,
                                    s.local_licenses = $licenses,
                                    s.integration_status = $integration,
                                    s.financial_metrics = $metrics,
                                    s.reporting_frequency = $reporting,
                                    s.requires_local_audit = $audit,
                                    s.corporate_governance_model = $governance,
                                    s.is_regulated = $regulated,
                                    s.is_customer = $customer,
                                    s.industry_codes = $industry,
                                    s.customer_id = $cust_id,
                                    s.customer_onboarding_date = $onboard_date,
                                    s.customer_risk_rating = $cust_risk,
                                    s.customer_status = $cust_status
                                ON MATCH SET
                                    s.legal_name = $name,
                                    s.tax_id = $tax_id,
                                    s.incorporation_country = $country,
                                    s.incorporation_date = $inc_date,
                                    s.acquisition_date = $acq_date,
                                    s.business_type = $type,
                                    s.operational_status = $status,
                                    s.parent_ownership_percentage = $ownership,
                                    s.consolidation_status = $consolidation,
                                    s.capital_investment = $capital,
                                    s.functional_currency = $currency,
                                    s.material_subsidiary = $material,
                                    s.risk_classification = $risk,
                                    s.regulatory_status = $reg_status,
                                    s.local_licenses = $licenses,
                                    s.integration_status = $integration,
                                    s.financial_metrics = $metrics,
                                    s.reporting_frequency = $reporting,
                                    s.requires_local_audit = $audit,
                                    s.corporate_governance_model = $governance,
                                    s.is_regulated = $regulated,
                                    s.is_customer = $customer,
                                    s.industry_codes = $industry,
                                    s.customer_id = $cust_id,
                                    s.customer_onboarding_date = $onboard_date,
                                    s.customer_risk_rating = $cust_risk,
                                    s.customer_status = $cust_status
                                WITH p, s
                                MERGE (p)-[:OWNS_SUBSIDIARY {
                                    ownership_percentage: $ownership,
                                    acquisition_date: $acq_date
                                }]->(s)
                                WITH s
                                MERGE (c:Country {code: $country})
                                MERGE (s)-[:INCORPORATED_IN]->(c)
                                WITH s, $inc_date as inc_date
                                WHERE inc_date IS NOT NULL
                                MERGE (bd:BusinessDate {date: inc_date})
                                MERGE (s)-[:INCORPORATED_ON]->(bd)
                            """, {
                                'id': str(row['subsidiary_id']),
                                'parent_id': str(row['parent_institution_id']),
                                'name': row['legal_name'],
                                'tax_id': row['tax_id'],
                                'country': row['incorporation_country'],
                                'inc_date': row['incorporation_date'],
                                'acq_date': row['acquisition_date'],
                                'type': row['business_type'],
                                'status': row['operational_status'],
                                'ownership': float(row['parent_ownership_percentage']),
                                'consolidation': row['consolidation_status'],
                                'capital': float(row['capital_investment']),
                                'currency': row['functional_currency'],
                                'material': bool(row['material_subsidiary']),
                                'risk': row['risk_classification'],
                                'reg_status': row['regulatory_status'],
                                'licenses': json.dumps(row['local_licenses']),
                                'integration': row['integration_status'],
                                'metrics': json.dumps(row['financial_metrics']),
                                'reporting': row['reporting_frequency'],
                                'audit': bool(row['requires_local_audit']),
                                'governance': row['corporate_governance_model'],
                                'regulated': bool(row['is_regulated']),
                                'customer': bool(row['is_customer']),
                                'industry': json.dumps(row.get('industry_codes', [])),
                                'cust_id': row.get('customer_id'),
                                'onboard_date': row.get('customer_onboarding_date'),
                                'cust_risk': row.get('customer_risk_rating'),
                                'cust_status': row.get('customer_status')
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
                                WITH a, $open_date as open_date
                                WHERE open_date IS NOT NULL
                                MERGE (bd:BusinessDate {date: open_date})
                                MERGE (a)-[:OPENED_ON]->(bd)
                                WITH a
                                MATCH (i:Institution {institution_id: $entity_id})
                                MERGE (i)-[:HAS_ACCOUNT]->(a)
                                WITH a
                                MERGE (bd:BusinessDate {date: $open_date})
                                MERGE (a)-[:OPENED_ON]->(bd)
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
                                MERGE (a)-[:TRANSACTED]->(t)
                                WITH t, $date as date
                                WHERE date IS NOT NULL
                                MERGE (bd:BusinessDate {date: date})
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
                                    bo.sanctions_status = $sanctions
                                ON MATCH SET
                                    bo.name = $name,
                                    bo.nationality = $nationality,
                                    bo.country_of_residence = $residence,
                                    bo.ownership_percentage = $ownership,
                                    bo.dob = $dob,
                                    bo.verification_date = $verification_date,
                                    bo.pep_status = $pep,
                                    bo.sanctions_status = $sanctions
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
                                'sanctions': bool(row['sanctions_status'])
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
                                    a.country = $country
                                ON MATCH SET
                                    a.address_type = $type,
                                    a.address_line1 = $line1,
                                    a.address_line2 = $line2,
                                    a.city = $city,
                                    a.state_province = $state,
                                    a.postal_code = $postal,
                                    a.country = $country
                                WITH e, a
                                MERGE (e)-[:HAS_ADDRESS {
                                    address_type: $type,
                                    effective_from: $effective_from
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
                                'effective_from': row['effective_from']
                            })

                # Save risk assessments
                if 'risk_assessments' in normalized_data and not normalized_data['risk_assessments'].empty:
                    for i in range(0, len(normalized_data['risk_assessments']), batch_size):
                        batch = normalized_data['risk_assessments'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (r:RiskAssessment {assessment_id: $id})
                                ON CREATE SET
                                    r.assessment_date = $date,
                                    r.risk_rating = $rating,
                                    r.risk_score = $score,
                                    r.assessment_type = $type,
                                    r.risk_factors = $factors,
                                    r.conducted_by = $assessor,
                                    r.next_review_date = $next_review
                                ON MATCH SET
                                    r.assessment_date = $date,
                                    r.risk_rating = $rating,
                                    r.risk_score = $score,
                                    r.assessment_type = $type,
                                    r.risk_factors = $factors,
                                    r.conducted_by = $assessor,
                                    r.next_review_date = $next_review
                                WITH e, r
                                MERGE (e)-[:HAS_RISK_ASSESSMENT]->(r)
                                WITH r, $date as date
                                WHERE date IS NOT NULL
                                MERGE (bd:BusinessDate {date: date})
                                MERGE (r)-[:ASSESSED_ON]->(bd)
                            """, {
                                'id': str(row['assessment_id']),
                                'entity_id': str(row['entity_id']),
                                'date': row['assessment_date'],
                                'rating': row['risk_rating'],
                                'score': float(row['risk_score']),
                                'type': row['assessment_type'],
                                'factors': json.dumps(row['risk_factors']),
                                'assessor': row.get('conducted_by'),
                                'next_review': row.get('next_review_date')
                            })

                # Save authorized persons
                if 'authorized_persons' in normalized_data and not normalized_data['authorized_persons'].empty:
                    for i in range(0, len(normalized_data['authorized_persons']), batch_size):
                        batch = normalized_data['authorized_persons'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (p:AuthorizedPerson {person_id: $id})
                                ON CREATE SET
                                    p.name = $name,
                                    p.title = $title,
                                    p.authorization_level = $auth_level,
                                    p.authorization_type = $auth_type,
                                    p.authorization_start = $auth_start,
                                    p.authorization_end = $auth_end,
                                    p.contact_info = $contact_info,
                                    p.is_active = $active,
                                    p.last_verification_date = $verification_date
                                ON MATCH SET
                                    p.name = $name,
                                    p.title = $title,
                                    p.authorization_level = $auth_level,
                                    p.authorization_type = $auth_type,
                                    p.authorization_start = $auth_start,
                                    p.authorization_end = $auth_end,
                                    p.contact_info = $contact_info,
                                    p.is_active = $active,
                                    p.last_verification_date = $verification_date
                                WITH e, p
                                MERGE (e)-[:HAS_AUTHORIZED_PERSON {
                                    title: $title,
                                    authorization_date: $auth_start
                                }]->(p)
                                WITH p
                                MERGE (c:Country {code: $nationality})
                                MERGE (p)-[:CITIZEN_OF]->(c)
                            """, {
                                'id': str(row['person_id']),
                                'entity_id': str(row['entity_id']),
                                'name': row['name'],
                                'title': row['title'],
                                'auth_level': row['authorization_level'],
                                'auth_type': row['authorization_type'],
                                'auth_start': row['authorization_start'],
                                'auth_end': row.get('authorization_end'),
                                'contact_info': json.dumps(row['contact_info']),
                                'active': bool(row['is_active']),
                                'verification_date': row.get('last_verification_date'),
                                'nationality': row.get('nationality', 'US')  # Default to US if not specified
                            })

                # Save documents
                if 'documents' in normalized_data and not normalized_data['documents'].empty:
                    for i in range(0, len(normalized_data['documents']), batch_size):
                        batch = normalized_data['documents'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (d:Document {document_id: $id})
                                ON CREATE SET
                                    d.document_type = $type,
                                    d.document_number = $number,
                                    d.issuing_authority = $authority,
                                    d.issuing_country = $country,
                                    d.issue_date = $issue_date,
                                    d.expiry_date = $expiry_date,
                                    d.verification_status = $status,
                                    d.verification_date = $verification_date
                                ON MATCH SET
                                    d.document_type = $type,
                                    d.document_number = $number,
                                    d.issuing_authority = $authority,
                                    d.issuing_country = $country,
                                    d.issue_date = $issue_date,
                                    d.expiry_date = $expiry_date,
                                    d.verification_status = $status,
                                    d.verification_date = $verification_date
                                WITH e, d
                                MERGE (e)-[:HAS_DOCUMENT {document_type: $type}]->(d)
                                WITH d, $issue_date as issue_date
                                WHERE issue_date IS NOT NULL
                                MERGE (bd:BusinessDate {date: issue_date})
                                MERGE (d)-[:ISSUED_ON]->(bd)
                                WITH d
                                MERGE (c:Country {code: $country})
                                MERGE (d)-[:ISSUED_IN]->(c)
                            """, {
                                'id': str(row['document_id']),
                                'entity_id': str(row['entity_id']),
                                'type': row['document_type'],
                                'number': row['document_number'],
                                'authority': row['issuing_authority'],
                                'country': row['issuing_country'],
                                'issue_date': row['issue_date'],
                                'expiry_date': row['expiry_date'],
                                'status': row['verification_status'],
                                'verification_date': row.get('verification_date')
                            })

                # Save compliance events
                if 'compliance_events' in normalized_data and not normalized_data['compliance_events'].empty:
                    for i in range(0, len(normalized_data['compliance_events']), batch_size):
                        batch = normalized_data['compliance_events'].iloc[i:i + batch_size]
                        for _, row in batch.iterrows():
                            await session.run("""
                                MATCH (e) 
                                WHERE (e:Institution OR e:Subsidiary) AND 
                                      (e.institution_id = $entity_id OR e.subsidiary_id = $entity_id)
                                MERGE (ce:ComplianceEvent {event_id: $id})
                                ON CREATE SET
                                    ce.event_type = $type,
                                    ce.event_date = $date,
                                    ce.severity = $severity,
                                    ce.status = $status,
                                    ce.description = $desc,
                                    ce.reported_by = $reporter,
                                    ce.resolution = $resolution,
                                    ce.resolution_date = $resolution_date
                                ON MATCH SET
                                    ce.event_type = $type,
                                    ce.event_date = $date,
                                    ce.severity = $severity,
                                    ce.status = $status,
                                    ce.description = $desc,
                                    ce.reported_by = $reporter,
                                    ce.resolution = $resolution,
                                    ce.resolution_date = $resolution_date
                                WITH e, ce
                                MERGE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                                WITH ce, $date as date
                                WHERE date IS NOT NULL
                                MERGE (bd:BusinessDate {date: date})
                                MERGE (ce)-[:OCCURRED_ON]->(bd)
                                WITH ce
                                MATCH (a:Account {account_id: $related_account_id})
                                MERGE (ce)-[:RELATED_TO]->(a)
                            """, {
                                'id': str(row['event_id']),
                                'entity_id': str(row['entity_id']),
                                'related_account_id': str(row['related_account_id']),
                                'type': row['event_type'],
                                'date': row['event_date'],
                                'severity': row.get('severity', 'medium'),
                                'status': row.get('status', 'open'),
                                'desc': row['event_description'],
                                'reporter': row.get('decision_maker'),
                                'resolution': row.get('decision'),
                                'resolution_date': row.get('decision_date')
                            })

            self._log_operation('save_batch', {'status': 'success'})
            
        except Exception as e:
            self._log_operation('save_batch', 
                              {'status': 'failed', 'error': str(e)})
            raise BatchError(f"Failed to save batch: {str(e)}", [])
    
    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]) -> None:
        """Save data to Neo4j database."""
        await self.save_batch(data)
    
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
                'account_id', 'entity_id', 'entity_type', 'account_type',
                'account_number', 'currency', 'status', 'balance'
            },
            'transactions': {
                'transaction_id', 'account_id', 'transaction_type', 'transaction_date',
                'amount', 'currency', 'transaction_status', 'is_debit'
            },
            'beneficial_owners': {
                'owner_id', 'entity_id', 'entity_type', 'name',
                'ownership_percentage', 'nationality', 'pep_status'
            },
            'addresses': {
                'address_id', 'entity_id', 'entity_type', 'address_type',
                'country', 'city', 'postal_code'
            },
            'risk_assessments': {
                'assessment_id', 'entity_id', 'assessment_date',
                'risk_factors', 'risk_rating'
            },
            'authorized_persons': {
                'person_id', 'entity_id', 'name', 'title',
                'authorization_start', 'authorization_type'
            },
            'documents': {
                'document_id', 'entity_id', 'document_type',
                'issue_date', 'expiry_date', 'verification_status'
            },
            'jurisdiction_presences': {
                'presence_id', 'entity_id', 'jurisdiction',
                'registration_date', 'status'
            },
            'compliance_events': {
                'event_id', 'entity_id', 'event_type',
                'event_date', 'event_description', 'new_state'
            },
            'subsidiaries': {
                'subsidiary_id', 'parent_institution_id', 'legal_name', 'tax_id',
                'incorporation_country', 'incorporation_date', 'acquisition_date',
                'business_type', 'operational_status', 'parent_ownership_percentage',
                'consolidation_status', 'capital_investment', 'functional_currency',
                'material_subsidiary', 'risk_classification', 'regulatory_status',
                'local_licenses', 'integration_status', 'financial_metrics',
                'reporting_frequency', 'requires_local_audit', 'corporate_governance_model',
                'is_regulated', 'is_customer', 'industry_codes', 'customer_id',
                'customer_onboarding_date', 'customer_risk_rating', 'customer_status'
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
