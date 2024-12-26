"""Neo4j database handler."""

import os
from typing import Dict, List, Any, Optional, Set
import pandas as pd
from neo4j import AsyncGraphDatabase, AsyncDriver
import json
from datetime import datetime, date
from uuid import UUID
from enum import Enum

from .base import DatabaseHandler
from .exceptions import ConnectionError, ValidationError, SchemaError, BatchError, DatabaseError, DatabaseInitializationError

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
                'account_id', 'entity_id', 'entity_type', 'debit_account_id', 'credit_account_id'
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
                'subsidiary_id', 'parent_institution_id', 'business_type',
                'capital_investment', 'consolidation_status', 'acquisition_date',
                'parent_ownership_percentage', 'created_at', 'updated_at',
                'revenue', 'assets', 'liabilities'
            ],
            'optional': [
                'deleted_at', 'is_customer', 'customer_id',
                'customer_onboarding_date', 'customer_risk_rating', 'customer_status'
            ]
        },
        'Entity': {
            'primary_key': ['entity_id'],
            'required': [
                'entity_id', 'entity_type', 'created_at', 'updated_at'
            ],
            'optional': [
                'parent_entity_id', 'deleted_at'
            ]
        },
        'Address': {
            'primary_key': ['address_id'],
            'required': [
                'address_id', 'address_line1', 'address_type',
                'city', 'country', 'postal_code'
            ],
            'optional': [
                'address_line2', 'state_province', 'region',
                'effective_from', 'effective_to'
            ]
        },
        'ComplianceEvent': {
            'primary_key': ['event_id'],
            'required': [
                'event_id', 'entity_id', 'event_type', 'event_date',
                'event_description', 'new_state'
            ],
            'optional': []
        },
        'AuthorizedPerson': {
            'primary_key': ['person_id'],
            'required': [
                'person_id', 'entity_id', 'name', 'title',
                'authorization_start', 'authorization_type'
            ],
            'optional': []
        },
        'Country': {
            'primary_key': ['code'],
            'required': [
                'code'
            ],
            'optional': []
        },
        'BusinessDate': {
            'primary_key': ['date'],
            'required': [
                'date'
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
        'OWNS_SUBSIDIARY': {
            'from_label': 'Institution',
            'to_label': 'Subsidiary',
            'properties': ['ownership_percentage', 'acquisition_date']
        },
        'IS_CUSTOMER': {
            'from_label': 'Subsidiary',
            'to_label': 'Institution',
            'properties': ['customer_id', 'customer_onboarding_date', 'customer_risk_rating']
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
        'INCORPORATED_ON': {
            'from_label': ['Institution', 'Subsidiary'],
            'to_label': 'BusinessDate',
            'properties': []
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
            'from_label': 'Transaction',
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
        'SENT': {
            'from_label': 'Account',
            'to_label': 'Transaction',
            'properties': ['amount', 'currency']
        },
        'RECEIVED': {
            'from_label': 'Transaction',
            'to_label': 'Account',
            'properties': ['amount', 'currency']
        },
        'IS_INSTITUTION': {
            'from_label': 'Entity',
            'to_label': 'Institution',
            'properties': ['created_at', 'updated_at']
        },
        'IS_SUBSIDIARY': {
            'from_label': 'Entity',
            'to_label': 'Subsidiary',
            'properties': ['created_at', 'updated_at']
        }
    }

    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j handler.
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
        """
        super().__init__()
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        self.is_connected = False
    
    async def connect(self) -> None:
        """Connect to Neo4j database."""
        try:
            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
            await self.driver.verify_connectivity()
            self.is_connected = True
            self._log_operation('connect', {'status': 'success'})
        except Exception as e:
            self._log_operation('connect', {'status': 'failed', 'error': str(e)})
            raise ConnectionError(f"Failed to connect to Neo4j: {str(e)}")
    
    async def close(self) -> None:
        """Close database connection."""
        try:
            if self.is_connected and self.driver:
                await self.driver.close()
                self.is_connected = False
                self._log_operation('close', {'status': 'success'})
        except Exception as e:
            self._log_operation('close', {'status': 'failed', 'error': str(e)})
            raise DatabaseError(f"Failed to close connection: {str(e)}")
    
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
    
    def _get_node_type(self, table_name: str) -> str:
        """Convert table name to node type."""
        # Map table names to node types
        table_to_node = {
            'institutions': 'Institution',
            'accounts': 'Account',
            'transactions': 'Transaction',
            'risk_assessments': 'RiskAssessment',
            'beneficial_owners': 'BeneficialOwner',
            'documents': 'Document',
            'jurisdiction_presences': 'JurisdictionPresence',
            'subsidiaries': 'Subsidiary',
            'entities': 'Entity',
            'addresses': 'Address',
            'compliance_events': 'ComplianceEvent',
            'authorized_persons': 'AuthorizedPerson'
        }
        return table_to_node.get(table_name, table_name)

    async def save_batch(self, table_name: str, records: List[Dict[str, Any]]) -> None:
        """Save a batch of records to Neo4j."""
        try:
            # Convert table name to node type
            node_type = self._get_node_type(table_name)
            
            # Validate records
            for record in records:
                self._validate_record(node_type, record)
            
            async with self.driver.session() as session:
                failed_items = []
                for record in records:
                    try:
                        # Convert data types
                        prepared_record = self._prepare_properties(record)
                        
                        # Get primary key field
                        primary_key = self.NODE_SCHEMAS[node_type]['primary_key'][0]
                        
                        # Create node
                        await session.run(f"""
                            MERGE (n:{node_type} {{{primary_key}: $record.{primary_key}}})
                            SET n = $record
                        """, record=prepared_record)
                        
                        # Create relationships based on node type
                        if node_type == 'Transaction':
                            # Create relationships with accounts
                            await session.run("""
                                // Create accounts if they don't exist with required fields
                                MERGE (debit:Account {account_id: $debit_account_id})
                                ON CREATE SET 
                                    debit.entity_id = $debit_account_id,
                                    debit.entity_type = 'Institution',
                                    debit.account_type = 'Unknown',
                                    debit.account_number = $debit_account_id,
                                    debit.currency = $currency,
                                    debit.status = 'Active',
                                    debit.opening_date = $transaction_date,
                                    debit.balance = 0,
                                    debit.risk_rating = 'Medium'

                                WITH debit

                                MERGE (credit:Account {account_id: $credit_account_id})
                                ON CREATE SET 
                                    credit.entity_id = $credit_account_id,
                                    credit.entity_type = 'Institution',
                                    credit.account_type = 'Unknown',
                                    credit.account_number = $credit_account_id,
                                    credit.currency = $currency,
                                    credit.status = 'Active',
                                    credit.opening_date = $transaction_date,
                                    credit.balance = 0,
                                    credit.risk_rating = 'Medium'

                                WITH debit, credit

                                // Match transaction
                                MATCH (t:Transaction {transaction_id: $transaction_id})

                                WITH debit, credit, t

                                // Create SENT and RECEIVED relationships
                                MERGE (debit)-[:SENT {
                                    amount: $amount,
                                    currency: $currency
                                }]->(t)
                                MERGE (t)-[:RECEIVED {
                                    amount: $amount,
                                    currency: $currency
                                }]->(credit)

                                WITH debit, credit, t

                                // Create TRANSACTED relationships
                                MERGE (debit)-[:TRANSACTED {
                                    transaction_date: $transaction_date
                                }]->(t)
                                MERGE (credit)-[:TRANSACTED {
                                    transaction_date: $transaction_date
                                }]->(t)

                                WITH t

                                // Create TRANSACTED_ON relationship with BusinessDate
                                MERGE (d:BusinessDate {date: $transaction_date})
                                MERGE (t)-[:TRANSACTED_ON]->(d)
                            """, {
                                'transaction_id': prepared_record['transaction_id'],
                                'debit_account_id': prepared_record['debit_account_id'],
                                'credit_account_id': prepared_record['credit_account_id'],
                                'amount': prepared_record['amount'],
                                'currency': prepared_record['currency'],
                                'transaction_date': prepared_record['transaction_date']
                            })
                        
                        elif node_type == 'Account':
                            # Create HAS_ACCOUNT relationship with Institution
                            await session.run("""
                                MATCH (i:Institution {institution_id: $entity_id})
                                MATCH (a:Account {account_id: $account_id})
                                MERGE (i)-[:HAS_ACCOUNT]->(a)
                            """, {
                                'entity_id': prepared_record['entity_id'],
                                'account_id': prepared_record['account_id']
                            })
                            # Create OPENED_ON relationship with BusinessDate
                            await session.run("""
                                MATCH (a:Account {account_id: $account_id})
                                MERGE (d:BusinessDate {date: $opening_date})
                                MERGE (a)-[:OPENED_ON]->(d)
                            """, {
                                'account_id': prepared_record['account_id'],
                                'opening_date': prepared_record['opening_date']
                            })
                        
                        elif node_type == 'RiskAssessment':
                            # Create HAS_RISK_ASSESSMENT relationship
                            await session.run("""
                                MATCH (i:Institution {institution_id: $entity_id})
                                MATCH (r:RiskAssessment {assessment_id: $assessment_id})
                                MERGE (i)-[:HAS_RISK_ASSESSMENT]->(r)
                            """, {
                                'entity_id': prepared_record['entity_id'],
                                'assessment_id': prepared_record['assessment_id']
                            })
                            
                        elif node_type == 'Subsidiary':
                            # Add timestamps if not present
                            if 'created_at' not in prepared_record:
                                prepared_record['created_at'] = pd.Timestamp.now().isoformat()
                            if 'updated_at' not in prepared_record:
                                prepared_record['updated_at'] = pd.Timestamp.now().isoformat()

                            try:
                                # Create Entity node and IS_SUBSIDIARY relationship
                                await session.run("""
                                    MERGE (s:Subsidiary {subsidiary_id: $subsidiary_id})
                                    MERGE (e:Entity {entity_id: $subsidiary_id})
                                    ON CREATE SET e.entity_type = 'subsidiary',
                                        e.created_at = $created_at,
                                        e.updated_at = $updated_at,
                                        e.parent_entity_id = $parent_institution_id
                                    ON MATCH SET e.updated_at = $updated_at,
                                        e.parent_entity_id = $parent_institution_id
                                    MERGE (e)-[:IS_SUBSIDIARY {
                                        created_at: $created_at,
                                        updated_at: $updated_at
                                    }]->(s)
                                """, {
                                    'subsidiary_id': prepared_record['subsidiary_id'],
                                    'parent_institution_id': prepared_record['parent_institution_id'],
                                    'created_at': prepared_record['created_at'],
                                    'updated_at': prepared_record['updated_at']
                                })
                            except Exception as e:
                                print(f"Failed to create Subsidiary Entity relationship: {str(e)}")
                                print(f"Prepared record: {prepared_record}")
                                raise e

                            # Create OWNS_SUBSIDIARY relationship with Institution
                            await session.run("""
                                MATCH (i:Institution {institution_id: $parent_institution_id})
                                MATCH (s:Subsidiary {subsidiary_id: $subsidiary_id})
                                MERGE (i)-[:OWNS_SUBSIDIARY {
                                    ownership_percentage: $ownership_percentage,
                                    acquisition_date: $acquisition_date
                                }]->(s)
                            """, {
                                'parent_institution_id': prepared_record['parent_institution_id'],
                                'subsidiary_id': prepared_record['subsidiary_id'],
                                'ownership_percentage': prepared_record['parent_ownership_percentage'],
                                'acquisition_date': prepared_record['acquisition_date']
                            })

                            # Create INCORPORATED_IN relationship with Country
                            await session.run("""
                                MATCH (s:Subsidiary {subsidiary_id: $subsidiary_id})
                                MERGE (c:Country {code: $country_code})
                                MERGE (s)-[:INCORPORATED_IN {
                                    incorporation_date: $incorporation_date
                                }]->(c)
                            """, {
                                'subsidiary_id': prepared_record['subsidiary_id'],
                                'country_code': prepared_record['incorporation_country'],
                                'incorporation_date': prepared_record['incorporation_date']
                            })

                            # Create INCORPORATED_ON relationship with BusinessDate
                            await session.run("""
                                MATCH (s:Subsidiary {subsidiary_id: $subsidiary_id})
                                MERGE (d:BusinessDate {date: $incorporation_date})
                                MERGE (s)-[:INCORPORATED_ON]->(d)
                            """, {
                                'subsidiary_id': prepared_record['subsidiary_id'],
                                'incorporation_date': prepared_record['incorporation_date']
                            })

                            # If subsidiary is also a customer, create IS_CUSTOMER relationship
                            if prepared_record.get('is_customer', False):
                                await session.run("""
                                    MATCH (s:Subsidiary {subsidiary_id: $subsidiary_id})
                                    MATCH (i:Institution {institution_id: $parent_institution_id})
                                    MERGE (s)-[:IS_CUSTOMER {
                                        customer_id: $customer_id,
                                        customer_onboarding_date: $customer_onboarding_date,
                                        customer_risk_rating: $customer_risk_rating
                                    }]->(i)
                                """, {
                                    'subsidiary_id': prepared_record['subsidiary_id'],
                                    'parent_institution_id': prepared_record['parent_institution_id'],
                                    'customer_id': prepared_record.get('customer_id'),
                                    'customer_onboarding_date': prepared_record.get('customer_onboarding_date'),
                                    'customer_risk_rating': prepared_record.get('customer_risk_rating')
                                })

                        elif node_type == 'Institution':
                            # Add timestamps if not present
                            if 'created_at' not in prepared_record:
                                prepared_record['created_at'] = pd.Timestamp.now().isoformat()
                            if 'updated_at' not in prepared_record:
                                prepared_record['updated_at'] = pd.Timestamp.now().isoformat()

                            try:
                                # Create Entity node and IS_INSTITUTION relationship
                                await session.run("""
                                    MERGE (i:Institution {institution_id: $institution_id})
                                    MERGE (e:Entity {entity_id: $institution_id})
                                    ON CREATE SET e.entity_type = 'institution',
                                        e.created_at = $created_at,
                                        e.updated_at = $updated_at
                                    ON MATCH SET e.updated_at = $updated_at
                                    MERGE (e)-[:IS_INSTITUTION {
                                        created_at: $created_at,
                                        updated_at: $updated_at
                                    }]->(i)
                                """, {
                                    'institution_id': prepared_record['institution_id'],
                                    'created_at': prepared_record['created_at'],
                                    'updated_at': prepared_record['updated_at']
                                })
                            except Exception as e:
                                print(f"Failed to create Institution Entity relationship: {str(e)}")
                                print(f"Prepared record: {prepared_record}")
                                raise e

                            # Create INCORPORATED_IN relationship with Country
                            await session.run("""
                                MATCH (i:Institution {institution_id: $institution_id})
                                MERGE (c:Country {code: $country_code})
                                MERGE (i)-[:INCORPORATED_IN {
                                    incorporation_date: $incorporation_date
                                }]->(c)
                            """, {
                                'institution_id': prepared_record['institution_id'],
                                'country_code': prepared_record['incorporation_country'],
                                'incorporation_date': prepared_record['incorporation_date']
                            })

                            # Create INCORPORATED_ON relationship with BusinessDate
                            await session.run("""
                                MATCH (i:Institution {institution_id: $institution_id})
                                MERGE (d:BusinessDate {date: $incorporation_date})
                                MERGE (i)-[:INCORPORATED_ON]->(d)
                            """, {
                                'institution_id': prepared_record['institution_id'],
                                'incorporation_date': prepared_record['incorporation_date']
                            })

                        elif node_type == 'Document':
                            # Create HAS_DOCUMENT relationship
                            await session.run("""
                                MATCH (d:Document {document_id: $document_id})
                                
                                // Try to match Institution or Subsidiary based on entity_type
                                OPTIONAL MATCH (i:Institution {institution_id: $entity_id})
                                OPTIONAL MATCH (s:Subsidiary {subsidiary_id: $entity_id})
                                
                                WITH d, 
                                     CASE WHEN i IS NOT NULL THEN i 
                                          WHEN s IS NOT NULL THEN s 
                                          ELSE null END as entity
                                
                                // Create relationship only if entity exists
                                FOREACH (e IN CASE WHEN entity IS NOT NULL THEN [entity] ELSE [] END |
                                    MERGE (e)-[:HAS_DOCUMENT {
                                        document_type: $document_type
                                    }]->(d)
                                )
                                
                                WITH d
                                
                                // Create ISSUED_ON relationship with BusinessDate
                                MERGE (bd:BusinessDate {date: $issue_date})
                                MERGE (d)-[:ISSUED_ON]->(bd)
                            """, {
                                'document_id': prepared_record['document_id'],
                                'entity_id': prepared_record['entity_id'],
                                'entity_type': prepared_record['entity_type'].lower(),
                                'document_type': prepared_record['document_type'],
                                'issue_date': prepared_record['issue_date']
                            })

                        elif node_type == 'BeneficialOwner':
                            # Create OWNED_BY and CITIZEN_OF relationships
                            await session.run("""
                                MATCH (bo:BeneficialOwner {owner_id: $owner_id})
                                
                                // Try to match Institution or Subsidiary based on entity_type
                                OPTIONAL MATCH (i:Institution {institution_id: $entity_id})
                                OPTIONAL MATCH (s:Subsidiary {subsidiary_id: $entity_id})
                                
                                WITH bo, 
                                     CASE WHEN i IS NOT NULL THEN i 
                                          WHEN s IS NOT NULL THEN s 
                                          ELSE null END as entity
                                
                                // Create OWNED_BY relationship if entity exists
                                FOREACH (e IN CASE WHEN entity IS NOT NULL THEN [entity] ELSE [] END |
                                    MERGE (e)-[:OWNED_BY {
                                        ownership_percentage: $ownership_percentage,
                                        verification_date: $verification_date
                                    }]->(bo)
                                )
                                
                                WITH bo
                                
                                // Create CITIZEN_OF relationship
                                MERGE (c:Country {code: $nationality})
                                MERGE (bo)-[:CITIZEN_OF]->(c)
                            """, {
                                'owner_id': prepared_record['owner_id'],
                                'entity_id': prepared_record['entity_id'],
                                'entity_type': prepared_record['entity_type'].lower(),
                                'ownership_percentage': prepared_record['ownership_percentage'],
                                'verification_date': prepared_record['verification_date'],
                                'nationality': prepared_record['nationality']
                            })

                        elif node_type == 'AuthorizedPerson':
                            # Create HAS_AUTHORIZED_PERSON and CITIZEN_OF relationships
                            await session.run("""
                                MATCH (ap:AuthorizedPerson {person_id: $person_id})
                                
                                // Try to match Institution or Subsidiary based on entity_type
                                OPTIONAL MATCH (i:Institution {institution_id: $entity_id})
                                OPTIONAL MATCH (s:Subsidiary {subsidiary_id: $entity_id})
                                
                                WITH ap, 
                                     CASE WHEN i IS NOT NULL THEN i 
                                          WHEN s IS NOT NULL THEN s 
                                          ELSE null END as entity
                                
                                // Create HAS_AUTHORIZED_PERSON relationship if entity exists
                                FOREACH (e IN CASE WHEN entity IS NOT NULL THEN [entity] ELSE [] END |
                                    MERGE (e)-[:HAS_AUTHORIZED_PERSON {
                                        title: $title,
                                        authorization_date: $authorization_start
                                    }]->(ap)
                                )
                                
                                WITH ap
                                
                                // Create CITIZEN_OF relationship if nationality exists
                                FOREACH (nat IN CASE WHEN $nationality IS NOT NULL THEN [$nationality] ELSE [] END |
                                    MERGE (c:Country {code: nat})
                                    MERGE (ap)-[:CITIZEN_OF]->(c)
                                )
                            """, {
                                'person_id': prepared_record['person_id'],
                                'entity_id': prepared_record['entity_id'],
                                'entity_type': prepared_record['entity_type'].lower(),
                                'title': prepared_record['title'],
                                'authorization_start': prepared_record['authorization_start'],
                                'nationality': prepared_record.get('nationality')
                            })

                        elif node_type == 'ComplianceEvent':
                            # Create HAS_COMPLIANCE_EVENT relationship
                            await session.run("""
                                MATCH (ce:ComplianceEvent {event_id: $event_id})
                                
                                // Try to match Institution or Subsidiary based on entity_type
                                OPTIONAL MATCH (i:Institution {institution_id: $entity_id})
                                OPTIONAL MATCH (s:Subsidiary {subsidiary_id: $entity_id})
                                
                                WITH ce, 
                                     CASE WHEN i IS NOT NULL THEN i 
                                          WHEN s IS NOT NULL THEN s 
                                          ELSE null END as entity
                                
                                // Create HAS_COMPLIANCE_EVENT relationship if entity exists
                                FOREACH (e IN CASE WHEN entity IS NOT NULL THEN [entity] ELSE [] END |
                                    MERGE (e)-[:HAS_COMPLIANCE_EVENT]->(ce)
                                )
                            """, {
                                'event_id': prepared_record['event_id'],
                                'entity_id': prepared_record['entity_id'],
                                'entity_type': prepared_record['entity_type'].lower()
                            })

                        elif node_type == 'RiskAssessment':
                            # Create HAS_RISK_ASSESSMENT relationship
                            await session.run("""
                                MATCH (ra:RiskAssessment {assessment_id: $assessment_id})
                                
                                // Try to match Institution or Subsidiary based on entity_type
                                OPTIONAL MATCH (i:Institution {institution_id: $entity_id})
                                OPTIONAL MATCH (s:Subsidiary {subsidiary_id: $entity_id})
                                
                                WITH ra, 
                                     CASE WHEN i IS NOT NULL THEN i 
                                          WHEN s IS NOT NULL THEN s 
                                          ELSE null END as entity
                                
                                // Create HAS_RISK_ASSESSMENT relationship if entity exists
                                FOREACH (e IN CASE WHEN entity IS NOT NULL THEN [entity] ELSE [] END |
                                    MERGE (e)-[:HAS_RISK_ASSESSMENT]->(ra)
                                )
                            """, {
                                'assessment_id': prepared_record['assessment_id'],
                                'entity_id': prepared_record['entity_id'],
                                'entity_type': prepared_record['entity_type'].lower()
                            })

                    except Exception as e:
                        failed_items.append({
                            'record': record,
                            'error': str(e),
                            'node_type': node_type,
                            'prepared_record': prepared_record
                        })
                        print(f"Failed to save {node_type} record: {str(e)}")
                        print(f"Record: {record}")
                        print(f"Prepared record: {prepared_record}")

                if failed_items:
                    raise BatchError(f"Failed to save {len(failed_items)} records", failed_items=failed_items)
            
            self._log_operation('save_batch', {
                'status': 'success',
                'node_type': node_type,
                'record_count': len(records)
            })
            
        except BatchError:
            raise
        except Exception as e:
            failed_items = [{
                'record': record,
                'error': str(e)
            } for record in records]
            self._log_operation('save_batch', {
                'status': 'failed',
                'node_type': node_type,
                'error': str(e)
            })
            if isinstance(e, (ValidationError, SchemaError)):
                raise
            raise BatchError(f"Failed to save batch: {str(e)}", failed_items=failed_items)
    
    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]) -> None:
        """Save data to Neo4j database."""
        await self.save_batch(data)
    
    async def wipe_clean(self) -> None:
        """Wipe all data from the database while preserving indexes and constraints."""
        try:
            async with self.driver.session() as session:
                # Delete all relationships first
                await session.run("MATCH ()-[r]-() DELETE r")
                # Then delete all nodes
                await session.run("MATCH (n) DELETE n")
                
            self._log_operation('wipe_clean', {'status': 'success'})
        except Exception as e:
            self._log_operation('wipe_clean', {'status': 'failed', 'error': str(e)})
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
                'amount', 'currency', 'transaction_status', 'is_debit', 'debit_account_id', 'credit_account_id'
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
            },
            'entities': {
                'entity_id', 'entity_type', 'created_at', 'updated_at'
            }
        }
        return required_fields.get(table_name, set())

    def _validate_record(self, table_name: str, record: Dict[str, Any]) -> None:
        """Validate a record against the schema."""
        node_type = self._get_node_type(table_name)
        if node_type not in self.NODE_SCHEMAS:
            raise ValidationError(f"Invalid node label: {node_type}")
            
        schema = self.NODE_SCHEMAS[node_type]
        missing_fields = set(schema['required']) - set(record.keys())
        if missing_fields:
            raise ValidationError(f"Missing required fields for {node_type}: {missing_fields}")

    def _prepare_properties(self, record: dict) -> dict:
        """Prepare properties for Neo4j by converting data types."""
        prepared = {}
        
        # Convert all values to Neo4j compatible types
        for key, value in record.items():
            if value is None:
                # Handle null values based on field type
                if key in ['risk_score', 'amount', 'total_amount', 'avg_risk_score', 'processing_fee', 'exchange_rate']:
                    value = 0.0
                elif key in ['transaction_count', 'alert_count']:
                    value = 0
                elif key in ['screening_alert', 'material_subsidiary']:
                    value = False
                else:
                    continue  # Skip null values for other fields
                
            try:
                if key == 'incorporation_date' or key == 'opening_date':
                    if isinstance(value, (int, float)):
                        raise ValidationError(f"Field {key} must be a date string, got {type(value)}")
                    if isinstance(value, str):
                        # Validate date format (YYYY-MM-DD)
                        datetime.strptime(value, '%Y-%m-%d')
                    prepared[key] = value
                elif key == 'transaction_date' or key == 'assessment_date' or key == 'created_at' or key == 'updated_at':
                    if isinstance(value, (int, float)):
                        raise ValidationError(f"Field {key} must be a datetime string, got {type(value)}")
                    if isinstance(value, str):
                        # Try parsing as ISO format first
                        try:
                            datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except ValueError:
                            # Fall back to date-only format
                            datetime.strptime(value, '%Y-%m-%d')
                    prepared[key] = value
                elif isinstance(value, (datetime, date)):
                    prepared[key] = value.isoformat()
                elif isinstance(value, UUID):
                    prepared[key] = str(value)
                elif isinstance(value, Enum):
                    prepared[key] = value.value
                elif isinstance(value, bool):
                    prepared[key] = bool(value)  # Ensure it's a proper boolean
                elif isinstance(value, (int, float)):
                    if key == 'material_subsidiary':  # Special handling for material_subsidiary
                        prepared[key] = bool(value)
                    else:
                        prepared[key] = float(value) if isinstance(value, float) or '.' in str(value) else int(value)
                elif isinstance(value, (dict, list)):
                    prepared[key] = json.dumps(value)
                else:
                    prepared[key] = str(value)
            except (ValueError, TypeError) as e:
                raise ValidationError(f"Invalid value for field {key}: {str(e)}")
                
        # Map ID fields based on node type and ensure they are strings
        if 'entity_id' in record:
            prepared['id'] = str(record['entity_id'])
        elif 'institution_id' in record:
            prepared['id'] = str(record['institution_id'])
        elif 'subsidiary_id' in record:
            prepared['id'] = str(record['subsidiary_id'])
        elif 'assessment_id' in record:
            prepared['id'] = str(record['assessment_id'])
        elif 'person_id' in record:
            prepared['id'] = str(record['person_id'])
        elif 'event_id' in record:
            prepared['id'] = str(record['event_id'])
        elif 'transaction_id' in record:
            prepared['id'] = str(record['transaction_id'])
            prepared['transaction_id'] = str(record['transaction_id'])
        elif 'account_id' in record:
            prepared['id'] = str(record['account_id'])
            prepared['account_id'] = str(record['account_id'])
            
        # Ensure specific fields are strings
        string_fields = ['account_id', 'entity_id', 'currency']
        for field in string_fields:
            if field in prepared:
                prepared[field] = str(prepared[field])
                
        return prepared

    async def insert_data(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Insert data into Neo4j database.
        
        Args:
            table_name (str): Name of the table/node type to insert data into
            data (List[Dict[str, Any]]): List of dictionaries containing the data to insert
            batch_size (int, optional): Number of records to insert at once. Defaults to 1000.
        """
        if not data:
            return
            
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(data)
        
        # Save the data using save_batch
        await self.save_batch(table_name, data)

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

    async def initialize(self) -> None:
        """Initialize database connection and create constraints if they don't exist."""
        try:
            await self.connect()
            await self.create_schema()
            self._log_operation('initialize', {'status': 'success'})
        except Exception as e:
            self._log_operation('initialize', {'status': 'failed', 'error': str(e)})
            raise DatabaseInitializationError(f"Failed to initialize Neo4j database: {str(e)}")
