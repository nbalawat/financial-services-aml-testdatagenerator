"""
Data Generator Module for AML Transaction Monitoring System.

This module provides a structured approach to generate test data for the AML monitoring system.
It uses generators for efficient memory usage and provides progress tracking.
"""

import json
import logging
import random
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import UUID
import pandas as pd

from .models import (
    Institution, Subsidiary, Address, BeneficialOwner,
    Account, Transaction, Entity, RiskAssessment, ComplianceEvent,
    AuthorizedPerson, Document, JurisdictionPresence
)
from .generators import (
    InstitutionGenerator, SubsidiaryGenerator, AddressGenerator,
    BeneficialOwnerGenerator, AccountGenerator, TransactionGenerator,
    RiskAssessmentGenerator, ComplianceEventGenerator, AuthorizedPersonGenerator,
    DocumentGenerator, JurisdictionPresenceGenerator
)
from .database import PostgresHandler, Neo4jHandler
from .database.exceptions import DatabaseError, BatchError

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Tracks progress of data generation."""
    
    def __init__(self, total: int, description: str = None):
        self.total = total
        self.description = description
        self.current = 0
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
        
    def update(self, n: int = 1):
        """Update progress."""
        self.current += n
        if self.current % 100 == 0:  # Only log every 100 items
            logger.warning(f"{self.description}: {self.current}/{self.total}")

class DataGenerator:
    """Main class for orchestrating data generation."""
    
    def __init__(self, config: Dict[str, Any], postgres_handler: PostgresHandler, neo4j_handler: Neo4jHandler):
        """Initialize the data generator."""
        self.config = config
        self.postgres_handler = postgres_handler
        self.neo4j_handler = neo4j_handler

        # Initialize generators
        self.institution_gen = InstitutionGenerator(config)
        self.subsidiary_gen = SubsidiaryGenerator(config)
        self.address_gen = AddressGenerator(config)
        self.beneficial_owner_gen = BeneficialOwnerGenerator(config)
        self.account_gen = AccountGenerator(config)
        self.transaction_gen = TransactionGenerator(config)
        self.risk_assessment_gen = RiskAssessmentGenerator(config)
        self.compliance_event_gen = ComplianceEventGenerator(config)
        self.authorized_person_gen = AuthorizedPersonGenerator(config)
        self.document_gen = DocumentGenerator(config)
        self.jurisdiction_presence_gen = JurisdictionPresenceGenerator(config)
        
        # Initialize database handlers
        self.postgres_handler = postgres_handler
        self.neo4j_handler = neo4j_handler

    async def initialize_db(self):
        """Initialize database connections."""
        await self.postgres_handler.initialize()
        await self.neo4j_handler.initialize()

    async def close_db(self):
        """Close database connections."""
        await self.postgres_handler.close()
        await self.neo4j_handler.close()

    def _convert_to_dataframe(self, data: List[Any]) -> 'pd.DataFrame':
        """Convert list of Pydantic models to DataFrame."""
        if not data:
            return pd.DataFrame()
        return pd.DataFrame([item.model_dump() for item in data])

    async def persist_batch(self, batch_data: Dict[str, List[Any]], batch_size: Optional[int] = None):
        """Persist a batch of data to both databases."""
        try:
            # Convert batch data to DataFrames
            df_data = {}
            for data_type, data_list in batch_data.items():
                if data_list:  # Only process non-empty lists
                    df_data[data_type] = self._convert_to_dataframe(data_list)
            
            # Save to PostgreSQL and Neo4j
            if df_data:
                await self.postgres_handler.save_batch(df_data)
                
                # Save to Neo4j - convert to dicts first
                for table_name, items in batch_data.items():
                    if items:
                        records = [item.model_dump() for item in items]
                        await self.neo4j_handler.save_batch(table_name, records)
                
                # Log a simple summary
                logger.warning(f"Saved: {', '.join(f'{k}={len(v)}' for k, v in batch_data.items())}")
        except (DatabaseError, BatchError) as e:
            raise DatabaseError(f"Failed to save batch: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error saving batch: {str(e)}")

    async def generate_all(self):
        """Generate all data types and persist them."""
        num_institutions = self.config['num_institutions']
        institutions_batch_size = self.config.get('batch_size', {}).get('institutions', 100)
        
        # Initialize batch data
        institution_subsidiary_batch = defaultdict(list)
        current_batch_size = 0
        
        # Progress tracking
        total_entities = 0
        total_subsidiaries = 0
        
        logger.warning(f"Starting data generation for {num_institutions} institutions")
        
        # Generate institutions and subsidiaries
        institution_count = 0
        async for institution in self.institution_gen.generate():
            institution_count += 1
            # Create entity record for institution
            institution_entity = Entity(
                entity_id=institution.institution_id,
                entity_type='institution',
                parent_entity_id=None,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deleted_at=None
            )
            institution_subsidiary_batch['entities'].append(institution_entity)
            institution_subsidiary_batch['institutions'].append(institution)
            current_batch_size += 1
            total_entities += 1
            
            # Generate subsidiaries for this institution
            num_subsidiaries = random.randint(
                self.config.get('min_subsidiaries_per_institution', 1),
                self.config.get('max_subsidiaries_per_institution', 5)
            )
            
            subsidiary_count = 0
            async for subsidiary in self.subsidiary_gen.generate(institution.institution_id):
                if len(institution_subsidiary_batch['subsidiaries']) < num_subsidiaries:
                    subsidiary_count += 1
                    # Create entity record for subsidiary
                    subsidiary_entity = Entity(
                        entity_id=subsidiary.subsidiary_id,
                        entity_type='subsidiary',
                        parent_entity_id=institution.institution_id,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                        deleted_at=None
                    )
                    institution_subsidiary_batch['entities'].append(subsidiary_entity)
                    institution_subsidiary_batch['subsidiaries'].append(subsidiary)
                    current_batch_size += 1
                    total_entities += 1
                    total_subsidiaries += 1
                else:
                    break
            
            # Show progress for this institution
            logger.warning(f"[{institution_count}/{num_institutions}] Generated institution with {subsidiary_count} subsidiaries")
            
            # Process batch if size threshold reached
            if current_batch_size >= institutions_batch_size:
                # First persist the entities
                entities_batch = {'entities': institution_subsidiary_batch['entities']}
                await self.persist_batch(entities_batch)
                logger.warning(f"Saved batch of {len(entities_batch['entities'])} entities")
                
                # Then persist institutions and subsidiaries
                institutions_subsidiaries_batch = {
                    'institutions': institution_subsidiary_batch['institutions'],
                    'subsidiaries': institution_subsidiary_batch['subsidiaries']
                }
                await self.persist_batch(institutions_subsidiaries_batch)
                logger.warning(f"Saved batch of {len(institutions_subsidiaries_batch['institutions'])} institutions and {len(institutions_subsidiaries_batch['subsidiaries'])} subsidiaries")
                
                # Finally generate and persist related data
                logger.warning(f"Generating related data for batch of {len(institution_subsidiary_batch['institutions'])} institutions...")
                await self.generate_all_related_data(institution_subsidiary_batch)
                institution_subsidiary_batch = defaultdict(list)
                current_batch_size = 0
        
        # Process remaining batch
        if current_batch_size > 0:
            logger.warning(f"Processing final batch...")
            # First persist the entities
            entities_batch = {'entities': institution_subsidiary_batch['entities']}
            await self.persist_batch(entities_batch)
            logger.warning(f"Saved final batch of {len(entities_batch['entities'])} entities")
            
            # Then persist institutions and subsidiaries
            institutions_subsidiaries_batch = {
                'institutions': institution_subsidiary_batch['institutions'],
                'subsidiaries': institution_subsidiary_batch['subsidiaries']
            }
            await self.persist_batch(institutions_subsidiaries_batch)
            logger.warning(f"Saved final batch of {len(institutions_subsidiaries_batch['institutions'])} institutions and {len(institutions_subsidiaries_batch['subsidiaries'])} subsidiaries")
            
            # Finally generate and persist related data
            logger.warning(f"Generating related data for final batch...")
            await self.generate_all_related_data(institution_subsidiary_batch)
            
        logger.warning(f"Completed: Generated {total_entities} total entities ({num_institutions} institutions, {total_subsidiaries} subsidiaries)")

    async def generate_all_related_data(self, institution_subsidiary_batch):
        """Generate and persist all entity-related data."""
        logger.warning("Generating related data...")

        # Process each institution
        for institution in institution_subsidiary_batch['institutions']:
            # Generate address for institution
            logger.warning(f"Generating address for institution {institution.institution_id}")
            address = await self.address_gen.generate(institution.institution_id, 'institution').__anext__()
            await self.persist_batch({'addresses': [address]})
            logger.warning("Saved institution address")

            # Generate beneficial owners for institution
            num_owners = random.randint(1, self.config.get('max_beneficial_owners_per_institution', 3))
            logger.warning(f"Generating {num_owners} beneficial owners for institution {institution.institution_id}")
            beneficial_owners = []
            for _ in range(num_owners):
                owner = await self.beneficial_owner_gen.generate(institution.institution_id, 'institution').__anext__()
                beneficial_owners.append(owner)
            
            # Save beneficial owners
            if beneficial_owners:
                await self.persist_batch({'beneficial_owners': beneficial_owners})
                logger.warning(f"Saved {len(beneficial_owners)} beneficial owners")
            
            # Generate accounts for institution
            num_accounts = random.randint(1, self.config.get('max_accounts_per_institution', 3))
            logger.warning(f"Generating {num_accounts} accounts for institution {institution.institution_id}")
            accounts = []
            for _ in range(num_accounts):
                account = await self.account_gen.generate(institution.institution_id, 'institution').__anext__()
                accounts.append(account)
            
            # Save accounts first
            if accounts:
                await self.persist_batch({'accounts': accounts})
                logger.warning(f"Saved {len(accounts)} accounts")
                
            # Now generate and save transactions for each account
            for account in accounts:
                # Generate transactions for this account
                num_transactions = random.randint(
                    self.config.get('min_transactions_per_account', 5),
                    self.config.get('max_transactions_per_account', 10)
                )
                
                logger.warning(f"Generating {num_transactions} transactions for account {account.account_id}")
                transactions = []
                for _ in range(num_transactions):
                    transaction = await self.transaction_gen.generate(account).__anext__()
                    transactions.append(transaction)
                
                # Save transactions for this account
                if transactions:
                    await self.persist_batch({'transactions': transactions})
                    logger.warning(f"Saved {len(transactions)} transactions")
            
            # Generate risk assessments for institution
            num_risk_assessments = random.randint(1, self.config.get('max_risk_assessments_per_institution', 2))
            logger.warning(f"Generating {num_risk_assessments} risk assessments for institution {institution.institution_id}")
            risk_assessments = []
            for _ in range(num_risk_assessments):
                assessment = await self.risk_assessment_gen.generate(institution.institution_id, 'institution').__anext__()
                risk_assessments.append(assessment)
            
            # Save risk assessments
            if risk_assessments:
                await self.persist_batch({'risk_assessments': risk_assessments})
                logger.warning(f"Saved {len(risk_assessments)} risk assessments")

            # Generate authorized persons
            num_auth_persons = random.randint(1, self.config.get('max_authorized_persons_per_institution', 3))
            logger.warning(f"Generating {num_auth_persons} authorized persons for institution {institution.institution_id}")
            auth_persons = []
            for _ in range(num_auth_persons):
                auth_person = await self.authorized_person_gen.generate(institution.institution_id, 'institution').__anext__()
                auth_persons.append(auth_person)
            
            if auth_persons:
                await self.persist_batch({'authorized_persons': auth_persons})
                logger.warning(f"Saved {len(auth_persons)} authorized persons")

            # Generate compliance events
            num_events = random.randint(1, self.config.get('max_compliance_events_per_institution', 3))
            logger.warning(f"Generating {num_events} compliance events for institution {institution.institution_id}")
            events = []
            for _ in range(num_events):
                event = await self.compliance_event_gen.generate(institution.institution_id, 'institution').__anext__()
                events.append(event)
            
            if events:
                await self.persist_batch({'compliance_events': events})
                logger.warning(f"Saved {len(events)} compliance events")

            # Generate documents
            num_documents = random.randint(1, self.config.get('max_documents_per_institution', 5))
            logger.warning(f"Generating {num_documents} documents for institution {institution.institution_id}")
            documents = []
            for _ in range(num_documents):
                document = await self.document_gen.generate(institution.institution_id, 'institution').__anext__()
                documents.append(document)
            
            if documents:
                await self.persist_batch({'documents': documents})
                logger.warning(f"Saved {len(documents)} documents")

            # Generate jurisdiction presences
            num_jurisdictions = random.randint(1, self.config.get('max_jurisdictions_per_institution', 3))
            logger.warning(f"Generating {num_jurisdictions} jurisdiction presences for institution {institution.institution_id}")
            jurisdictions = []
            for _ in range(num_jurisdictions):
                jurisdiction = await self.jurisdiction_presence_gen.generate(institution.institution_id, 'institution').__anext__()
                jurisdictions.append(jurisdiction)
            
            if jurisdictions:
                await self.persist_batch({'jurisdiction_presences': jurisdictions})
                logger.warning(f"Saved {len(jurisdictions)} jurisdiction presences")

        # Process each subsidiary
        for subsidiary in institution_subsidiary_batch['subsidiaries']:
            # Generate address for subsidiary
            logger.warning(f"Generating address for subsidiary {subsidiary.subsidiary_id}")
            address = await self.address_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
            await self.persist_batch({'addresses': [address]})
            logger.warning("Saved subsidiary address")

            # Generate authorized persons for subsidiary
            num_auth_persons = random.randint(1, self.config.get('max_authorized_persons_per_subsidiary', 2))
            logger.warning(f"Generating {num_auth_persons} authorized persons for subsidiary {subsidiary.subsidiary_id}")
            auth_persons = []
            for _ in range(num_auth_persons):
                auth_person = await self.authorized_person_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                auth_persons.append(auth_person)
            
            if auth_persons:
                await self.persist_batch({'authorized_persons': auth_persons})
                logger.warning(f"Saved {len(auth_persons)} authorized persons")

            # Generate compliance events for subsidiary
            num_events = random.randint(1, self.config.get('max_compliance_events_per_subsidiary', 2))
            logger.warning(f"Generating {num_events} compliance events for subsidiary {subsidiary.subsidiary_id}")
            events = []
            for _ in range(num_events):
                event = await self.compliance_event_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                events.append(event)
            
            if events:
                await self.persist_batch({'compliance_events': events})
                logger.warning(f"Saved {len(events)} compliance events")

            # Generate documents for subsidiary
            num_documents = random.randint(1, self.config.get('max_documents_per_subsidiary', 3))
            logger.warning(f"Generating {num_documents} documents for subsidiary {subsidiary.subsidiary_id}")
            documents = []
            for _ in range(num_documents):
                document = await self.document_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                documents.append(document)
            
            if documents:
                await self.persist_batch({'documents': documents})
                logger.warning(f"Saved {len(documents)} documents")

            # Generate jurisdiction presences for subsidiary
            num_jurisdictions = random.randint(1, self.config.get('max_jurisdictions_per_subsidiary', 2))
            logger.warning(f"Generating {num_jurisdictions} jurisdiction presences for subsidiary {subsidiary.subsidiary_id}")
            jurisdictions = []
            for _ in range(num_jurisdictions):
                jurisdiction = await self.jurisdiction_presence_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                jurisdictions.append(jurisdiction)
            
            if jurisdictions:
                await self.persist_batch({'jurisdiction_presences': jurisdictions})
                logger.warning(f"Saved {len(jurisdictions)} jurisdiction presences")
            
        logger.warning("Completed generating related data")

async def generate_test_data(config: Dict[str, Any], postgres_handler: PostgresHandler, neo4j_handler: Neo4jHandler):
    """
    Generate and persist test data based on configuration.

    Args:
        config: Configuration dictionary containing generation parameters
               Required keys:
               - num_institutions: Number of institutions to generate
               - min_transactions_per_account: Minimum transactions per account
               - max_transactions_per_account: Maximum transactions per account
               - batch_size: Number of records to persist at once
    """
    generator = DataGenerator(config, postgres_handler, neo4j_handler)
    await generator.initialize_db()
    try:
        await generator.generate_all()
    finally:
        await generator.close_db()
