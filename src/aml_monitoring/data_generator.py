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
from tqdm import tqdm

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Tracks progress of data generation with tqdm."""
    
    def __init__(self, total: int, description: str = None):
        self.total = total
        self.description = description
        self.pbar = None
        
    async def __aenter__(self):
        self.pbar = tqdm(range(self.total), desc=self.description)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pbar:
            self.pbar.close()
            
    async def update(self, n: int = 1):
        if self.pbar:
            self.pbar.update(n)

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
        """
        Persist a batch of data to both databases.

        Args:
            batch_data (Dict[str, List[Any]]): Dictionary mapping data types to lists of Pydantic models
            batch_size (Optional[int], optional): Batch size for database operations. If not provided,
                will use the batch size from config. Defaults to None.
        """
        batch_size = batch_size or self.config.get('batch_size', 1000)

        # Convert to DataFrames for Postgres
        df_data = {
            data_type: self._convert_to_dataframe(items)
            for data_type, items in batch_data.items()
        }

        # Save to PostgreSQL
        await self.postgres_handler.save_batch(df_data)

        # Save to Neo4j - handle each table separately
        for table_name, items in batch_data.items():
            # Convert Pydantic models to dicts
            records = [item.model_dump() for item in items]
            await self.neo4j_handler.save_batch(table_name, records)

    async def generate_all(self):
        """Generate all data types and persist them."""
        num_institutions = self.config['num_institutions']
        institutions_batch_size = self.config.get('batch_size', {}).get('institutions', 100)
        
        # Initialize batch data
        institution_subsidiary_batch = defaultdict(list)
        current_batch_size = 0
        
        # Generate institutions and subsidiaries
        async with ProgressTracker(num_institutions, "Generating Institutions") as inst_progress:
            async for institution in self.institution_gen.generate():
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
                
                # Generate subsidiaries for this institution
                num_subsidiaries = random.randint(
                    self.config.get('min_subsidiaries_per_institution', 1),
                    self.config.get('max_subsidiaries_per_institution', 5)
                )
                
                async for subsidiary in self.subsidiary_gen.generate(institution.institution_id):
                    if len(institution_subsidiary_batch['subsidiaries']) < num_subsidiaries:
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
                    else:
                        break
                
                await inst_progress.update(1)
                
                # Process batch if size threshold reached
                if current_batch_size >= institutions_batch_size:
                    # First persist the entities
                    entities_batch = {'entities': institution_subsidiary_batch['entities']}
                    await self.persist_batch(entities_batch)
                    
                    # Then persist institutions and subsidiaries
                    institutions_subsidiaries_batch = {
                        'institutions': institution_subsidiary_batch['institutions'],
                        'subsidiaries': institution_subsidiary_batch['subsidiaries']
                    }
                    await self.persist_batch(institutions_subsidiaries_batch)
                    
                    # Finally generate and persist related data
                    await self.generate_all_related_data(institution_subsidiary_batch)
                    institution_subsidiary_batch = defaultdict(list)
                    current_batch_size = 0
        
        # Process remaining batch
        if current_batch_size > 0:
            # First persist the entities
            entities_batch = {'entities': institution_subsidiary_batch['entities']}
            await self.persist_batch(entities_batch)
            
            # Then persist institutions and subsidiaries
            institutions_subsidiaries_batch = {
                'institutions': institution_subsidiary_batch['institutions'],
                'subsidiaries': institution_subsidiary_batch['subsidiaries']
            }
            await self.persist_batch(institutions_subsidiaries_batch)
            
            # Finally generate and persist related data
            await self.generate_all_related_data(institution_subsidiary_batch)

    async def generate_all_related_data(self, institution_subsidiary_batch):
        """Generate and persist all entity-related data."""
        entity_related_data = defaultdict(list)

        # Process each institution
        for institution in institution_subsidiary_batch['institutions']:
            # Generate addresses for institution
            address = await self.address_gen.generate(institution.institution_id, 'institution').__anext__()
            entity_related_data['addresses'].append(address)
            
            # Generate beneficial owners for institution
            num_owners = random.randint(1, self.config.get('max_beneficial_owners_per_institution', 2))
            async with ProgressTracker(num_owners, "Generating Beneficial Owners") as bo_progress:
                for _ in range(num_owners):
                    owner = await self.beneficial_owner_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['beneficial_owners'].append(owner)
                    await bo_progress.update(1)
            
            # Generate accounts and their transactions for institution
            num_accounts = random.randint(1, self.config.get('max_accounts_per_institution', 3))
            account_batch = []
            for _ in range(num_accounts):
                account = await self.account_gen.generate(institution.institution_id, 'institution').__anext__()
                account_batch.append(account)
                
            # Save accounts first
            if account_batch:
                await self.persist_batch({'accounts': account_batch})
                
            # Now generate and save transactions for each account
            for account in account_batch:
                # Generate transactions for this account
                num_transactions = random.randint(
                    self.config.get('min_transactions_per_account', 5),
                    self.config.get('max_transactions_per_account', 20)
                )
                async with ProgressTracker(num_transactions, "Generating Transactions") as tx_progress:
                    transaction_batch = []
                    for _ in range(num_transactions):
                        transaction = await self.transaction_gen.generate(account).__anext__()
                        transaction_batch.append(transaction)
                        await tx_progress.update(1)
                        
                        # Persist transactions in smaller batches
                        if len(transaction_batch) >= 10:  # Smaller batch size for transactions
                            await self.persist_batch({'transactions': transaction_batch})
                            transaction_batch = []
                            
                    # Persist any remaining transactions
                    if transaction_batch:
                        await self.persist_batch({'transactions': transaction_batch})
            
            # Generate risk assessments for institution
            num_risk_assessments = random.randint(1, self.config.get('max_risk_assessments_per_institution', 2))
            async with ProgressTracker(num_risk_assessments, "Generating Risk Assessments") as ra_progress:
                for _ in range(num_risk_assessments):
                    assessment = await self.risk_assessment_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['risk_assessments'].append(assessment)
                    await ra_progress.update(1)
            
            # Generate compliance events for institution
            num_events = random.randint(1, self.config.get('max_compliance_events_per_institution', 3))
            async with ProgressTracker(num_events, "Generating Compliance Events") as ce_progress:
                for _ in range(num_events):
                    event = await self.compliance_event_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['compliance_events'].append(event)
                    await ce_progress.update(1)
            
            # Generate authorized persons for institution
            num_auth_persons = random.randint(1, self.config.get('max_authorized_persons_per_institution', 3))
            async with ProgressTracker(num_auth_persons, "Generating Authorized Persons") as ap_progress:
                for _ in range(num_auth_persons):
                    auth_person = await self.authorized_person_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['authorized_persons'].append(auth_person)
                    await ap_progress.update(1)
            
            # Generate documents for institution
            num_documents = random.randint(1, self.config.get('max_documents_per_institution', 5))
            async with ProgressTracker(num_documents, "Generating Documents") as doc_progress:
                for _ in range(num_documents):
                    document = await self.document_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['documents'].append(document)
                    await doc_progress.update(1)
            
            # Generate jurisdiction presences for institution
            num_jurisdictions = random.randint(1, self.config.get('max_jurisdictions_per_institution', 3))
            async with ProgressTracker(num_jurisdictions, "Generating Jurisdiction Presences") as jp_progress:
                for _ in range(num_jurisdictions):
                    presence = await self.jurisdiction_presence_gen.generate(institution.institution_id, 'institution').__anext__()
                    entity_related_data['jurisdiction_presences'].append(presence)
                    await jp_progress.update(1)

        # Process each subsidiary
        for subsidiary in institution_subsidiary_batch['subsidiaries']:
            # Generate address for subsidiary
            subsidiary_address = await self.address_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
            entity_related_data['addresses'].append(subsidiary_address)
            
            # Generate beneficial owners for subsidiary
            num_sub_owners = random.randint(1, self.config.get('max_beneficial_owners_per_subsidiary', 2))
            async with ProgressTracker(num_sub_owners, "Generating Beneficial Owners") as bo_progress:
                for _ in range(num_sub_owners):
                    sub_owner = await self.beneficial_owner_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['beneficial_owners'].append(sub_owner)
                    await bo_progress.update(1)
            
            # Generate accounts and transactions for subsidiary
            num_sub_accounts = random.randint(1, self.config.get('max_accounts_per_subsidiary', 2))
            sub_account_batch = []
            for _ in range(num_sub_accounts):
                sub_account = await self.account_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                sub_account_batch.append(sub_account)
                
            # Save accounts first
            if sub_account_batch:
                await self.persist_batch({'accounts': sub_account_batch})
                
            # Now generate and save transactions for each account
            for account in sub_account_batch:
                # Generate transactions for subsidiary account
                num_sub_transactions = random.randint(
                    self.config.get('min_transactions_per_account', 5),
                    self.config.get('max_transactions_per_account', 20)
                )
                async with ProgressTracker(num_sub_transactions, "Generating Transactions") as tx_progress:
                    transaction_batch = []
                    for _ in range(num_sub_transactions):
                        transaction = await self.transaction_gen.generate(account).__anext__()
                        transaction_batch.append(transaction)
                        await tx_progress.update(1)
                        
                        # Persist transactions in smaller batches
                        if len(transaction_batch) >= 10:  # Smaller batch size for transactions
                            await self.persist_batch({'transactions': transaction_batch})
                            transaction_batch = []
                            
                    # Persist any remaining transactions
                    if transaction_batch:
                        await self.persist_batch({'transactions': transaction_batch})
            
            # Generate risk assessments for subsidiary
            num_sub_risk_assessments = random.randint(1, self.config.get('max_risk_assessments_per_subsidiary', 2))
            async with ProgressTracker(num_sub_risk_assessments, "Generating Risk Assessments") as ra_progress:
                for _ in range(num_sub_risk_assessments):
                    sub_assessment = await self.risk_assessment_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['risk_assessments'].append(sub_assessment)
                    await ra_progress.update(1)
            
            # Generate compliance events for subsidiary
            num_sub_events = random.randint(1, self.config.get('max_compliance_events_per_subsidiary', 2))
            async with ProgressTracker(num_sub_events, "Generating Compliance Events") as ce_progress:
                for _ in range(num_sub_events):
                    sub_event = await self.compliance_event_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['compliance_events'].append(sub_event)
                    await ce_progress.update(1)
            
            # Generate authorized persons for subsidiary
            num_sub_auth_persons = random.randint(1, self.config.get('max_authorized_persons_per_subsidiary', 2))
            async with ProgressTracker(num_sub_auth_persons, "Generating Authorized Persons") as ap_progress:
                for _ in range(num_sub_auth_persons):
                    sub_auth_person = await self.authorized_person_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['authorized_persons'].append(sub_auth_person)
                    await ap_progress.update(1)
            
            # Generate documents for subsidiary
            num_sub_documents = random.randint(1, self.config.get('max_documents_per_subsidiary', 3))
            async with ProgressTracker(num_sub_documents, "Generating Documents") as doc_progress:
                for _ in range(num_sub_documents):
                    sub_document = await self.document_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['documents'].append(sub_document)
                    await doc_progress.update(1)
            
            # Generate jurisdiction presences for subsidiary
            num_sub_jurisdictions = random.randint(1, self.config.get('max_jurisdictions_per_subsidiary', 2))
            async with ProgressTracker(num_sub_jurisdictions, "Generating Jurisdiction Presences") as jp_progress:
                for _ in range(num_sub_jurisdictions):
                    sub_presence = await self.jurisdiction_presence_gen.generate(subsidiary.subsidiary_id, 'subsidiary').__anext__()
                    entity_related_data['jurisdiction_presences'].append(sub_presence)
                    await jp_progress.update(1)

        # Now persist all the entity-related data
        await self.persist_batch(entity_related_data)

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
