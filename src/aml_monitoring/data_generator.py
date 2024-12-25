"""
Data Generator Module for AML Transaction Monitoring System.

This module provides a structured approach to generate test data for the AML monitoring system.
It uses generators for efficient memory usage and provides progress tracking.
"""

from abc import ABC, abstractmethod
from typing import Generator, Dict, List, Any, Optional, Union, AsyncGenerator
import asyncio
import uuid
import random
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
import numpy as np
from faker import Faker
import pandas as pd
import json

from .database import PostgresHandler, Neo4jHandler
from .models import (
    Institution, Address, Account, BeneficialOwner, Transaction,
    BusinessType, OperationalStatus, RiskRating, TransactionType,
    TransactionStatus, RiskAssessment, AuthorizedPerson, Document,
    JurisdictionPresence, ComplianceEvent, ComplianceEventType, Subsidiary
)

class ProgressTracker:
    """Tracks progress of data generation with tqdm."""
    
    def __init__(self, total: int, description: str):
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

class BaseGenerator(ABC):
    """Base class for all data generators."""
    
    def __init__(self, config: Dict[str, Any], fake: Optional[Faker] = None):
        self.config = config
        self.fake = fake or Faker()
        
    @abstractmethod
    async def generate(self) -> AsyncGenerator[Any, None]:
        """Generate data items."""
        pass

class InstitutionGenerator(BaseGenerator):
    """Generates institution data."""
    
    async def generate(self) -> AsyncGenerator[Institution, None]:
        num_institutions = self.config['num_institutions']
        high_risk_threshold = self.config.get('high_risk_percentage', 0.1)
        
        async with ProgressTracker(num_institutions, "Generating Institutions") as tracker:
            for _ in range(num_institutions):
                business_type = random.choice(list(BusinessType))
                
                # Determine risk rating based on high_risk_percentage
                if random.random() < high_risk_threshold:
                    risk_rating = RiskRating.HIGH
                else:
                    risk_rating = random.choice([RiskRating.LOW, RiskRating.MEDIUM])
                    
                operational_status = random.choice(list(OperationalStatus))
                
                incorporation_date = self.fake.date_between(
                    start_date=self.config.get('date_start', '-30y'),
                    end_date=self.config.get('date_end', '-1y')
                )
                onboarding_date = self.fake.date_between(
                    start_date=incorporation_date,
                    end_date=self.config.get('date_end', 'today')
                )
                
                institution = Institution(
                    institution_id=str(uuid.uuid4()),
                    legal_name=self.fake.company(),
                    business_type=business_type.value,
                    incorporation_country=self.fake.country_code(),
                    incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                    onboarding_date=onboarding_date.strftime('%Y-%m-%d'),
                    risk_rating=risk_rating.value,
                    operational_status=operational_status.value,
                    primary_currency=random.choice(['USD', 'EUR', 'GBP', 'JPY']),
                    regulatory_status='regulated',
                    primary_business_activity=f"{business_type.value}_activities",
                    primary_regulator=f"{self.fake.country_code()}_regulator",
                    licenses=[f"license_{i}" for i in range(random.randint(1, 3))],
                    aml_program_status='active',
                    kyc_refresh_date=self.fake.date_between(
                        start_date=onboarding_date,
                        end_date=self.config.get('date_end', 'today')
                    ).strftime('%Y-%m-%d'),
                    last_audit_date=self.fake.date_between(
                        start_date=onboarding_date,
                        end_date=self.config.get('date_end', 'today')
                    ).strftime('%Y-%m-%d'),
                    next_audit_date=self.fake.date_between(
                        start_date='today',
                        end_date='+1y'
                    ).strftime('%Y-%m-%d'),
                    relationship_manager=self.fake.name(),
                    relationship_status='active',
                    swift_code=self.fake.bothify(text='????##??'),
                    lei_code=self.fake.bothify(text='########????????####'),
                    tax_id=self.fake.bothify(text='??########'),
                    website=self.fake.url(),
                    primary_contact_name=self.fake.name(),
                    primary_contact_email=self.fake.company_email(),
                    primary_contact_phone=self.fake.phone_number(),
                    annual_revenue=float(random.randint(1000000, 1000000000)),
                    employee_count=random.randint(50, 10000),
                    year_established=incorporation_date.year,
                    customer_status='active',
                    last_review_date=self.fake.date_between(
                        start_date=onboarding_date,
                        end_date=self.config.get('date_end', 'today')
                    ).strftime('%Y-%m-%d'),
                    industry_codes=[str(random.randint(1000, 9999)) for _ in range(2)],
                    public_company=random.choice([True, False])
                )
                
                await tracker.update()
                yield institution

class AddressGenerator(BaseGenerator):
    """Generates address data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Address, None]:
        num_addresses = random.randint(1, 3)
        
        for i in range(num_addresses):
            address = Address(
                address_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                address_type='primary' if i == 0 else 'secondary',
                address_line1=self.fake.street_address(),
                address_line2=self.fake.secondary_address() if random.random() > 0.5 else None,
                city=self.fake.city(),
                state_province=self.fake.state(),
                postal_code=self.fake.postcode(),
                country=self.fake.country_code(),
                status='active',
                effective_from=self.fake.date_this_decade().strftime('%Y-%m-%d'),
                effective_to=None,
                primary_address=i == 0,
                validation_status='verified',
                last_verified=self.fake.date_this_year().strftime('%Y-%m-%d'),
                geo_coordinates={'latitude': float(self.fake.latitude()),
                               'longitude': float(self.fake.longitude())},
                timezone=self.fake.timezone()
            )
            yield address

class AccountGenerator(BaseGenerator):
    """Generates account data for institutions."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Account, None]:
        num_accounts = random.randint(
            self.config.get('min_accounts_per_institution', 1),
            self.config.get('max_accounts_per_institution', 5)
        )
        
        for _ in range(num_accounts):
            opening_date = self.fake.date_between(
                start_date=self.config.get('date_start', '-10y'),
                end_date=self.config.get('date_end', 'today')
            )
            account = Account(
                account_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                account_type=random.choice(['checking', 'savings', 'investment']),
                account_number=self.fake.bothify(text='########'),
                currency=random.choice(['USD', 'EUR', 'GBP']),
                status='active',
                opening_date=opening_date.strftime('%Y-%m-%d'),
                last_activity_date=self.fake.date_between(
                    start_date=opening_date).strftime('%Y-%m-%d'),
                balance=round(random.uniform(10000, 1000000), 2),
                risk_rating=random.choice(list(RiskRating)),
                purpose='business',
                average_monthly_balance=round(random.uniform(5000, 500000), 2),
                custodian_bank=self.fake.company(),
                account_officer=self.fake.name(),
                custodian_country=self.fake.country_code()
            )
            yield account

class TransactionGenerator(BaseGenerator):
    """Generates transaction data for accounts."""
    
    async def generate(self, account: Account) -> AsyncGenerator[Transaction, None]:
        num_transactions = random.randint(
            self.config.get('min_transactions_per_account', 50),
            self.config.get('max_transactions_per_account', 200)
        )
        
        start_date = datetime.now() - timedelta(days=730)  # 2 years ago
        
        for _ in range(num_transactions):
            is_debit = random.choice([True, False])
            amount = round(random.uniform(100, 100000), 2)
            
            transaction = Transaction(
                transaction_id=str(uuid.uuid4()),
                transaction_type=random.choice(list(TransactionType)),
                transaction_date=(start_date + timedelta(
                    days=random.randint(1, 730))).strftime('%Y-%m-%d'),
                amount=amount,
                currency=account.currency,
                transaction_status=random.choice(list(TransactionStatus)),
                is_debit=is_debit,
                account_id=account.account_id,
                counterparty_account=self.fake.bothify(text='########'),
                counterparty_name=self.fake.company(),
                counterparty_bank=self.fake.company(),
                entity_id=account.entity_id,
                entity_type=account.entity_type,
                counterparty_entity_name=self.fake.company(),
                originating_country=self.fake.country_code(),
                destination_country=self.fake.country_code(),
                purpose=random.choice(['payment', 'transfer', 'investment']),
                reference_number=self.fake.bothify(text='??####??####'),
                screening_alert=random.random() < 0.05,  # 5% chance of alert
                risk_score=random.randint(0, 100) if random.random() < 0.1 else None,
                processing_fee=round(random.uniform(1, 100), 2),
                exchange_rate=round(random.uniform(0.8, 1.2), 4),
                value_date=(start_date + timedelta(
                    days=random.randint(1, 730))).strftime('%Y-%m-%d'),
                batch_id=str(uuid.uuid4()) if random.random() < 0.3 else None
            )
            yield transaction

class BeneficialOwnerGenerator(BaseGenerator):
    """Generates beneficial owner data for institutions."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[BeneficialOwner, None]:
        num_owners = random.randint(
            self.config.get('min_beneficial_owners', 1),
            self.config.get('max_beneficial_owners', 3)
        )
        
        async with ProgressTracker(num_owners, "Generating Beneficial Owners") as tracker:
            for _ in range(num_owners):
                owner = BeneficialOwner(
                    owner_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    name=self.fake.name(),
                    nationality=self.fake.country_code(),
                    country_of_residence=self.fake.country_code(),
                    ownership_percentage=round(random.uniform(5, 100), 2),
                    dob=self.fake.date_of_birth(minimum_age=18).strftime('%Y-%m-%d'),
                    verification_date=self.fake.date_this_year().strftime('%Y-%m-%d'),
                    pep_status=random.choice([True, False]),
                    sanctions_status=random.choice([True, False])
                )
                
                await tracker.update()
                yield owner

class RiskAssessmentGenerator(BaseGenerator):
    """Generates risk assessment data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[RiskAssessment, None]:
        num_assessments = random.randint(2, 4)  # Multiple assessments over time
        
        for _ in range(num_assessments):
            risk_factors = {
                'geographic': random.randint(1, 5),
                'product': random.randint(1, 5),
                'client': random.randint(1, 5),
                'transaction': random.randint(1, 5),
                'delivery_channel': random.randint(1, 5)
            }
            
            assessment_date = self.fake.date_this_year()
            
            assessment = RiskAssessment(
                assessment_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                assessment_date=assessment_date.strftime('%Y-%m-%d'),
                risk_rating=random.choice(list(RiskRating)).value,
                risk_score=str(sum(risk_factors.values()) / len(risk_factors)),
                assessment_type=random.choice(['initial', 'periodic', 'triggered']),
                risk_factors=risk_factors,
                conducted_by=self.fake.name(),
                approved_by=self.fake.name(),
                findings=self.fake.text(max_nb_chars=200),
                assessor=self.fake.company(),
                next_review_date=(assessment_date + timedelta(days=365)).strftime('%Y-%m-%d'),
                notes=self.fake.text(max_nb_chars=100) if random.random() > 0.5 else None
            )
            yield assessment

class AuthorizedPersonGenerator(BaseGenerator):
    """Generates authorized person data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[AuthorizedPerson, None]:
        num_persons = random.randint(2, 5)
        
        for _ in range(num_persons):
            auth_start = self.fake.date_this_decade()
            auth_end = None if random.random() > 0.3 else self.fake.date_between(
                start_date=auth_start,
                end_date=datetime.now() + timedelta(days=365)
            )
            
            person = AuthorizedPerson(
                person_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                name=self.fake.name(),
                title=random.choice(['Director', 'CFO', 'CEO', 'Treasurer', 'Controller']),
                authorization_level=random.choice(['full', 'limited', 'view_only']),
                authorization_type=random.choice(['signatory', 'trader', 'administrator']),
                authorization_start=auth_start.strftime('%Y-%m-%d'),
                authorization_end=auth_end.strftime('%Y-%m-%d') if auth_end else None,
                contact_info={
                    'email': self.fake.company_email(),
                    'phone': self.fake.phone_number(),
                    'office': self.fake.phone_number()
                },
                is_active=auth_end is None,
                last_verification_date=self.fake.date_this_year().strftime('%Y-%m-%d')
            )
            yield person

class DocumentGenerator(BaseGenerator):
    """Generates document data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Document, None]:
        num_documents = random.randint(
            self.config.get('min_documents', 1),
            self.config.get('max_documents', 5)
        )
        
        for _ in range(num_documents):
            issue_date = self.fake.date_between(
                start_date=self.config.get('date_start', '-10y'),
                end_date=self.config.get('date_end', 'today')
            )
            expiry_date = self.fake.date_between(
                start_date=issue_date,
                end_date='+5y'
            )
            
            document = Document(
                document_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                document_type=random.choice(['passport', 'license', 'certificate']),
                document_number=self.fake.bothify(text='??####??####'),
                issuing_authority=f"{self.fake.country()}_authority",
                issuing_country=self.fake.country_code(),
                issue_date=issue_date.strftime('%Y-%m-%d'),
                expiry_date=expiry_date.strftime('%Y-%m-%d'),
                verification_status='verified',
                verification_date=self.fake.date_this_year().strftime('%Y-%m-%d')
            )
            yield document

class JurisdictionPresenceGenerator(BaseGenerator):
    """Generates jurisdiction presence data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[JurisdictionPresence, None]:
        num_jurisdictions = random.randint(1, 4)
        
        for _ in range(num_jurisdictions):
            registration_date = self.fake.date_this_decade()
            effective_from = registration_date
            effective_to = None if random.random() > 0.2 else self.fake.date_between(
                start_date=effective_from,
                end_date=datetime.now() + timedelta(days=365)
            )
            
            presence = JurisdictionPresence(
                presence_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                jurisdiction=self.fake.country_code(),
                registration_date=registration_date.strftime('%Y-%m-%d'),
                effective_from=effective_from.strftime('%Y-%m-%d'),
                effective_to=effective_to.strftime('%Y-%m-%d') if effective_to else None,
                status='active' if not effective_to else 'inactive',
                local_registration_id=self.fake.bothify(text='???###???'),
                local_registration_date=registration_date.strftime('%Y-%m-%d'),
                local_registration_authority=f"{self.fake.country()}_registry",
                notes=self.fake.text(max_nb_chars=100) if random.random() > 0.7 else None
            )
            yield presence

class ComplianceEventGenerator(BaseGenerator):
    """Generates compliance event data for entities."""
    
    async def generate(self, entity_id: str, entity_type: str, account_id: str) -> AsyncGenerator[ComplianceEvent, None]:
        num_events = random.randint(2, 6)
        
        for _ in range(num_events):
            event_type = random.choice(list(ComplianceEventType))
            event_date = self.fake.date_this_year()
            
            # Generate appropriate old and new states based on event type
            old_state = None
            new_state = None
            if event_type == ComplianceEventType.RISK_LEVEL_CHANGE:
                old_state = random.choice(list(RiskRating)).value
                new_state = random.choice(list(RiskRating)).value
                while new_state == old_state:
                    new_state = random.choice(list(RiskRating)).value
            elif event_type == ComplianceEventType.ACCOUNT_OPENED:
                new_state = 'active'
            elif event_type == ComplianceEventType.ACCOUNT_CLOSED:
                old_state = 'active'
                new_state = 'closed'
            else:
                new_state = random.choice(['completed', 'pending', 'flagged'])
            
            event = ComplianceEvent(
                event_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                event_date=event_date.strftime('%Y-%m-%d'),
                event_type=event_type.value,
                event_description=self.fake.text(max_nb_chars=200),
                old_state=old_state,
                new_state=new_state,
                decision=random.choice(['approved', 'rejected', 'escalated']) if random.random() > 0.3 else None,
                decision_date=(event_date + timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%d'),
                decision_maker=self.fake.name(),
                next_review_date=(event_date + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
                related_account_id=account_id,
                notes=self.fake.text(max_nb_chars=100) if random.random() > 0.7 else None
            )
            yield event

class SubsidiaryGenerator(BaseGenerator):
    """Generates subsidiary data for institutions."""
    
    async def generate(self, parent_institution_id: str) -> AsyncGenerator[Subsidiary, None]:
        num_subsidiaries = random.randint(1, 3)
        
        for _ in range(num_subsidiaries):
            incorporation_date = self.fake.date_between(start_date='-10y', end_date='-1y')
            acquisition_date = self.fake.date_between(start_date=incorporation_date, end_date='today')
            is_customer = random.choice([True, False])
            
            subsidiary_data = {
                'subsidiary_id': str(uuid.uuid4()),
                'parent_institution_id': parent_institution_id,
                'legal_name': self.fake.company(),
                'tax_id': self.fake.numerify(text='##-#######'),
                'incorporation_country': self.fake.country_code(),
                'incorporation_date': incorporation_date.strftime('%Y-%m-%d'),
                'acquisition_date': acquisition_date.strftime('%Y-%m-%d'),
                'business_type': random.choice(list(BusinessType)).value,
                'operational_status': random.choice(list(OperationalStatus)).value,
                'parent_ownership_percentage': round(random.uniform(51.0, 100.0), 2),
                'consolidation_status': random.choice(['FULL', 'PARTIAL', 'NONE']),
                'capital_investment': round(random.uniform(1000000, 100000000), 2),
                'functional_currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CHF']),
                'material_subsidiary': random.choice([True, False]),
                'risk_classification': random.choice(list(RiskRating)).value,
                'regulatory_status': random.choice(['COMPLIANT', 'UNDER_REVIEW', 'NON_COMPLIANT']),
                'local_licenses': [self.fake.license_plate() for _ in range(random.randint(1, 3))],
                'integration_status': random.choice(['FULL', 'PARTIAL', 'IN_PROGRESS', 'NOT_STARTED']),
                'financial_metrics': {
                    'revenue': round(random.uniform(1000000, 50000000), 2),
                    'profit_margin': round(random.uniform(0.05, 0.3), 2),
                    'asset_value': round(random.uniform(5000000, 200000000), 2)
                },
                'reporting_frequency': random.choice(['MONTHLY', 'QUARTERLY', 'SEMI_ANNUAL', 'ANNUAL']),
                'requires_local_audit': random.choice([True, False]),
                'corporate_governance_model': random.choice(['CENTRALIZED', 'DECENTRALIZED', 'HYBRID']),
                'is_regulated': random.choice([True, False]),
                'industry_codes': [str(random.randint(1000, 9999)) for _ in range(random.randint(1, 3))],
                'is_customer': is_customer
            }
            
            if is_customer:
                onboarding_date = self.fake.date_between(start_date=acquisition_date, end_date='today')
                subsidiary_data.update({
                    'customer_id': str(uuid.uuid4()),
                    'customer_onboarding_date': onboarding_date.strftime('%Y-%m-%d'),
                    'customer_risk_rating': random.choice(list(RiskRating)).value,
                    'customer_status': random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED'])
                })
            
            subsidiary = Subsidiary(**subsidiary_data)
            yield subsidiary

class DataGenerator:
    """Main class for orchestrating data generation."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.fake = Faker()
        
        # Initialize generators
        self.institution_gen = InstitutionGenerator(config, self.fake)
        self.address_gen = AddressGenerator(config, self.fake)
        self.account_gen = AccountGenerator(config, self.fake)
        self.transaction_gen = TransactionGenerator(config, self.fake)
        self.beneficial_owner_gen = BeneficialOwnerGenerator(config, self.fake)
        self.risk_assessment_gen = RiskAssessmentGenerator(config, self.fake)
        self.authorized_person_gen = AuthorizedPersonGenerator(config, self.fake)
        self.document_gen = DocumentGenerator(config, self.fake)
        self.jurisdiction_presence_gen = JurisdictionPresenceGenerator(config, self.fake)
        self.compliance_event_gen = ComplianceEventGenerator(config, self.fake)
        self.subsidiary_gen = SubsidiaryGenerator(config, self.fake)
        
        # Initialize database handlers
        self.postgres_handler = PostgresHandler()
        self.neo4j_handler = Neo4jHandler()
        
    async def initialize_db(self):
        """Initialize database connections."""
        # First connect to databases
        await self.postgres_handler.connect()
        await self.neo4j_handler.connect()
        
        # Then initialize schemas
        await self.postgres_handler.initialize_database()
        # await self.neo4j_handler.initialize_database()
        
    async def close_db(self):
        """Close database connections."""
        await self.postgres_handler.disconnect()
        await self.neo4j_handler.disconnect()
        
    def _convert_to_dataframe(self, data: List[Any]) -> pd.DataFrame:
        """Convert list of Pydantic models to DataFrame."""
        # Convert each model to a dict and handle JSON fields
        rows = []
        for item in data:
            row = item.model_dump()
            # Handle JSONB fields by converting dicts to JSON strings
            if isinstance(row.get('financial_metrics'), dict):
                row['financial_metrics'] = json.dumps(row['financial_metrics'])
            if isinstance(row.get('payment_details'), dict):
                row['payment_details'] = json.dumps(row['payment_details'])
            if isinstance(row.get('risk_factors'), dict):
                row['risk_factors'] = json.dumps(row['risk_factors'])
            if isinstance(row.get('contact_info'), dict):
                row['contact_info'] = json.dumps(row['contact_info'])
            rows.append(row)
            
        return pd.DataFrame(rows)
        
    async def persist_batch(self, data: Dict[str, List[Any]], batch_size: int = 1000):
        """Persist a batch of data to both databases."""
        # Convert data to DataFrames
        df_data = {
            key: self._convert_to_dataframe(value) 
            for key, value in data.items()
        }
        
        # Save to PostgreSQL
        try:
            await self.postgres_handler.save_batch(df_data)
        except Exception as e:
            print(f"Error saving to PostgreSQL: {str(e)}")
            raise
            
        # Save to Neo4j
        try:
            await self.neo4j_handler.save_to_neo4j(df_data)
        except Exception as e:
            print(f"Error saving to Neo4j: {str(e)}")
            raise
        
    async def generate_all(self) -> None:
        """Generate all data types and persist them."""
        try:
            data = {
                'institutions': [],
                'addresses': [],
                'accounts': [],
                'beneficial_owners': [],
                'transactions': [],
                'risk_assessments': [],
                'authorized_persons': [],
                'documents': [],
                'jurisdiction_presences': [],
                'compliance_events': [],
                'subsidiaries': []
            }
            batch_size = self.config.get('batch_size', 1000)
            
            # Generate institutions and related data
            async for institution in self.institution_gen.generate():
                data['institutions'].append(institution)
                
                # Generate related data for each institution
                async for address in self.address_gen.generate(institution.institution_id, 'institution'):
                    data['addresses'].append(address)
                    
                async for owner in self.beneficial_owner_gen.generate(institution.institution_id, 'institution'):
                    data['beneficial_owners'].append(owner)
                    
                async for assessment in self.risk_assessment_gen.generate(institution.institution_id, 'institution'):
                    data['risk_assessments'].append(assessment)
                    
                async for person in self.authorized_person_gen.generate(institution.institution_id, 'institution'):
                    data['authorized_persons'].append(person)
                    
                async for document in self.document_gen.generate(institution.institution_id, 'institution'):
                    data['documents'].append(document)
                    
                async for presence in self.jurisdiction_presence_gen.generate(institution.institution_id, 'institution'):
                    data['jurisdiction_presences'].append(presence)
                
                # Generate accounts and transactions
                async for account in self.account_gen.generate(institution.institution_id, 'institution'):
                    data['accounts'].append(account)
                    
                    async for transaction in self.transaction_gen.generate(account):
                        data['transactions'].append(transaction)
                        
                    async for event in self.compliance_event_gen.generate(
                        institution.institution_id, 'institution', account.account_id):
                        data['compliance_events'].append(event)
                
                # Generate subsidiaries
                async for subsidiary in self.subsidiary_gen.generate(institution.institution_id):
                    data['subsidiaries'].append(subsidiary)
                
                # Persist batch if size threshold reached
                if len(data['institutions']) >= batch_size:
                    await self.persist_batch(data)
                    data = {k: [] for k in data.keys()}  # Reset data dict
        
            # Persist any remaining data
            if any(len(v) > 0 for v in data.values()):
                await self.persist_batch(data)
                
        except Exception as e:
            print(f"Error during data generation: {str(e)}")
            raise
            
        finally:
            await self.close_db()

async def generate_test_data(config: Dict[str, Any]) -> None:
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
    generator = DataGenerator(config)
    await generator.generate_all()
