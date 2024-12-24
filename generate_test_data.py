import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, date, timedelta
import json
import uuid
import random
from typing import List, Dict, Optional
import argparse
from models import *
from pydantic import BaseModel
import os

fake = Faker()

class RiskAssessment(BaseModel):
    assessment_id: str
    entity_id: str
    entity_type: str
    assessment_date: str
    risk_score: str
    assessment_type: str
    risk_factors: Dict[str, int]
    conducted_by: str
    approved_by: str
    findings: str
    next_review_date: str
    notes: Optional[str]

class BeneficialOwner(BaseModel):
    owner_id: str
    institution_id: str
    full_name: str
    dob: str
    nationality: str
    ownership_percentage: float
    pep_status: str
    identification_type: str
    identification_number: str
    verification_date: str
    verification_source: str

class AuthorizedPerson(BaseModel):
    auth_person_id: str
    institution_id: str
    full_name: str
    title: str
    role: str
    permissions: List[str]
    authorization_start: str
    authorization_end: Optional[str]
    status: str
    contact_details: Dict[str, str]
    verification_status: str

class Document(BaseModel):
    document_id: str
    institution_id: str
    document_type: str
    document_number: str
    issuing_authority: str
    issuing_country: str
    submission_date: str
    verification_date: str
    expiry_date: Optional[str]
    document_status: str
    verification_status: str
    verification_method: str
    file_reference: str
    metadata: Dict[str, str]

class JurisdictionPresence(BaseModel):
    presence_id: str
    entity_id: str
    entity_type: str
    jurisdiction: str
    presence_type: str
    regulatory_status: str
    local_licenses: List[str]
    establishment_date: str
    local_regulator: str
    compliance_status: str
    reporting_requirements: Dict[str, str]
    supervisory_authority: str
    material_entity_flag: bool

class Account(BaseModel):
    account_id: str
    entity_id: str
    entity_type: str
    account_type: str
    account_number: str
    currency: str
    status: str
    opening_date: str
    last_activity_date: str
    balance: float
    risk_rating: str
    purpose: str
    average_monthly_balance: float
    custodian_bank: Optional[str]
    account_officer: str

class SubsidiaryRelationship(BaseModel):
    relationship_id: str
    parent_id: str
    subsidiary_id: str
    ownership_percentage: float
    relationship_type: str
    control_type: str
    relationship_status: str
    relationship_start: str
    approval_status: str
    risk_factors: Dict[str, int]

class Subsidiary(BaseModel):
    subsidiary_id: str
    parent_institution_id: str
    legal_name: str
    tax_id: str
    incorporation_country: str
    incorporation_date: str
    business_type: str
    operational_status: str
    parent_ownership_percentage: float
    consolidation_status: str
    capital_investment: float
    functional_currency: str
    material_subsidiary: bool
    risk_classification: str
    regulatory_status: str
    local_licenses: List[str]
    acquisition_date: str
    integration_status: str
    financial_metrics: Dict[str, float]
    reporting_frequency: str
    requires_local_audit: bool
    corporate_governance_model: str
    is_customer: bool
    customer_id: Optional[str]
    customer_onboarding_date: Optional[str]
    customer_risk_rating: Optional[str]
    customer_status: Optional[str]

class Institution(BaseModel):
    institution_id: str
    legal_name: str
    tax_id: str
    incorporation_country: str
    incorporation_date: str
    business_type: str
    primary_business_activity: str
    regulatory_status: str
    primary_regulator: str
    licenses: List[str]
    annual_revenue: float
    employee_count: int
    risk_rating: str
    aml_program_status: str
    customer_status: str
    onboarding_date: str
    last_review_date: str
    relationship_manager: str
    swift_code: Optional[str]
    lei_code: Optional[str]
    industry_codes: Dict[str, str]
    website: str

class ComplianceEvent(BaseModel):
    event_id: str
    entity_id: str
    entity_type: str
    event_date: str
    event_type: str
    event_description: str
    old_state: str
    new_state: str
    decision: str
    decision_date: str
    decision_maker: str
    next_review_date: str
    related_account_id: str
    notes: str

class ComplianceEventType(str, Enum):
    ONBOARDING = 'onboarding'
    ACCOUNT_OPENED = 'account_opened'
    ACCOUNT_CLOSED = 'account_closed'
    PERIODIC_REVIEW = 'periodic_review'
    RISK_LEVEL_CHANGE = 'risk_level_change'
    ENHANCED_DUE_DILIGENCE = 'enhanced_due_diligence'
    KYC_UPDATE = 'kyc_update'
    ADVERSE_MEDIA = 'adverse_media'
    SANCTIONS_SCREENING = 'sanctions_screening'

class ReportingRequirements(BaseModel):
    frequency: str
    reports: List[str]
    next_due_date: str

class Address(BaseModel):
    address_id: str
    entity_id: str
    entity_type: str
    address_type: str
    street_address: str
    city: str
    state: str
    postal_code: str
    country: str
    is_primary: bool
    effective_from: str
    last_verified: str
    verification_source: str
    notes: Optional[str]

class TestDataGenerator:
    COUNTRIES = [
        'US', 'GB', 'DE', 'FR', 'CH', 'JP', 'SG', 'HK', 'CA', 'AU', 'NZ', 'ES', 'IT', 'NL',
        'SE', 'DK', 'NO', 'FI', 'BE', 'IE', 'PT', 'AT', 'LU', 'MY', 'TH', 'KR'
    ]

    BUSINESS_TYPES = [
        'BANK', 'BROKER_DEALER', 'ASSET_MANAGER', 'INSURANCE', 'PAYMENT_PROVIDER',
        'CRYPTO_EXCHANGE', 'INVESTMENT_FIRM', 'PENSION_FUND', 'HEDGE_FUND', 'TRUST_COMPANY'
    ]

    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'SGD', 'HKD', 'AUD', 'CAD', 'NZD']

    def __init__(self, num_institutions=10, seed=None, output_dir='./data'):
        """Initialize the test data generator.
        
        Args:
            num_institutions (int): Number of institutions to generate
            seed (int): Random seed for reproducibility
            output_dir (str): Directory to save output CSV files
        """
        self.num_institutions = num_institutions
        self.output_dir = os.path.abspath(output_dir)
        
        # Set random seed if provided
        if seed is not None:
            random.seed(seed)
            fake.seed_instance(seed)
            np.random.seed(seed)
        
        # Constants
        self.countries = self.COUNTRIES
        self.business_types = self.BUSINESS_TYPES
        self.currencies = self.CURRENCIES
        
        # Initialize data dictionary
        self.data = {
            'institutions': None,
            'subsidiaries': None,
            'addresses': None,
            'risk_assessments': None,
            'beneficial_owners': None,
            'authorized_persons': None,
            'documents': None,
            'jurisdiction_presences': None,
            'accounts': None,
            'subsidiary_relationships': None,
            'compliance_events': None
        }

    def _validate_and_convert_to_dict(self, model_instance: BaseModel) -> dict:
        """Validate and convert a Pydantic model instance to a dictionary with proper date handling."""
        try:
            # Convert to dict and handle date serialization
            data = model_instance.model_dump()
            
            # Convert dates to ISO format strings
            for key, value in data.items():
                if isinstance(value, date):
                    data[key] = value.isoformat()
                elif isinstance(value, dict):
                    for k, v in value.items():
                        if isinstance(v, date):
                            value[k] = v.isoformat()
            
            return data
        except Exception as e:
            print(f"Validation error for {type(model_instance).__name__}: {e}")
            raise

    def generate_institution_data(self):
        """Generate institution data."""
        print("Generating institutions...")
        institutions = []
        
        for _ in range(self.num_institutions):
            business_type = random.choice(self.business_types).lower()
            incorporation_date = fake.date_between(start_date='-20y', end_date='-1y')
            onboarding_date = fake.date_between(start_date=incorporation_date, end_date='today')
            last_review_date = fake.date_between(start_date=onboarding_date, end_date='today')
            
            try:
                institution = Institution(
                    institution_id=str(uuid.uuid4()),
                    legal_name=fake.company(),
                    business_type=business_type,
                    incorporation_country=random.choice(self.countries),
                    incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                    onboarding_date=onboarding_date.strftime('%Y-%m-%d'),
                    risk_rating=random.choice(['low', 'medium', 'high']),
                    operational_status='active',
                    primary_currency=random.choice(self.currencies),
                    regulatory_status=random.choice(['fully_regulated', 'limited_permission', 'exempt']),
                    primary_business_activity=self._get_business_activity(business_type),
                    primary_regulator=self._get_regulator(business_type),
                    licenses=self._generate_licenses(business_type),
                    aml_program_status=random.choice(['approved', 'under_review', 'enhancement_required']),
                    kyc_refresh_date=fake.date_between(start_date='-1y', end_date='+1y').strftime('%Y-%m-%d'),
                    last_audit_date=fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
                    next_audit_date=fake.date_between(start_date='today', end_date='+2y').strftime('%Y-%m-%d'),
                    relationship_manager=fake.name(),
                    relationship_status='active',
                    swift_code=fake.bothify(text='????##??###'),
                    lei_code=fake.bothify(text='????????0000########??'),
                    tax_id=fake.numerify(text='##-#######'),
                    website=fake.url(),
                    primary_contact_name=fake.name(),
                    primary_contact_email=fake.email(),
                    primary_contact_phone=fake.phone_number(),
                    annual_revenue=round(random.uniform(1000000, 1000000000), 2),
                    employee_count=random.randint(50, 10000),
                    year_established=random.randint(1950, 2020),
                    customer_status='active',
                    last_review_date=last_review_date.strftime('%Y-%m-%d'),
                    industry_codes=self._generate_industry_codes(),
                    public_company=random.choice([True, False]),
                    stock_symbol=fake.bothify(text='????') if random.random() > 0.7 else None,
                    stock_exchange=random.choice(['NYSE', 'NASDAQ', 'LSE', 'TSE']) if random.random() > 0.7 else None
                )
                institutions.append(self._validate_and_convert_to_dict(institution))
            except Exception as e:
                print(f"Error generating institution: {e}")
                continue
        
        self.data['institutions'] = pd.DataFrame(institutions)
        return self.data['institutions']

    def generate_subsidiaries(self):
        """Generate subsidiary data for institutions."""
        print("Generating subsidiaries...")
        subsidiaries = []
        
        for institution in self.data['institutions'].to_dict('records'):
            num_subsidiaries = random.randint(1, 3)
            for _ in range(num_subsidiaries):
                incorporation_date = fake.date_between(start_date='-10y', end_date='-1y')
                acquisition_date = fake.date_between(start_date=incorporation_date, end_date='today')
                is_customer = random.choice([True, False])
                business_type = random.choice(['bank', 'insurance', 'investment_firm', 'other_financial'])
                
                # Convert percentage to float (remove % and divide by 100)
                capital_ratio = round(random.uniform(8, 15), 2)
                
                try:
                    subsidiary_data = {
                        'subsidiary_id': str(uuid.uuid4()),
                        'parent_institution_id': institution['institution_id'],
                        'legal_name': fake.company(),
                        'tax_id': fake.numerify(text='##-#######'),
                        'incorporation_country': random.choice(['US', 'GB', 'DE', 'FR', 'CH', 'JP', 'SG', 'HK', 'CA', 'AU']),
                        'incorporation_date': incorporation_date.strftime('%Y-%m-%d'),
                        'acquisition_date': acquisition_date.strftime('%Y-%m-%d'),
                        'business_type': business_type,
                        'operational_status': random.choice(['active', 'inactive', 'dissolved']),
                        'parent_ownership_percentage': round(random.uniform(51, 100), 2),
                        'consolidation_status': random.choice(['full', 'equity_method', 'not_consolidated']),
                        'capital_investment': round(random.uniform(1000000, 50000000), 2),
                        'functional_currency': random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CHF']),
                        'material_subsidiary': random.choice([True, False]),
                        'risk_classification': random.choice(['low', 'medium', 'high']),
                        'regulatory_status': random.choice(['fully_regulated', 'limited_permission', 'exempt']),
                        'local_licenses': [f"{business_type}_license_{i}" for i in range(random.randint(1, 3))],
                        'integration_status': random.choice(['fully_integrated', 'partially_integrated', 'standalone']),
                        'financial_metrics': {
                            'total_assets': random.randint(1000000, 1000000000),
                            'annual_revenue': random.randint(100000, 100000000),
                            'capital_ratio': capital_ratio,
                            'net_income': random.randint(-1000000, 10000000)
                        },
                        'reporting_frequency': random.choice(['monthly', 'quarterly', 'annually']),
                        'requires_local_audit': random.choice([True, False]),
                        'corporate_governance_model': random.choice(['board_of_directors', 'supervisory_board', 'executive_committee']),
                        'is_regulated': random.choice([True, False]),
                        'is_customer': is_customer
                    }
                    
                    # Add customer fields if is_customer is True
                    if is_customer:
                        customer_onboarding = fake.date_between(start_date=acquisition_date, end_date='today')
                        subsidiary_data.update({
                            'customer_id': str(uuid.uuid4()),
                            'customer_onboarding_date': customer_onboarding.strftime('%Y-%m-%d'),
                            'customer_risk_rating': random.choice(['low', 'medium', 'high']),
                            'customer_status': random.choice(['active', 'inactive', 'pending'])
                        })
                    else:
                        # Add placeholder values for required customer fields
                        subsidiary_data.update({
                            'customer_id': None,
                            'customer_onboarding_date': None,
                            'customer_risk_rating': None,
                            'customer_status': None
                        })
                    
                    subsidiary = Subsidiary(**subsidiary_data)
                    subsidiaries.append(self._validate_and_convert_to_dict(subsidiary))
                except Exception as e:
                    print(f"Validation error for subsidiary: {e}")
                    continue
        
        self.data['subsidiaries'] = pd.DataFrame(subsidiaries)
        return self.data['subsidiaries']

    def generate_addresses(self):
        """Generate address data for institutions and subsidiaries."""
        print("Generating addresses...")
        addresses = []
        
        # Generate addresses for institutions
        for institution in self.data['institutions'].to_dict('records'):
            num_addresses = random.randint(1, 3)
            for _ in range(num_addresses):
                effective_from = fake.date_between(start_date='-3y', end_date='-1y')
                last_verified = fake.date_between(start_date=effective_from, end_date='+1y')
                
                try:
                    address = Address(
                        address_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        address_type=random.choice(['headquarters', 'branch', 'operations']),
                        street_address=fake.street_address(),
                        city=fake.city(),
                        state=fake.state(),
                        postal_code=fake.postcode(),
                        country=fake.country(),
                        is_primary=random.choice([True, False]),
                        effective_from=effective_from.strftime('%Y-%m-%d'),
                        last_verified=last_verified.strftime('%Y-%m-%d'),
                        verification_source=random.choice(['document', 'site_visit', 'third_party']),
                        notes=fake.text(max_nb_chars=200) if random.random() > 0.5 else None
                    )
                    addresses.append(self._validate_and_convert_to_dict(address))
                except Exception as e:
                    print(f"Validation error for address: {e}")
                    continue
        
        # Generate addresses for subsidiaries if they exist
        if 'subsidiaries' in self.data and not self.data['subsidiaries'].empty:
            for subsidiary in self.data['subsidiaries'].to_dict('records'):
                num_addresses = random.randint(1, 2)
                for _ in range(num_addresses):
                    effective_from = fake.date_between(start_date='-3y', end_date='-1y')
                    last_verified = fake.date_between(start_date=effective_from, end_date='+1y')
                    
                    try:
                        address = Address(
                            address_id=str(uuid.uuid4()),
                            entity_id=subsidiary['subsidiary_id'],
                            entity_type='subsidiary',
                            address_type=random.choice(['headquarters', 'branch', 'operations']),
                            street_address=fake.street_address(),
                            city=fake.city(),
                            state=fake.state(),
                            postal_code=fake.postcode(),
                            country=fake.country(),
                            is_primary=random.choice([True, False]),
                            effective_from=effective_from.strftime('%Y-%m-%d'),
                            last_verified=last_verified.strftime('%Y-%m-%d'),
                            verification_source=random.choice(['document', 'site_visit', 'third_party']),
                            notes=fake.text(max_nb_chars=200) if random.random() > 0.5 else None
                        )
                        addresses.append(self._validate_and_convert_to_dict(address))
                    except Exception as e:
                        print(f"Validation error for address: {e}")
                        continue
        
        self.data['addresses'] = pd.DataFrame(addresses)
        return self.data['addresses']

    def generate_risk_assessments(self):
        """Generate risk assessment data."""
        print("Generating risk assessments...")
        risk_assessments = []
        
        # For institutions
        for institution in self.data['institutions'].to_dict('records'):
            num_assessments = random.randint(2, 5)
            for _ in range(num_assessments):
                assessment_date = fake.date_between(start_date='-2y', end_date='today')
                risk_score = random.randint(1, 100)
                assessor = fake.name()
                
                try:
                    assessment = RiskAssessment(
                        assessment_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        assessment_date=assessment_date.strftime('%Y-%m-%d'),
                        risk_rating=random.choice(['low', 'medium', 'high']),
                        risk_score=str(risk_score),
                        assessment_type=random.choice(['annual', 'ad_hoc', 'triggered']),
                        risk_factors={
                            'geographic_risk': random.randint(1, 5),
                            'product_risk': random.randint(1, 5),
                            'client_risk': random.randint(1, 5),
                            'transaction_risk': random.randint(1, 5)
                        },
                        conducted_by=assessor,
                        approved_by=fake.name(),
                        findings=fake.text(max_nb_chars=200),
                        next_review_date=(assessment_date + timedelta(days=365)).strftime('%Y-%m-%d'),
                        notes=fake.text(max_nb_chars=200) if random.random() > 0.5 else None
                    )
                    risk_assessments.append(self._validate_and_convert_to_dict(assessment))
                except Exception as e:
                    print(f"Error generating risk assessment: {e}")
                    continue
        
        # For subsidiaries
        if 'subsidiaries' in self.data and not self.data['subsidiaries'].empty:
            for subsidiary in self.data['subsidiaries'].to_dict('records'):
                num_assessments = random.randint(1, 3)
                for _ in range(num_assessments):
                    assessment_date = fake.date_between(start_date='-2y', end_date='today')
                    risk_score = random.randint(1, 100)
                    assessor = fake.name()
                    
                    try:
                        assessment = RiskAssessment(
                            assessment_id=str(uuid.uuid4()),
                            entity_id=subsidiary['subsidiary_id'],
                            entity_type='subsidiary',
                            assessment_date=assessment_date.strftime('%Y-%m-%d'),
                            risk_rating=random.choice(['low', 'medium', 'high']),
                            risk_score=str(risk_score),
                            assessment_type=random.choice(['annual', 'ad_hoc', 'triggered']),
                            risk_factors={
                                'geographic_risk': random.randint(1, 5),
                                'operational_risk': random.randint(1, 5),
                                'regulatory_risk': random.randint(1, 5),
                                'reputational_risk': random.randint(1, 5)
                            },
                            conducted_by=assessor,
                            approved_by=fake.name(),
                            findings=fake.text(max_nb_chars=200),
                            next_review_date=(assessment_date + timedelta(days=365)).strftime('%Y-%m-%d'),
                            notes=fake.text(max_nb_chars=200) if random.random() > 0.5 else None
                        )
                        risk_assessments.append(self._validate_and_convert_to_dict(assessment))
                    except Exception as e:
                        print(f"Error generating subsidiary risk assessment: {e}")
                        continue
        
        self.data['risk_assessments'] = pd.DataFrame(risk_assessments)
        return self.data['risk_assessments']

    def generate_beneficial_owners(self):
        beneficial_owners = []
        for institution in self.data['institutions'].to_dict('records'):
            num_owners = random.randint(1, 3)
            for _ in range(num_owners):
                dob = fake.date_of_birth(minimum_age=30, maximum_age=80)
                try:
                    owner = BeneficialOwner(
                        owner_id=str(uuid.uuid4()),
                        institution_id=institution['institution_id'],
                        full_name=fake.name(),
                        dob=dob.strftime('%Y-%m-%d'),
                        nationality=random.choice(self.countries),
                        ownership_percentage=round(random.uniform(10, 100), 2),
                        pep_status=random.choice(['none', 'domestic', 'foreign', 'international_org']),
                        identification_type=random.choice(['passport', 'national_id', 'drivers_license']),
                        identification_number=str(fake.uuid4()),
                        verification_date=fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
                        verification_source=random.choice(['document_verification', 'database_check', 'third_party_provider'])
                    )
                    beneficial_owners.append(self._validate_and_convert_to_dict(owner))
                except ValueError as e:
                    print(f"Validation error for beneficial owner: {e}")
                    continue

        self.data['beneficial_owners'] = pd.DataFrame(beneficial_owners)
        return self.data['beneficial_owners']

    def generate_authorized_persons(self):
        authorized_persons = []
        for institution in self.data['institutions'].to_dict('records'):
            num_persons = random.randint(2, 6)
            for _ in range(num_persons):
                auth_start = fake.date_between(start_date='-5y', end_date='today')
                try:
                    authorized_person = AuthorizedPerson(
                        auth_person_id=str(uuid.uuid4()),
                        institution_id=institution['institution_id'],
                        full_name=fake.name(),
                        title=random.choice(['Director', 'CFO', 'Treasurer', 'Controller', 'VP Finance']),
                        role=random.choice(['primary_contact', 'secondary_contact', 'authorized_signer']),
                        permissions=['view_accounts', 'initiate_payments', 'approve_transactions'],
                        authorization_start=auth_start.strftime('%Y-%m-%d'),
                        authorization_end=fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d') if random.random() > 0.8 else None,
                        status='active',
                        contact_details={
                            'email': fake.email(),
                            'phone': fake.phone_number(),
                            'office_location': fake.city()
                        },
                        verification_status=random.choice(['verified', 'pending', 'expired'])
                    )
                    authorized_persons.append(self._validate_and_convert_to_dict(authorized_person))
                except ValueError as e:
                    print(f"Validation error for authorized person: {e}")
                    continue
        
        self.data['authorized_persons'] = pd.DataFrame(authorized_persons)
        return self.data['authorized_persons']

    def generate_documents(self):
        documents = []
        for institution in self.data['institutions'].to_dict('records'):
            num_docs = random.randint(1, 5)
            for _ in range(num_docs):
                submission_date = fake.date_between(start_date='-2y', end_date='today')
                verification_date = fake.date_between(start_date=submission_date, end_date='+6m')
                expiry_date = fake.date_between(start_date=verification_date, end_date='+3y') if random.random() > 0.5 else None
                
                # Convert dates to string format
                submission_date_str = submission_date.strftime('%Y-%m-%d')
                verification_date_str = verification_date.strftime('%Y-%m-%d')
                expiry_date_str = expiry_date.strftime('%Y-%m-%d') if expiry_date else None
                
                try:
                    document = Document(
                        document_id=str(uuid.uuid4()),
                        institution_id=institution['institution_id'],
                        document_type=random.choice(['incorporation_certificate', 'license', 'regulatory_filing', 'tax_document', 'ownership_proof']),
                        document_number=fake.bothify(text='???###???###'),
                        issuing_authority=fake.company(),
                        issuing_country=random.choice(self.countries),
                        submission_date=submission_date_str,
                        verification_date=verification_date_str,
                        expiry_date=expiry_date_str,
                        document_status=random.choice(['active', 'expired', 'pending_review']),
                        verification_status=random.choice(['verified', 'pending', 'rejected']),
                        verification_method=random.choice(['manual', 'automated', 'third_party']),
                        file_reference=f"DOC_{fake.bothify(text='???###')}.pdf",
                        metadata={
                            'file_size': str(random.randint(1000000, 10000000)),
                            'file_type': 'application/pdf',
                            'page_count': str(random.randint(1, 50))
                        }
                    )
                    documents.append(self._validate_and_convert_to_dict(document))
                except Exception as e:
                    print(f"Error generating document: {e}")
                    continue
        
        self.data['documents'] = pd.DataFrame(documents)
        return self.data['documents']

    def generate_jurisdiction_presence(self):
        jurisdiction_presences = []
        for institution in self.data['institutions'].to_dict('records'):
            num_jurisdictions = random.randint(1, 3)
            for _ in range(num_jurisdictions):
                establishment_date = fake.date_between(start_date='-10y', end_date='today')
                establishment_date_str = establishment_date.strftime('%Y-%m-%d')
                
                try:
                    jurisdiction = JurisdictionPresence(
                        presence_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        jurisdiction=random.choice(self.countries),
                        presence_type=random.choice(['branch', 'subsidiary', 'representative_office']),
                        regulatory_status=random.choice(['registered', 'licensed', 'exempt']),
                        local_licenses=self._generate_licenses(institution['business_type']),
                        establishment_date=establishment_date_str,
                        local_regulator=self._get_regulator(institution['business_type']),
                        compliance_status=random.choice(['compliant', 'under_review', 'remediation_required']),
                        reporting_requirements={
                            'frequency': random.choice(['monthly', 'quarterly', 'annually']),
                            'reports': ','.join(['regulatory_returns', 'financial_statements', 'compliance_reports']),
                            'next_due_date': fake.date_between(start_date='today', end_date='+1y').strftime('%Y-%m-%d')
                        },
                        supervisory_authority=random.choice(['central_bank', 'financial_authority', 'securities_commission']),
                        material_entity_flag=random.choice([True, False])
                    )
                    jurisdiction_presences.append(self._validate_and_convert_to_dict(jurisdiction))
                except Exception as e:
                    print(f"Error generating jurisdiction presence: {e}")
                    continue
        
        self.data['jurisdiction_presences'] = pd.DataFrame(jurisdiction_presences)
        return self.data['jurisdiction_presences']

    def generate_accounts(self):
        accounts = []
        account_types = self._get_account_types()
        
        for institution in self.data['institutions'].to_dict('records'):
            num_accounts = random.randint(1, 5)
            business_type = institution['business_type']
            
            # If business_type not in account_types, use 'other'
            if business_type not in account_types:
                business_type = 'other'
                
            for _ in range(num_accounts):
                try:
                    account = Account(
                        account_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        account_type=random.choice(account_types[business_type]),
                        account_number=fake.bothify(text='??####???###'),
                        currency=random.choice(self.currencies),
                        status=random.choice(['active', 'dormant', 'closed']),
                        opening_date=fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
                        last_activity_date=fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                        balance=round(random.uniform(10000, 10000000), 2),
                        risk_rating=random.choice(['low', 'medium', 'high']),
                        purpose=random.choice(['operations', 'trading', 'settlement', 'investment']),
                        average_monthly_balance=round(random.uniform(100000, 10000000), 2),
                        custodian_bank=fake.company() if random.random() > 0.5 else None,
                        account_officer=fake.name()
                    )
                    accounts.append(self._validate_and_convert_to_dict(account))
                except Exception as e:
                    print(f"Error generating account: {e}")
                    continue
        
        self.data['accounts'] = pd.DataFrame(accounts)
        return self.data['accounts']

    def generate_subsidiary_relationships(self):
        if 'subsidiaries' not in self.data or self.data['subsidiaries'].empty:
            print("No subsidiaries found, skipping subsidiary relationship generation")
            self.data['subsidiary_relationships'] = pd.DataFrame()
            return self.data['subsidiary_relationships']
            
        subsidiary_relationships = []
        subsidiaries_list = self.data['subsidiaries']['subsidiary_id'].tolist()
        
        for subsidiary_id in subsidiaries_list:
            try:
                relationship = SubsidiaryRelationship(
                    relationship_id=str(uuid.uuid4()),
                    parent_id=random.choice(self.data['institutions']['institution_id'].tolist()),
                    subsidiary_id=subsidiary_id,
                    ownership_percentage=round(random.uniform(51, 100), 2),
                    relationship_type=random.choice(['direct', 'indirect']),
                    control_type=random.choice(['majority_ownership', 'operational_control', 'board_control']),
                    relationship_status='active',
                    relationship_start=fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
                    approval_status=random.choice(['approved', 'pending', 'under_review']),
                    risk_factors={
                        'operational_dependency': random.randint(1, 5),
                        'financial_exposure': random.randint(1, 5),
                        'reputational_risk': random.randint(1, 5)
                    }
                )
                subsidiary_relationships.append(self._validate_and_convert_to_dict(relationship))
            except Exception as e:
                print(f"Error generating subsidiary relationship: {e}")
                continue
        
        self.data['subsidiary_relationships'] = pd.DataFrame(subsidiary_relationships)
        return self.data['subsidiary_relationships']

    def generate_compliance_events(self):
        """Generate compliance events for institutions."""
        print("Generating compliance events...")
        compliance_events = []

        # Generate events for each institution
        for institution in self.data['institutions'].to_dict('records'):
            # Get institution's accounts
            institution_accounts = self.data['accounts'][
                self.data['accounts']['entity_id'] == institution['institution_id']
            ]
            
            if institution_accounts.empty:
                continue
                
            account_ids = institution_accounts['account_id'].tolist()

            # Generate onboarding event
            onboarding_date = datetime.strptime(institution['onboarding_date'], '%Y-%m-%d')
            try:
                onboarding_event = ComplianceEvent(
                    event_id=str(uuid.uuid4()),
                    entity_id=institution['institution_id'],
                    entity_type='institution',
                    event_date=institution['onboarding_date'],
                    event_type='onboarding',
                    event_description='Initial customer onboarding',
                    old_state='not_onboarded',
                    new_state='onboarded',
                    decision='approved',
                    decision_date=institution['onboarding_date'],
                    decision_maker=institution['relationship_manager'],
                    next_review_date=(onboarding_date + timedelta(days=365)).strftime('%Y-%m-%d'),
                    related_account_id=random.choice(account_ids),
                    notes='Initial onboarding completed successfully'
                )
                compliance_events.append(self._validate_and_convert_to_dict(onboarding_event))
            except Exception as e:
                print(f"Error generating onboarding event: {e}")
                continue

            # Generate risk rating change events
            last_risk_rating = institution['risk_rating']
            num_risk_changes = random.randint(1, 3)
            for _ in range(num_risk_changes):
                change_date = fake.date_between(start_date=onboarding_date, end_date='today')
                new_risk_rating = random.choice([r for r in ['low', 'medium', 'high'] if r != last_risk_rating])
                
                try:
                    risk_change_event = ComplianceEvent(
                        event_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        event_date=change_date.strftime('%Y-%m-%d'),
                        event_type='risk_rating_change',
                        event_description=f'Risk rating changed from {last_risk_rating} to {new_risk_rating}',
                        old_state=last_risk_rating,
                        new_state=new_risk_rating,
                        decision='approved',
                        decision_date=change_date.strftime('%Y-%m-%d'),
                        decision_maker=institution['relationship_manager'],
                        next_review_date=(change_date + timedelta(days=180)).strftime('%Y-%m-%d'),
                        related_account_id=random.choice(account_ids),
                        notes=f'Risk rating updated based on periodic review'
                    )
                    compliance_events.append(self._validate_and_convert_to_dict(risk_change_event))
                    last_risk_rating = new_risk_rating
                except Exception as e:
                    print(f"Error generating risk change event: {e}")
                    continue

            # Generate periodic review events
            num_reviews = random.randint(1, 2)
            last_review_date = onboarding_date
            for _ in range(num_reviews):
                review_date = fake.date_between(start_date=last_review_date, end_date='today')
                try:
                    review_event = ComplianceEvent(
                        event_id=str(uuid.uuid4()),
                        entity_id=institution['institution_id'],
                        entity_type='institution',
                        event_date=review_date.strftime('%Y-%m-%d'),
                        event_type='periodic_review',
                        event_description='Periodic compliance review',
                        old_state='under_review',
                        new_state='review_completed',
                        decision='completed',
                        decision_date=review_date.strftime('%Y-%m-%d'),
                        decision_maker=institution['relationship_manager'],
                        next_review_date=(review_date + timedelta(days=365)).strftime('%Y-%m-%d'),
                        related_account_id=random.choice(account_ids),
                        notes='Annual compliance review completed'
                    )
                    compliance_events.append(self._validate_and_convert_to_dict(review_event))
                    last_review_date = review_date
                except Exception as e:
                    print(f"Error generating review event: {e}")
                    continue

        self.data['compliance_events'] = pd.DataFrame(compliance_events)
        return self.data['compliance_events']

    def generate_all_data(self):
        print("Generating institutions...")
        self.generate_institution_data()
        
        print("Generating subsidiaries...")
        self.generate_subsidiaries()
        
        print("Generating addresses...")
        self.generate_addresses()
        
        print("Generating risk assessments...")
        self.generate_risk_assessments()
        
        print("Generating beneficial owners...")
        self.generate_beneficial_owners()
        
        print("Generating authorized persons...")
        self.generate_authorized_persons()
        
        print("Generating documents...")
        self.generate_documents()
        
        print("Generating jurisdiction presence...")
        self.generate_jurisdiction_presence()
        
        print("Generating accounts...")
        self.generate_accounts()
        
        print("Generating subsidiary relationships...")
        self.generate_subsidiary_relationships()
        
        print("Generating compliance events...")
        self.generate_compliance_events()
        
        print("Saving all data to CSV files...")
        self.save_to_csv()

    def save_to_csv(self):
        """Save all generated data to CSV files."""
        print("Saving all data to CSV files...")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Save each dataframe to a CSV file
        for name, df in self.data.items():
            if df is not None and not df.empty:
                output_path = os.path.join(self.output_dir, f"{name}.csv")
                df.to_csv(output_path, index=False)
                print(f"Saved {name}.csv with {len(df)} records")
            else:
                print(f"No data to save for {name}")
        
        print(f"Data generation complete. Files have been saved to {self.output_dir}")

    def _get_account_types(self):
        """Get mapping of business types to their possible activities"""
        return {
            'hedge_fund': ['trading', 'margin', 'custody', 'cash_management'],
            'bank': ['correspondent', 'nostro', 'vostro', 'treasury'],
            'broker_dealer': ['trading', 'settlement', 'custody', 'client_funds'],
            'insurance': ['premium', 'claims', 'investment', 'operational'],
            'asset_manager': ['investment', 'custody', 'client_funds', 'operational'],
            'pension_fund': ['investment', 'custody', 'benefit_payments', 'operational'],
            'other': ['operational', 'investment', 'cash_management']
        }

    def _get_business_activities(self):
        """Get mapping of business types to their possible activities"""
        return {
            'hedge_fund': [
                'Global Macro Trading',
                'Long/Short Equity',
                'Event Driven',
                'Quantitative Trading',
                'Multi-Strategy'
            ],
            'bank': [
                'Commercial Banking',
                'Investment Banking',
                'Retail Banking',
                'Private Banking',
                'Corporate Banking'
            ],
            'broker_dealer': [
                'Securities Trading',
                'Market Making',
                'Investment Advisory',
                'Securities Underwriting',
                'Prime Brokerage'
            ],
            'insurance': [
                'Life Insurance',
                'Property & Casualty',
                'Reinsurance',
                'Health Insurance',
                'Commercial Insurance'
            ],
            'asset_manager': [
                'Mutual Fund Management',
                'ETF Management',
                'Wealth Management',
                'Portfolio Management',
                'Investment Advisory'
            ],
            'pension_fund': [
                'Public Pension Management',
                'Private Pension Management',
                'Retirement Fund Administration',
                'Benefits Management',
                'Investment Management'
            ],
            'other': [
                'Financial Technology',
                'Payment Processing',
                'Credit Services',
                'Financial Advisory',
                'Investment Research'
            ]
        }

    def _get_business_activity(self, business_type: str) -> str:
        """Get a random business activity for the given business type"""
        activities = self._get_business_activities()
        return random.choice(activities.get(business_type, activities['other']))

    def _get_regulators(self):
        """Get mapping of business types to their possible regulators"""
        return {
            'hedge_fund': [
                'SEC',
                'CFTC',
                'NFA',
                'State Securities Regulators'
            ],
            'bank': [
                'Federal Reserve',
                'OCC',
                'FDIC',
                'State Banking Regulators'
            ],
            'broker_dealer': [
                'SEC',
                'FINRA',
                'State Securities Regulators',
                'Federal Reserve'
            ],
            'insurance': [
                'State Insurance Regulators',
                'NAIC',
                'Federal Insurance Office',
                'State Insurance Commissioners'
            ],
            'asset_manager': [
                'SEC',
                'State Securities Regulators',
                'FINRA',
                'CFTC'
            ],
            'pension_fund': [
                'Department of Labor',
                'PBGC',
                'IRS',
                'State Pension Regulators'
            ],
            'other': [
                'SEC',
                'State Regulators',
                'Federal Reserve',
                'CFPB'
            ]
        }

    def _get_regulator(self, business_type: str) -> str:
        """Get a random regulator for the given business type"""
        regulators = self._get_regulators()
        return random.choice(regulators.get(business_type, regulators['other']))

    def _generate_licenses(self, business_type: str) -> List[str]:
        """Generate appropriate licenses for the given business type"""
        licenses = {
            'bank': [
                'Federal Banking Charter',
                'State Banking License',
                'International Banking License',
                'Money Transfer License'
            ],
            'broker_dealer': [
                'Broker-Dealer Registration',
                'Municipal Securities License',
                'Investment Banking License',
                'Securities Trading License'
            ],
            'asset_manager': [
                'Investment Advisor Registration',
                'Portfolio Management License',
                'Mutual Fund License',
                'Investment Company License'
            ],
            'insurance': [
                'Property & Casualty License',
                'Life Insurance License',
                'Reinsurance License',
                'Insurance Broker License'
            ],
            'payment_provider': [
                'Money Services Business License',
                'Payment Institution License',
                'E-Money License',
                'Money Transfer License'
            ],
            'crypto_exchange': [
                'Virtual Asset Service Provider License',
                'Money Services Business License',
                'Digital Asset Exchange License'
            ],
            'hedge_fund': [
                'Investment Advisor Registration',
                'Commodity Pool Operator License',
                'Alternative Investment Fund License'
            ],
            'other': [
                'Financial Services License',
                'Business Operation License',
                'General Trading License'
            ]
        }
        
        available_licenses = licenses.get(business_type, licenses['other'])
        num_licenses = random.randint(1, len(available_licenses))
        return random.sample(available_licenses, num_licenses)

    def _generate_industry_codes(self) -> Dict[str, str]:
        return {
            'NAICS': fake.numerify(text='######'),
            'SIC': fake.numerify(text='####'),
            'GICS': fake.numerify(text='##########')
        }

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate test data for AML transaction monitoring system')
    parser.add_argument('--num-institutions', type=int, default=100, help='Number of institutions to generate')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--output-dir', type=str, default='./data', help='Output directory for CSV files')
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize generator
    generator = TestDataGenerator(num_institutions=args.num_institutions, seed=args.seed, output_dir=args.output_dir)
    
    # Generate all data
    generator.generate_all_data()
    
    print(f"Data generation complete. Files have been saved to {args.output_dir}")
