import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from faker import Faker
from enum import Enum

from .models import (
    Institution, Subsidiary, ComplianceEvent, RiskAssessment,
    BeneficialOwner, AuthorizedPerson, Address, Document,
    JurisdictionPresence, Account, RiskRating, BusinessType,
    ComplianceEventType, OperationalStatus, Customer
)

fake = Faker()

class TestDataGenerator:
    # Business constants
    BUSINESS_TYPES = [bt.value for bt in BusinessType]
    OPERATIONAL_STATUSES = [os.value for os in OperationalStatus]
    COUNTRIES = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'SG', 'HK', 'CH']
    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'SGD', 'HKD']
    RISK_RATINGS = [rating.value for rating in RiskRating]

    def __init__(self):
        """Initialize the test data generator."""
        self.data = {}
        self.fake = Faker()

    def _validate_and_convert_to_dict(self, model_instance):
        """Helper method to validate and convert a model instance to a dictionary."""
        try:
            return model_instance.model_dump()
        except Exception as e:
            print("Error converting model to dictionary: {}".format(e))
            return None

    def generate_institution_data(self) -> dict:
        """Generate data for a single institution."""
        business_type = random.choice(self.BUSINESS_TYPES)
        incorporation_date = fake.date_between(start_date='-20y', end_date='-1y')
        onboarding_date = fake.date_between(start_date=incorporation_date, end_date='today')
        last_review_date = fake.date_between(start_date=onboarding_date, end_date='today')
        next_review_date = fake.date_between(start_date='today', end_date='+1y')
        
        try:
            institution = Institution(
                institution_id=str(uuid.uuid4()),
                legal_name=fake.company(),
                business_type=business_type,
                incorporation_country=random.choice(self.COUNTRIES),
                incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                onboarding_date=onboarding_date.strftime('%Y-%m-%d'),
                risk_rating=random.choices(
                    [RiskRating.LOW.value, RiskRating.MEDIUM.value, RiskRating.HIGH.value],
                    weights=[60, 30, 10]
                )[0],
                operational_status=random.choice(self.OPERATIONAL_STATUSES),
                primary_currency=random.choice(self.CURRENCIES),
                regulatory_status=random.choice(['fully_regulated', 'limited_permission', 'exempt']),
                primary_business_activity=self._get_business_activity(business_type),
                primary_regulator=fake.company(),
                licenses=[f"{business_type}_license_{i}" for i in range(random.randint(1, 3))],
                aml_program_status=random.choice(['compliant', 'under_review', 'remediation_required']),
                kyc_refresh_date=last_review_date.strftime('%Y-%m-%d'),
                last_audit_date=last_review_date.strftime('%Y-%m-%d'),
                next_audit_date=next_review_date.strftime('%Y-%m-%d'),
                relationship_manager=fake.name(),
                relationship_status=random.choice(['active', 'inactive', 'terminated']),
                swift_code=fake.bothify(text='????##??###'),
                lei_code=fake.bothify(text='#######?????#####'),
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
            return self._validate_and_convert_to_dict(institution)
        except ValueError as e:
            print(f"Validation error for institution: {e}")
            return None

    def generate_subsidiaries(self, institution: dict, num_subsidiaries: int) -> List[dict]:
        """Generate subsidiaries for an institution."""
        subsidiaries = []
        for _ in range(num_subsidiaries):
            try:
                # Generate base dates
                incorporation_date = fake.date_between(start_date='-10y', end_date='-1y')
                acquisition_date = fake.date_between(start_date=incorporation_date, end_date='today')
                
                # Generate risk classification
                risk_classification = random.choices(
                    [RiskRating.LOW.value, RiskRating.MEDIUM.value, RiskRating.HIGH.value],
                    weights=[50, 35, 15]
                )[0]

                # Base subsidiary data
                subsidiary_data = {
                    'subsidiary_id': str(uuid.uuid4()),
                    'parent_institution_id': institution['institution_id'],
                    'legal_name': fake.company(),
                    'tax_id': fake.numerify(text='##-#######'),
                    'incorporation_country': random.choice(self.COUNTRIES),
                    'incorporation_date': incorporation_date.strftime('%Y-%m-%d'),
                    'acquisition_date': acquisition_date.strftime('%Y-%m-%d'),
                    'business_type': random.choice(self.BUSINESS_TYPES),
                    'operational_status': random.choice(self.OPERATIONAL_STATUSES),
                    'parent_ownership_percentage': round(random.uniform(51, 100), 2),
                    'consolidation_status': random.choice(['full', 'proportional', 'equity_method']),
                    'capital_investment': round(random.uniform(1000000, 10000000), 2),
                    'functional_currency': random.choice(self.CURRENCIES),
                    'material_subsidiary': random.choice([True, False]),
                    'risk_classification': risk_classification,
                    'regulatory_status': random.choice(['fully_regulated', 'limited_permission', 'exempt']),
                    'local_licenses': [f"license_{i}" for i in range(random.randint(1, 3))],
                    'integration_status': random.choice(['fully_integrated', 'partially_integrated', 'standalone']),
                    'financial_metrics': {
                        'total_assets': round(random.uniform(10000000, 100000000), 2),
                        'annual_revenue': round(random.uniform(1000000, 10000000), 2),
                        'net_income': round(random.uniform(100000, 1000000), 2)
                    },
                    'reporting_frequency': random.choice(['monthly', 'quarterly', 'semi_annual']),
                    'requires_local_audit': random.choice([True, False]),
                    'corporate_governance_model': random.choice(['board_of_directors', 'supervisory_board', 'mixed']),
                    'is_regulated': random.choice([True, False])
                }
                
                # Randomly decide if this subsidiary is a customer
                is_customer = random.choice([True, False])
                if is_customer:
                    customer_onboarding_date = fake.date_between(
                        start_date=acquisition_date,
                        end_date='today'
                    ).strftime('%Y-%m-%d')
                    
                    customer_data = {
                        'is_customer': True,
                        'customer_id': str(uuid.uuid4()),
                        'customer_onboarding_date': customer_onboarding_date,
                        'customer_risk_rating': random.choices(
                            [RiskRating.LOW.value, RiskRating.MEDIUM.value, RiskRating.HIGH.value],
                            weights=[60, 30, 10]
                        )[0],
                        'customer_status': random.choice(['active', 'inactive', 'suspended'])
                    }
                else:
                    customer_data = {
                        'is_customer': False,
                        'customer_id': None,
                        'customer_onboarding_date': None,
                        'customer_risk_rating': None,
                        'customer_status': None
                    }
                
                # Create the complete subsidiary data
                subsidiary_data.update(customer_data)
                
                # Create and validate the subsidiary
                subsidiary = Subsidiary(**subsidiary_data)
                subsidiaries.append(self._validate_and_convert_to_dict(subsidiary))
            except Exception as e:
                print(f"Validation error for subsidiary: {str(e)}")
                continue
                
        return subsidiaries

    def generate_addresses(self, entity_id: str, entity_type: str, num_addresses: int) -> List[dict]:
        """Generate addresses for an entity."""
        addresses = []
        for i in range(num_addresses):
            try:
                address = Address(
                    address_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    address_type=random.choice(['registered', 'business', 'mailing']),
                    address_line1=fake.street_address(),
                    address_line2=fake.secondary_address() if random.random() > 0.5 else None,
                    city=fake.city(),
                    state_province=fake.state(),
                    postal_code=fake.postcode(),
                    country=random.choice(self.COUNTRIES),
                    status='active',
                    effective_from=fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
                    effective_to=fake.date_between(start_date='today', end_date='+2y').strftime('%Y-%m-%d'),
                    primary_address=i == 0,  # First address is primary
                    validation_status=random.choice(['verified', 'pending', 'failed']),
                    last_verified=fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                    geo_coordinates={'latitude': float(fake.latitude()), 'longitude': float(fake.longitude())},
                    timezone=fake.timezone()
                )
                addresses.append(self._validate_and_convert_to_dict(address))
            except Exception as e:
                print(f"Validation error for address: {e}")
        return addresses

    def generate_risk_assessments(self, entity_id: str, entity_type: str, num_assessments: int) -> List[dict]:
        """Generate risk assessments for an entity."""
        assessments = []
        for _ in range(num_assessments):
            assessment_date = fake.date_between(start_date='-1y', end_date='today')
            try:
                assessment = RiskAssessment(
                    assessment_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    assessment_date=assessment_date.strftime('%Y-%m-%d'),
                    risk_rating=random.choices(
                        [RiskRating.LOW.value, RiskRating.MEDIUM.value, RiskRating.HIGH.value],
                        weights=[60, 30, 10]
                    )[0],
                    risk_score=str(random.randint(1, 100)),
                    assessment_type=random.choice(['initial', 'periodic', 'triggered']),
                    risk_factors={
                        'geographic': random.randint(1, 5),
                        'product': random.randint(1, 5),
                        'client': random.randint(1, 5),
                        'transaction': random.randint(1, 5)
                    },
                    conducted_by=fake.name(),
                    approved_by=fake.name(),
                    findings=fake.text(max_nb_chars=200),
                    assessor=fake.name(),
                    next_review_date=fake.date_between(start_date=assessment_date, end_date='+1y').strftime('%Y-%m-%d'),
                    notes=fake.text(max_nb_chars=100) if random.random() > 0.5 else None
                )
                assessments.append(self._validate_and_convert_to_dict(assessment))
            except Exception as e:
                print(f"Error generating risk assessment: {e}")
        return assessments

    def generate_beneficial_owners(self, entity_id: str, entity_type: str, num_owners: int):
        beneficial_owners = []
        for _ in range(num_owners):
            dob = fake.date_of_birth(minimum_age=30, maximum_age=80)
            try:
                owner = BeneficialOwner(
                    owner_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    name=fake.name(),
                    nationality=random.choice(self.COUNTRIES),
                    country_of_residence=random.choice(self.COUNTRIES),
                    ownership_percentage=round(random.uniform(10, 100), 2),
                    dob=dob.strftime('%Y-%m-%d'),
                    verification_date=fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
                    pep_status=random.choice([True, False]),
                    sanctions_status=random.choice([True, False]),
                    adverse_media_status=random.choice([True, False]),
                    verification_source=random.choice(['document_verification', 'database_check', 'third_party_provider']),
                    notes=fake.text(max_nb_chars=200) if random.random() > 0.5 else None
                )
                beneficial_owners.append(self._validate_and_convert_to_dict(owner))
            except ValueError as e:
                print(f"Validation error for beneficial owner: {e}")
                continue

        return beneficial_owners

    def generate_authorized_persons(self, entity_id: str, entity_type: str, num_persons: int):
        authorized_persons = []
        for _ in range(num_persons):
            auth_start = fake.date_between(start_date='-5y', end_date='today')
            try:
                authorized_person = AuthorizedPerson(
                    person_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    name=fake.name(),
                    title=random.choice(['Director', 'CFO', 'Treasurer', 'Controller', 'VP Finance']),
                    authorization_level=random.choice(['primary', 'secondary', 'tertiary']),
                    authorization_type=random.choice(['full', 'limited', 'view_only']),
                    authorization_start=auth_start.strftime('%Y-%m-%d'),
                    authorization_end=fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d') if random.random() > 0.8 else None,
                    contact_info={
                        'email': fake.email(),
                        'phone': fake.phone_number(),
                        'office_location': fake.city()
                    },
                    is_active=True,
                    last_verification_date=fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
                )
                authorized_persons.append(self._validate_and_convert_to_dict(authorized_person))
            except ValueError as e:
                print(f"Validation error for authorized person: {e}")
                continue
        
        return authorized_persons

    def generate_documents(self, entity_id: str, entity_type: str, num_documents: int) -> List[dict]:
        """Generate documents for an entity."""
        documents = []
        for _ in range(num_documents):
            try:
                issue_date = self.fake.date_between(start_date='-2y', end_date='today')
                expiry_date = self.fake.date_between(start_date=issue_date, end_date='+5y')
                
                document = Document(
                    document_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    document_type=random.choice(['passport', 'incorporation_certificate', 'license', 'tax_document', 'regulatory_filing']),
                    document_number=self.fake.bothify(text='???###???###'),
                    issuing_authority=self.fake.company(),
                    issuing_country=random.choice(self.COUNTRIES),
                    issue_date=issue_date.strftime('%Y-%m-%d'),
                    expiry_date=expiry_date.strftime('%Y-%m-%d'),
                    verification_status=random.choice(['verified', 'pending', 'rejected']),
                    verification_date=self.fake.date_between(start_date=issue_date, end_date='today').strftime('%Y-%m-%d'),
                    document_category=random.choice(['identity', 'financial', 'regulatory', 'legal']),
                    notes=self.fake.text(max_nb_chars=100) if random.random() > 0.5 else None
                )
                validated_document = self._validate_and_convert_to_dict(document)
                if validated_document is not None:
                    documents.append(validated_document)
            except Exception as e:
                print(f"Error generating document: {e}")
                continue
        return documents

    def generate_jurisdiction_presences(self, entity_id: str, entity_type: str, num_presences: int):
        jurisdiction_presences = []
        for _ in range(num_presences):
            try:
                presence = JurisdictionPresence(
                    presence_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    jurisdiction=random.choice(self.COUNTRIES),
                    presence_type=random.choice(['branch', 'subsidiary', 'representative_office']),
                    regulatory_status=random.choice(['fully_regulated', 'limited_permission', 'exempt']),
                    local_licenses=[f"license_{i}" for i in range(random.randint(1, 3))],
                    establishment_date=fake.date_between(start_date='-10y', end_date='-1y').strftime('%Y-%m-%d'),
                    local_regulator=fake.company(),
                    compliance_status=random.choice(['compliant', 'under_review', 'non_compliant']),
                    reporting_requirements={
                        'frequency': random.choice(['monthly', 'quarterly', 'annually']),
                        'reports': [f"report_{i}" for i in range(random.randint(1, 5))]
                    },
                    supervisory_authority=fake.company(),
                    material_entity_flag=random.choice([True, False])
                )
                jurisdiction_presences.append(self._validate_and_convert_to_dict(presence))
            except ValueError as e:
                print(f"Validation error for jurisdiction presence: {e}")
                continue
        
        return jurisdiction_presences

    def generate_accounts(self, entity_id: str, entity_type: str, num_accounts: int) -> List[dict]:
        """Generate accounts for an entity."""
        accounts = []
        for _ in range(num_accounts):
            try:
                opening_date = fake.date_between(start_date='-2y', end_date='today')
                account = Account(
                    account_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    account_type=random.choice(['current', 'savings', 'investment', 'loan']),
                    account_number=fake.bothify(text='#########'),
                    currency=random.choice(self.CURRENCIES),
                    status=random.choice(['active', 'dormant', 'closed']),
                    opening_date=opening_date.strftime('%Y-%m-%d'),
                    last_activity_date=fake.date_between(start_date=opening_date, end_date='today').strftime('%Y-%m-%d'),
                    balance=round(random.uniform(1000, 1000000), 2),
                    risk_rating=random.choices(
                        [RiskRating.LOW.value, RiskRating.MEDIUM.value, RiskRating.HIGH.value],
                        weights=[60, 30, 10]
                    )[0],
                    purpose=random.choice(['business', 'investment', 'operational', 'settlement']),
                    average_monthly_balance=round(random.uniform(1000, 1000000), 2),
                    custodian_bank=fake.company() if random.random() > 0.5 else None,
                    account_officer=fake.name() if random.random() > 0.5 else None
                )
                accounts.append(self._validate_and_convert_to_dict(account))
            except Exception as e:
                print(f"Error generating account: {e}")
        return accounts

    def generate_compliance_events(self, entity_id: str, entity_type: str, onboarding_date: Optional[str], num_events: int) -> List[dict]:
        """Generate compliance events for an entity."""
        events = []
        if onboarding_date is None:
            onboarding_date = fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            
        onboarding_datetime = datetime.strptime(onboarding_date, '%Y-%m-%d')
        
        for _ in range(num_events):
            try:
                event_date = fake.date_between(start_date=onboarding_datetime, end_date='today')
                event = ComplianceEvent(
                    event_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    event_date=event_date.strftime('%Y-%m-%d'),
                    event_type=random.choice([e.value for e in ComplianceEventType]),
                    event_description=fake.text(max_nb_chars=200),
                    old_state=random.choice(['pending', 'in_progress', 'completed']) if random.random() > 0.5 else None,
                    new_state=random.choice(['pending', 'in_progress', 'completed']),
                    decision=random.choice(['approved', 'rejected', 'pending_review']) if random.random() > 0.5 else None,
                    decision_date=fake.date_between(start_date=event_date, end_date='today').strftime('%Y-%m-%d') if random.random() > 0.5 else None,
                    decision_maker=fake.name() if random.random() > 0.5 else None,
                    next_review_date=fake.date_between(start_date='today', end_date='+1y').strftime('%Y-%m-%d') if random.random() > 0.5 else None,
                    related_account_id=str(uuid.uuid4()),
                    notes=fake.text(max_nb_chars=100) if random.random() > 0.5 else None
                )
                events.append(self._validate_and_convert_to_dict(event))
            except Exception as e:
                print(f"Error generating compliance event: {e}")
        return events

    def _get_business_activity(self, business_type: str) -> str:
        """Helper method to get primary business activity based on business type."""
        activities = {
            'bank': 'Commercial Banking',
            'insurance': 'Insurance Underwriting',
            'broker_dealer': 'Securities Trading',
            'asset_manager': 'Asset Management',
            'hedge_fund': 'Alternative Investments',
            'pension_fund': 'Pension Fund Management',
            'other': 'Financial Services'
        }
        return activities.get(business_type.lower(), 'Financial Services')

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

    def _generate_industry_codes(self) -> List[str]:
        """Generate industry classification codes."""
        return [
            f"SIC:{fake.numerify(text='####')}",
            f"NAICS:{fake.numerify(text='######')}",
            f"GICS:{fake.numerify(text='##########')}"
        ]

    def generate_all_data(self, num_institutions: int = 5) -> Dict[str, pd.DataFrame]:
        """Generate all test data."""
        print(f"Generating data for {num_institutions} institutions...")
        
        # Generate institutions
        institutions = []
        for _ in range(num_institutions):
            institution = self.generate_institution_data()
            if institution is not None:
                institutions.append(institution)
        
        if not institutions:
            raise ValueError("Failed to generate any valid institutions")
            
        self.data['institutions'] = pd.DataFrame(institutions)
        print(f"Generated {len(institutions)} institutions")

        # Generate subsidiaries (2-5 per institution)
        subsidiaries = []
        for institution in institutions:
            num_subsidiaries = random.randint(2, 5)
            new_subsidiaries = self.generate_subsidiaries(institution, num_subsidiaries)
            if new_subsidiaries:
                subsidiaries.extend(new_subsidiaries)
                print(f"Generated {len(new_subsidiaries)} subsidiaries for institution {institution['institution_id']}")
        
        self.data['subsidiaries'] = pd.DataFrame(subsidiaries if subsidiaries else [])
        print(f"Total subsidiaries generated: {len(subsidiaries)}")

        # Create a list of all entities (institutions and subsidiaries)
        entities = [(inst['institution_id'], 'institution') for inst in institutions]
        if not self.data['subsidiaries'].empty:
            entities.extend([
                (row['subsidiary_id'], 'subsidiary') 
                for _, row in self.data['subsidiaries'].iterrows()
            ])
        
        print(f"Processing {len(entities)} total entities")

        # Initialize lists for related data
        compliance_events = []
        risk_assessments = []
        beneficial_owners = []
        authorized_persons = []
        addresses = []
        documents = []
        jurisdiction_presences = []
        accounts = []

        # Generate related data for each entity
        for entity_id, entity_type in entities:
            try:
                # Get onboarding date
                onboarding_date = None
                if entity_type == 'institution':
                    inst = next(inst for inst in institutions if inst['institution_id'] == entity_id)
                    onboarding_date = inst['onboarding_date']
                else:
                    sub = self.data['subsidiaries'][
                        self.data['subsidiaries']['subsidiary_id'] == entity_id
                    ].iloc[0]
                    onboarding_date = sub.get('customer_onboarding_date')
                
                # Generate related data
                new_events = self.generate_compliance_events(
                    entity_id, entity_type, onboarding_date, random.randint(1, 3)
                )
                compliance_events.extend(new_events)
                
                new_assessments = self.generate_risk_assessments(
                    entity_id, entity_type, random.randint(1, 2)
                )
                risk_assessments.extend(new_assessments)
                
                new_owners = self.generate_beneficial_owners(
                    entity_id, entity_type, random.randint(1, 3)
                )
                beneficial_owners.extend(new_owners)
                
                new_persons = self.generate_authorized_persons(
                    entity_id, entity_type, random.randint(1, 3)
                )
                authorized_persons.extend(new_persons)
                
                new_addresses = self.generate_addresses(
                    entity_id, entity_type, random.randint(1, 2)
                )
                addresses.extend(new_addresses)
                
                new_documents = self.generate_documents(
                    entity_id, entity_type, random.randint(2, 4)
                )
                documents.extend(new_documents)
                
                new_presences = self.generate_jurisdiction_presences(
                    entity_id, entity_type, random.randint(1, 3)
                )
                jurisdiction_presences.extend(new_presences)
                
                new_accounts = self.generate_accounts(
                    entity_id, entity_type, random.randint(1, 3)
                )
                accounts.extend(new_accounts)
                
                print(f"Generated related data for {entity_type} {entity_id}")
                
            except Exception as e:
                print(f"Error generating data for {entity_type} {entity_id}: {e}")
                continue

        # Convert lists to DataFrames with error handling
        def safe_to_dataframe(data_list, name):
            if not data_list:
                print(f"Warning: No {name} data generated")
                return pd.DataFrame()
            return pd.DataFrame(data_list)

        self.data.update({
            'compliance_events': safe_to_dataframe(compliance_events, 'compliance events'),
            'risk_assessments': safe_to_dataframe(risk_assessments, 'risk assessments'),
            'beneficial_owners': safe_to_dataframe(beneficial_owners, 'beneficial owners'),
            'authorized_persons': safe_to_dataframe(authorized_persons, 'authorized persons'),
            'addresses': safe_to_dataframe(addresses, 'addresses'),
            'documents': safe_to_dataframe(documents, 'documents'),
            'jurisdiction_presences': safe_to_dataframe(jurisdiction_presences, 'jurisdiction presences'),
            'accounts': safe_to_dataframe(accounts, 'accounts')
        })

        print("Data generation completed")
        return self.data

    def save_to_csv(self, output_dir: str):
        """Save all generated data to CSV files."""
        os.makedirs(output_dir, exist_ok=True)
        for name, df in self.data.items():
            if not df.empty:
                output_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(output_path, index=False)
                print(f"Saved {name} to {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    parser.add_argument('--num-institutions', type=int, default=5, help='Number of institutions to generate')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--output-dir', type=str, default='./data', help='Output directory for CSV files')
    parser.add_argument('--save-to-db', action='store_true', help='Save data to databases')
    
    args = parser.parse_args()
    
    # Set random seed for reproducibility
    random.seed(args.seed)
    fake.seed_instance(args.seed)
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate data
    generator = TestDataGenerator()
    data = generator.generate_all_data(args.num_institutions)
    
    # Save to CSV files
    print("Saving all data to CSV files...")
    generator.save_to_csv(args.output_dir)
    
    # Save to databases if requested
    if args.save_to_db:
        try:
            from db_handlers import DatabaseManager
            print("Saving data to databases...")
            db_manager = DatabaseManager()
            db_manager.save_data(generator.data)
            db_manager.close()
            print("Data successfully saved to databases")
        except Exception as e:
            print(f"Error saving to databases: {e}")
    
    print(f"Data generation complete. Files have been saved to {args.output_dir}")

if __name__ == '__main__':
    main()
