import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from faker import Faker
from enum import Enum
import asyncio
import argparse
from .models import (
    Institution, Subsidiary, ComplianceEvent, RiskAssessment,
    BeneficialOwner, AuthorizedPerson, Address, Document,
    JurisdictionPresence, Account, RiskRating, BusinessType,
    ComplianceEventType, OperationalStatus, Customer, Transaction, TransactionType, TransactionStatus
)
from .transaction_generator import TransactionGenerator

fake = Faker()

class TestDataGenerator:
    # Business constants
    BUSINESS_TYPES = [bt.value for bt in BusinessType]
    OPERATIONAL_STATUSES = [os.value for os in OperationalStatus]
    # Mix of low-risk and high-risk countries
    COUNTRIES = [
        # Low/Medium Risk Countries
        'US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'SG', 'HK', 'CH',
        # High Risk Countries - Sanctions
        'IR', 'KP', 'SY', 'CU', 'VE',
        # High Risk Countries - AML Concerns
        'AF', 'MM', 'LA', 'KH', 'YE',
        # High Risk Countries - Tax Havens
        'KY', 'VG', 'TC', 'BZ', 'PA'
    ]
    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'SGD', 'HKD']
    RISK_RATINGS = [rating.value for rating in RiskRating]

    def __init__(self):
        """Initialize the test data generator."""
        self.data = {}
        self.fake = Faker()
        self.transaction_generator = TransactionGenerator()

    def _validate_and_convert_to_dict(self, model_instance):
        """Helper method to validate and convert a model instance to a dictionary."""
        try:
            return model_instance.model_dump()
        except Exception as e:
            print("Error converting model to dictionary: {}".format(e))
            return None

    def generate_institution_data(self) -> dict:
        """Generate data for a single institution."""
        fake = Faker()
        
        onboarding_date = fake.date_between(start_date='-5y', end_date='today')
        
        institution = {
            'entity_id': str(uuid.uuid4()),
            'legal_name': fake.company(),
            'business_type': random.choice(['Bank', 'Credit Union', 'Investment Firm']),
            'incorporation_country': fake.country_code(),
            'onboarding_date': datetime.strftime(onboarding_date, '%Y-%m-%d'),
            'risk_rating': random.randint(1, 100)
        }
        
        return institution

    def generate_institutions(self, num_institutions: int) -> List[dict]:
        """Generate data for multiple institutions."""
        institutions = []
        for _ in range(num_institutions):
            institution = self.generate_institution_data()
            if institution:
                institutions.append(institution)
        
        if not institutions:
            raise ValueError("Failed to generate any valid institutions")
        
        print(f"Generated {len(institutions)} institutions")
        return institutions

    def generate_subsidiaries(self, institutions: List[dict]) -> List[dict]:
        """Generate subsidiaries for institutions."""
        subsidiaries = []
        for institution in institutions:
            num_subsidiaries = random.randint(2, 5)
            for _ in range(num_subsidiaries):
                subsidiary = {
                    'entity_id': str(uuid.uuid4()),
                    'parent_entity_id': institution['entity_id'],
                    'legal_name': f"{institution['legal_name']} {Faker().company_suffix()}",
                    'business_type': random.choice(['Subsidiary Bank', 'Trading Desk', 'Investment Vehicle']),
                    'incorporation_country': Faker().country_code(),
                    'onboarding_date': institution['onboarding_date'],
                    'parent_ownership_percentage': random.randint(51, 100)
                }
                subsidiaries.append(subsidiary)
            print(f"Generated {num_subsidiaries} subsidiaries for institution {institution['entity_id']}")
        
        print(f"Total subsidiaries generated: {len(subsidiaries)}")
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

    def generate_accounts(self, entity_id: str, entity_type: str, onboarding_date: str = None) -> List[dict]:
        """Generate accounts for an entity."""
        accounts = []
        num_accounts = random.randint(1, 3)
        
        if onboarding_date:
            onboarding_dt = datetime.strptime(onboarding_date, '%Y-%m-%d')
        else:
            onboarding_dt = datetime.now() - timedelta(days=random.randint(30, 730))
            
        for _ in range(num_accounts):
            account_type = random.choice(['current', 'savings', 'investment', 'loan'])
            try:
                account = {
                    'account_id': str(uuid.uuid4()),
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'account_type': account_type,
                    'account_number': fake.numerify(text='#########'),
                    'currency': random.choice(self.CURRENCIES),
                    'status': 'active',
                    'opening_date': datetime.strftime(onboarding_dt + timedelta(days=random.randint(0, 30)), '%Y-%m-%d'),
                    'last_activity_date': datetime.strftime(datetime.now() - timedelta(days=random.randint(0, 30)), '%Y-%m-%d'),
                    'balance': round(random.uniform(1000, 1000000), 2)
                }
                accounts.append(account)
            except Exception as e:
                print(f"Error generating account: {e}")
                continue
        
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

    async def generate_all_data(self, num_institutions: int = 5) -> Dict[str, pd.DataFrame]:
        """Generate all test data."""
        print(f"Generating data for {num_institutions} institutions...")
        
        # Generate base entities
        institutions = self.generate_institutions(num_institutions)
        subsidiaries = self.generate_subsidiaries(institutions)
        
        # Create entity list for related data generation
        entities = [(inst['entity_id'], 'institution') for inst in institutions]
        entities.extend([(sub['entity_id'], 'subsidiary') for sub in subsidiaries])
        
        # Initialize data containers
        beneficial_owners = []
        authorized_persons = []
        addresses = []
        documents = []
        jurisdiction_presences = []
        accounts = []
        transactions = []

        # Generate related data for each entity
        for entity_id, entity_type in entities:
            try:
                # Find the entity data
                if entity_type == 'institution':
                    entity_data = next(inst for inst in institutions if inst['entity_id'] == entity_id)
                else:
                    entity_data = next(sub for sub in subsidiaries if sub['entity_id'] == entity_id)
                
                # Generate accounts for this entity
                new_accounts = self.generate_accounts(
                    entity_id, entity_type,
                    onboarding_date=entity_data.get('onboarding_date', None)
                )
                accounts.extend(new_accounts)
                
                print(f"Generated related data for {entity_type} {entity_id}")
                
            except Exception as e:
                print(f"Error generating data for {entity_type} {entity_id}: {e}")
                continue
        
        # Generate transactions asynchronously
        if accounts:
            print("Generating transactions...")
            # Set date range to ensure transactions are within 2 years
            end_date = datetime.now()
            start_date = end_date - timedelta(days=730)  # 2 years ago
            
            transactions = await self.transaction_generator.generate_transactions_for_accounts(
                pd.DataFrame(accounts),
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            print(f"Generated {len(transactions)} transactions")
        
        # Create DataFrames
        return self._create_dataframes({
            'institutions': institutions,
            'subsidiaries': subsidiaries,
            'beneficial_owners': beneficial_owners,
            'authorized_persons': authorized_persons,
            'addresses': addresses,
            'documents': documents,
            'jurisdiction_presences': jurisdiction_presences,
            'accounts': accounts,
            'transactions': transactions
        })

    def _create_dataframes(self, data_dict: Dict[str, List[dict]]) -> Dict[str, pd.DataFrame]:
        """Convert dictionaries to DataFrames."""
        dataframes = {}
        for key, data in data_dict.items():
            if not isinstance(data, pd.DataFrame) and (data is None or len(data) == 0):
                print(f"Warning: No {key} data generated")
                if key == 'transactions':
                    # Create empty transactions DataFrame with required columns
                    dataframes[key] = pd.DataFrame(columns=[
                        'transaction_id', 'transaction_type', 'amount', 'currency',
                        'account_id', 'is_debit', 'transaction_date', 'screening_alert',
                        'risk_score', 'alert_details'
                    ])
                else:
                    dataframes[key] = pd.DataFrame()
            else:
                if isinstance(data, pd.DataFrame):
                    dataframes[key] = data
                else:
                    dataframes[key] = pd.DataFrame(data)
        return dataframes

    def generate_transactions(self, accounts: List[dict], start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Generate transactions for a list of accounts."""
        if not accounts:
            return pd.DataFrame()  # Return empty DataFrame if no accounts
            
        fake = Faker()
        transactions = []
        
        # Set default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).isoformat()
        if not end_date:
            end_date = datetime.now().isoformat()
            
        # Generate transactions for each account
        for account in accounts:
            num_transactions = random.randint(50, 200)  # Generate between 50-200 transactions per account
            
            for _ in range(num_transactions):
                transaction_date = fake.date_time_between(
                    start_date=start_date,
                    end_date=end_date
                )
                
                is_debit = random.choice([True, False])
                amount = round(random.uniform(100, 50000), 2)
                
                # Generate counterparty details
                counterparty_country = random.choice(self.COUNTRIES)
                is_high_risk = counterparty_country in self.COUNTRIES
                
                transaction = {
                    'transaction_id': str(uuid.uuid4()),
                    'account_id': account['account_id'],
                    'transaction_date': transaction_date.isoformat(),
                    'transaction_type': random.choice(['Wire', 'ACH', 'Check', 'Cash']),
                    'amount': amount,
                    'currency': account['currency'],
                    'is_debit': is_debit,
                    'counterparty_name': fake.company(),
                    'counterparty_account': fake.bothify(text='#########'),
                    'counterparty_country': counterparty_country,
                    'screening_alert': is_high_risk,
                    'risk_score': random.randint(70, 100) if is_high_risk else random.randint(0, 69),
                    'alert_details': f'High-risk country: {counterparty_country}' if is_high_risk else None
                }
                transactions.append(transaction)
        
        # Convert to DataFrame and sort by date
        df = pd.DataFrame(transactions)
        if not df.empty:
            df = df.sort_values('transaction_date')
        
        return df

async def main():
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    parser.add_argument('--num-institutions', type=int, default=5, help='Number of institutions to generate')
    args = parser.parse_args()

    generator = TestDataGenerator()

    try:
        # Generate data
        data = await generator.generate_all_data(args.num_institutions)
        
        # Save to CSV files
        output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in data.items():
            if not df.empty:
                output_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(output_path, index=False)
                print(f"Saved {name} to {output_path}")
        
        print("Data generation and saving completed successfully")
        
    except Exception as e:
        print(f"Error during data generation: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())
