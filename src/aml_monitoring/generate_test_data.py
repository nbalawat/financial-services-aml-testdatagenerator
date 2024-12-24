import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from faker import Faker
import argparse
import asyncio
from enum import Enum
from .models import (
    Institution, Subsidiary, ComplianceEvent, RiskAssessment,
    BeneficialOwner, AuthorizedPerson, Address, Document,
    JurisdictionPresence, Account, RiskRating, BusinessType,
    ComplianceEventType, OperationalStatus, Customer, Transaction, TransactionType, TransactionStatus
)
from .transaction_generator import TransactionGenerator
from .db_handlers import DatabaseManager

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

    def generate_institutions(self, num_institutions: int) -> List[dict]:
        """Generate financial institutions."""
        institutions = []
        try:
            for _ in range(num_institutions):
                # Generate dates in the past
                today = datetime.now().date()
                incorporation_date = today - timedelta(days=random.randint(365, 20*365))  # 1-20 years ago
                onboarding_date = incorporation_date + timedelta(days=random.randint(30, 365))  # 1-12 months after incorporation
                
                # Last review date between onboarding and today
                days_since_onboarding = (today - onboarding_date).days
                if days_since_onboarding > 0:
                    random_review_days = random.randint(0, days_since_onboarding)
                    last_review_date = onboarding_date + timedelta(days=random_review_days)
                else:
                    last_review_date = onboarding_date
                
                # Generate audit dates
                last_audit_date = onboarding_date + timedelta(days=random.randint(30, max(days_since_onboarding, 30)))
                next_audit_date = today + timedelta(days=random.randint(30, 365))  # 1-12 months in future
                
                # KYC refresh date between onboarding and today
                kyc_refresh_date = onboarding_date + timedelta(days=random.randint(0, max(days_since_onboarding, 30)))
                
                # Next review date in the future (up to 1 year)
                next_review_date = today + timedelta(days=random.randint(1, 365))
                
                institution = Institution(
                    institution_id=str(uuid.uuid4()),
                    legal_name=fake.company(),
                    incorporation_country=random.choice(self.COUNTRIES),
                    incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                    onboarding_date=onboarding_date.strftime('%Y-%m-%d'),
                    business_type=random.choice(['bank', 'credit_union', 'investment_firm', 'insurance_company']),
                    operational_status='active',
                    primary_currency=random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CHF']),
                    regulatory_status='regulated',
                    primary_business_activity=random.choice(['Commercial Banking', 'Investment Banking', 'Asset Management', 'Insurance']),
                    primary_regulator=random.choice(['SEC', 'FCA', 'FINMA', 'MAS', 'HKMA']),
                    licenses=['banking_license', 'securities_license'],
                    aml_program_status='active',
                    kyc_refresh_date=kyc_refresh_date.strftime('%Y-%m-%d'),
                    last_audit_date=last_audit_date.strftime('%Y-%m-%d'),
                    next_audit_date=next_audit_date.strftime('%Y-%m-%d'),
                    relationship_manager=fake.name(),
                    relationship_status='active',
                    swift_code=fake.bothify(text='????##??###'),
                    lei_code=fake.bothify(text='????????0000########??'),
                    tax_id=fake.bothify(text='##-#######'),
                    website=fake.url(),
                    primary_contact_name=fake.name(),
                    primary_contact_email=fake.email(),
                    primary_contact_phone=fake.phone_number(),
                    annual_revenue=float(random.randint(1000000, 1000000000)),
                    employee_count=random.randint(100, 10000),
                    year_established=incorporation_date.year,
                    customer_status='active',
                    last_review_date=last_review_date.strftime('%Y-%m-%d'),
                    next_review_date=next_review_date.strftime('%Y-%m-%d'),
                    industry_codes=['6021', '6022', '6029'],
                    public_company=random.choice([True, False]),
                    risk_rating=random.choice(['low', 'medium', 'high']),
                    business_description=self._get_business_activity('financial'),
                    status='active',
                    notes=fake.text(max_nb_chars=100) if random.random() < 0.3 else None
                )
                institutions.append(self._validate_and_convert_to_dict(institution))
        except Exception as e:
            print(f"Error in generate_institutions: {e}")
        return institutions

    def generate_subsidiaries(self, institutions: List[dict]) -> List[dict]:
        """Generate subsidiaries for institutions."""
        subsidiaries = []
        try:
            print(f"Starting subsidiary generation for {len(institutions)} institutions")
            for institution in institutions:
                print(f"Processing institution {institution['institution_id']}")
                num_subsidiaries = random.randint(1, 3)
                print(f"Attempting to generate {num_subsidiaries} subsidiaries")
                
                inst_incorporation = datetime.strptime(institution['incorporation_date'], '%Y-%m-%d').date()
                print(f"Institution incorporation date: {inst_incorporation}")
                
                today = datetime.now().date()
                days_since_parent_inc = (today - inst_incorporation).days
                print(f"Days since parent incorporation: {days_since_parent_inc}")
                
                if days_since_parent_inc <= 0:
                    print(f"Warning: Institution incorporation date {inst_incorporation} is in the future")
                    continue
                
                for i in range(num_subsidiaries):
                    print(f"Generating subsidiary {i+1}/{num_subsidiaries}")
                    # Generate incorporation date between parent incorporation and today
                    max_days = min(days_since_parent_inc, 3650)  # Cap at ~10 years
                    if max_days <= 30:
                        print(f"Warning: Parent institution too new (only {max_days} days old)")
                        continue
                        
                    incorporation_date = inst_incorporation + timedelta(days=random.randint(30, max_days))
                    print(f"Generated subsidiary incorporation date: {incorporation_date}")
                    
                    # Generate acquisition date after subsidiary incorporation but before today
                    days_since_incorporation = (today - incorporation_date).days
                    if days_since_incorporation <= 30:  # Need at least 30 days between incorporation and acquisition
                        print(f"Warning: Subsidiary incorporation too recent")
                        continue
                    
                    acquisition_date = incorporation_date + timedelta(days=random.randint(30, days_since_incorporation))
                    print(f"Generated subsidiary acquisition date: {acquisition_date}")
                    
                    try:
                        # Generate financial metrics
                        financial_metrics = {
                            'annual_revenue': float(random.randint(1000000, 100000000)),
                            'total_assets': float(random.randint(5000000, 500000000)),
                            'net_income': float(random.randint(100000, 10000000)),
                            'employee_count': random.randint(50, 5000)
                        }
                        
                        # Generate local licenses based on business type
                        business_type = random.choice(['lending', 'investment', 'insurance', 'payment_processing'])
                        local_licenses = []
                        if business_type == 'lending':
                            local_licenses = ['lending_license', 'consumer_credit_license']
                        elif business_type == 'investment':
                            local_licenses = ['investment_advisor_license', 'broker_dealer_license']
                        elif business_type == 'insurance':
                            local_licenses = ['insurance_license', 'reinsurance_license']
                        else:
                            local_licenses = ['payment_processor_license', 'money_transmitter_license']
                        
                        subsidiary = Subsidiary(
                            subsidiary_id=str(uuid.uuid4()),
                            parent_institution_id=institution['institution_id'],
                            legal_name=fake.company(),
                            tax_id=fake.bothify(text='##-#######'),
                            incorporation_country=random.choice(self.COUNTRIES),
                            incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                            acquisition_date=acquisition_date.strftime('%Y-%m-%d'),
                            business_type=business_type,
                            operational_status='active',
                            parent_ownership_percentage=round(random.uniform(51, 100), 2),
                            consolidation_status='consolidated',
                            capital_investment=float(random.randint(1000000, 50000000)),
                            functional_currency=random.choice(self.CURRENCIES),
                            material_subsidiary=random.choice([True, False]),
                            risk_classification=random.choice(['low', 'medium', 'high']),
                            regulatory_status='regulated',
                            local_licenses=local_licenses,
                            integration_status='integrated',
                            financial_metrics=financial_metrics,
                            reporting_frequency='quarterly',
                            requires_local_audit=True,
                            corporate_governance_model='board_of_directors',
                            is_regulated=True,
                            is_customer=False
                        )
                        subsidiary_dict = self._validate_and_convert_to_dict(subsidiary)
                        if subsidiary_dict:
                            subsidiaries.append(subsidiary_dict)
                            print(f"Successfully generated subsidiary {subsidiary_dict['subsidiary_id']}")
                        else:
                            print("Failed to validate subsidiary")
                    except Exception as e:
                        print(f"Error creating subsidiary: {e}")
                        continue
                    
        except Exception as e:
            print(f"Error in generate_subsidiaries: {e}")
            
        print(f"Generated {len(subsidiaries)} subsidiaries in total")
        return subsidiaries

    def generate_addresses(self, entity_id: str, entity_type: str, num_addresses: int) -> List[dict]:
        """Generate addresses for an entity."""
        addresses = []
        try:
            for i in range(num_addresses):
                # Generate dates in the past 5 years
                today = datetime.now().date()
                effective_from = today - timedelta(days=random.randint(0, 5*365))  # Up to 5 years ago
                
                # 20% chance of having an effective_to date
                effective_to = None
                if random.random() < 0.2:
                    days_until_today = (today - effective_from).days
                    if days_until_today > 0:
                        random_end_days = random.randint(0, days_until_today)
                        effective_to = effective_from + timedelta(days=random_end_days)
                
                # Generate last_verified date between effective_from and today
                days_since_effective = (today - effective_from).days
                if days_since_effective > 0:
                    random_verify_days = random.randint(0, days_since_effective)
                    last_verified = effective_from + timedelta(days=random_verify_days)
                else:
                    last_verified = effective_from
                
                address = Address(
                    address_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    address_type=random.choice(['business', 'mailing', 'registered']),
                    address_line1=fake.street_address(),
                    address_line2=fake.secondary_address() if random.random() < 0.3 else None,
                    city=fake.city(),
                    state_province=fake.state(),
                    postal_code=fake.postcode(),
                    country=random.choice(self.COUNTRIES),
                    status='active',
                    effective_from=effective_from.strftime('%Y-%m-%d'),
                    effective_to=effective_to.strftime('%Y-%m-%d') if effective_to else None,
                    primary_address=i == 0,  # First address is primary
                    validation_status='verified',
                    last_verified=last_verified.strftime('%Y-%m-%d'),
                    geo_coordinates={'latitude': float(fake.latitude()), 'longitude': float(fake.longitude())},
                    timezone=fake.timezone()
                )
                addresses.append(self._validate_and_convert_to_dict(address))
        except Exception as e:
            print(f"Error in generate_addresses: {e}")
        return addresses

    def generate_beneficial_owners(self, entity_id: str, entity_type: str, num_owners: int) -> List[dict]:
        """Generate beneficial owners for an entity."""
        owners = []
        total_ownership = 100.0
        
        for i in range(num_owners):
            # For the last owner, assign remaining ownership
            if i == num_owners - 1:
                ownership = total_ownership
            else:
                # Generate random ownership ensuring we don't exceed 100%
                max_ownership = min(75.0, total_ownership - (num_owners - i - 1) * 5.0)  # Leave at least 5% for others
                ownership = round(random.uniform(5.0, max_ownership), 2)
                total_ownership -= ownership
            
            verification_date = fake.date_between(start_date='-2y', end_date='today')
            
            owner = BeneficialOwner(
                owner_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                name=fake.name(),
                nationality=random.choice(self.COUNTRIES),
                country_of_residence=random.choice(self.COUNTRIES),
                ownership_percentage=ownership,
                dob=fake.date_between(start_date='-80y', end_date='-25y').strftime('%Y-%m-%d'),
                verification_date=verification_date.strftime('%Y-%m-%d'),
                pep_status=random.random() < 0.1,  # 10% chance of being PEP
                sanctions_status=random.random() < 0.05,  # 5% chance of sanctions
                adverse_media_status=random.random() < 0.15,  # 15% chance of adverse media
                verification_source=random.choice(['document_verification', 'database_check', 'third_party_verification']),
                notes=fake.text(max_nb_chars=200) if random.random() < 0.3 else None
            )
            owners.append(self._validate_and_convert_to_dict(owner))
        return owners

    def generate_accounts(self, entity_id: str, entity_type: str, num_accounts: int) -> List[dict]:
        """Generate accounts for an entity."""
        accounts = []
        for _ in range(num_accounts):
            opening_date = fake.date_between(start_date='-5y', end_date='today')
            last_activity = fake.date_between(start_date=opening_date, end_date='today')
            currency = random.choice(self.CURRENCIES)
            
            account = Account(
                account_id=str(uuid.uuid4()),
                entity_id=entity_id,
                entity_type=entity_type,
                account_type=random.choice(['checking', 'savings', 'investment', 'custody']),
                account_number=fake.bothify(text='??##########'),
                currency=currency,
                status='active',
                opening_date=opening_date.strftime('%Y-%m-%d'),
                last_activity_date=last_activity.strftime('%Y-%m-%d'),
                balance=float(random.randint(10000, 10000000)),
                risk_rating=random.choice(self.RISK_RATINGS),
                purpose=random.choice(['business_operations', 'investment', 'trading']) if random.random() < 0.8 else None,
                average_monthly_balance=float(random.randint(5000, 5000000)) if random.random() < 0.8 else None,
                custodian_bank=fake.company() if random.random() < 0.5 else None,
                account_officer=fake.name() if random.random() < 0.7 else None,
                custodian_country=random.choice(self.COUNTRIES) if random.random() < 0.5 else None
            )
            accounts.append(self._validate_and_convert_to_dict(account))
        return accounts

    def generate_compliance_events(self, entity_id: str, entity_type: str, onboarding_date: Optional[str], num_events: int) -> List[dict]:
        """Generate compliance events for an entity."""
        events = []
        if not onboarding_date:
            return events

        try:
            # Parse onboarding date
            onboarding = datetime.strptime(onboarding_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            
            # Calculate days between onboarding and today
            days_since_onboarding = (today - onboarding).days
            if days_since_onboarding <= 0:
                return events
            
            for _ in range(num_events):
                # Generate random event date between onboarding and today
                random_days = random.randint(0, days_since_onboarding)
                event_date = onboarding + timedelta(days=random_days)
                
                # Generate decision date (80% chance, must be after event date)
                decision_date = None
                decision = None
                decision_maker = None
                if random.random() < 0.8:
                    decision_date = today + timedelta(days=random.randint(1, 365))
                    decision = random.choice(['approved', 'rejected', 'pending_review'])
                    decision_maker = fake.name()
                
                # Generate next review date (70% chance)
                next_review = None
                if random.random() < 0.7:
                    next_review = today + timedelta(days=random.randint(1, 365))
                
                event = ComplianceEvent(
                    event_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    event_date=event_date.strftime('%Y-%m-%d'),
                    event_type=random.choice([e.value for e in ComplianceEventType]),
                    event_description=fake.text(max_nb_chars=200),
                    old_state=random.choice(['active', 'pending', 'under_review']) if random.random() < 0.5 else None,
                    new_state=random.choice(['active', 'pending', 'under_review', 'completed']),
                    decision=decision,
                    decision_date=decision_date.strftime('%Y-%m-%d') if decision_date else None,
                    decision_maker=decision_maker,
                    next_review_date=next_review.strftime('%Y-%m-%d') if next_review else None,
                    related_account_id=str(uuid.uuid4()),
                    notes=fake.text(max_nb_chars=100) if random.random() < 0.3 else None
                )
                events.append(self._validate_and_convert_to_dict(event))
        except Exception as e:
            print(f"Error in generate_compliance_events: {e}")
            
        return events

    def generate_jurisdiction_presence(self, entity_id: str, entity_type: str, registration_date: str, num_presence: int) -> List[dict]:
        """Generate jurisdiction presence for an entity."""
        presence = []
        try:
            # Parse registration_date to datetime
            reg_date = datetime.strptime(registration_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            
            for _ in range(num_presence):
                # Generate random date between registration date and today
                days_since_reg = (today - reg_date).days
                if days_since_reg <= 0:
                    continue
                    
                random_days = random.randint(0, days_since_reg)
                effective_from = reg_date + timedelta(days=random_days)
                
                # 20% chance of having an effective_to date
                effective_to = None
                if random.random() < 0.2:
                    effective_to = today + timedelta(days=random.randint(1, 365))
                
                jurisdiction = JurisdictionPresence(
                    presence_id=str(uuid.uuid4()),
                    entity_id=entity_id,
                    entity_type=entity_type,
                    jurisdiction=random.choice(self.COUNTRIES),
                    registration_date=registration_date,
                    effective_from=effective_from.strftime('%Y-%m-%d'),
                    effective_to=effective_to.strftime('%Y-%m-%d') if effective_to else None,
                    status=random.choice(['active', 'inactive', 'pending']),
                    local_registration_id=fake.bothify(text='##########'),
                    local_registration_date=effective_from.strftime('%Y-%m-%d'),
                    local_registration_authority=random.choice(['Financial Services Authority', 'Central Bank', 'Securities Commission', 'Monetary Authority']),
                    notes=fake.text(max_nb_chars=100) if random.random() < 0.3 else None
                )
                presence.append(self._validate_and_convert_to_dict(jurisdiction))
        except Exception as e:
            print(f"Error in generate_jurisdiction_presence: {e}")
        return presence

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

    def get_entity_by_id(self, entity_id: str, entity_type: str, institutions: List[dict], subsidiaries: List[dict]) -> Optional[dict]:
        """Helper function to get entity by ID and type."""
        if entity_type == 'institution':
            return next((inst for inst in institutions if inst['institution_id'] == entity_id), None)
        else:
            return next((sub for sub in subsidiaries if sub['subsidiary_id'] == entity_id), None)

    async def process_entity(self, entity_id: str, entity_type: str, institutions: List[dict], subsidiaries: List[dict]):
        """Process a single entity."""
        entity_data = self.get_entity_by_id(entity_id, entity_type, institutions, subsidiaries)
        if not entity_data:
            return None

    async def generate_all_data(self, num_institutions: int = 5) -> Dict[str, pd.DataFrame]:
        """Generate all test data."""
        print(f"Generating {num_institutions} institutions...")
        
        # Generate institutions first
        institutions = self.generate_institutions(num_institutions)
        if not institutions:
            raise ValueError("Failed to generate institutions")
            
        # Generate subsidiaries for each institution
        print("Generating subsidiaries...")
        subsidiaries = []
        for inst in institutions:
            # Generate 1-3 subsidiaries per institution
            subs = self.generate_subsidiaries([inst])  # Pass as list since generate_subsidiaries expects a list
            if subs:
                subsidiaries.extend(subs)
            else:
                print(f"Warning: No subsidiaries generated for institution {inst['institution_id']}")
        
        # Create a list of all entities (institutions and subsidiaries) with their registration dates
        print("Processing entities...")
        entities = []
        for inst in institutions:
            entities.append((inst['institution_id'], 'institution', inst.get('incorporation_date')))
        for sub in subsidiaries:
            # Use incorporation_date instead of acquisition_date for jurisdiction presence
            entities.append((sub['subsidiary_id'], 'subsidiary', sub.get('incorporation_date')))
        
        # Generate addresses for each entity
        print("Generating addresses...")
        addresses = []
        for entity_id, entity_type, _ in entities:
            addresses.extend(self.generate_addresses(entity_id, entity_type, random.randint(1, 3)))
        
        # Generate beneficial owners for each entity
        print("Generating beneficial owners...")
        beneficial_owners = []
        for entity_id, entity_type, _ in entities:
            beneficial_owners.extend(self.generate_beneficial_owners(entity_id, entity_type, random.randint(1, 5)))
        
        # Generate jurisdiction presence for each entity
        print("Generating jurisdiction presence...")
        jurisdiction_presence = []
        for entity_id, entity_type, reg_date in entities:
            if reg_date:  # Only generate if we have a registration date
                try:
                    presence_records = self.generate_jurisdiction_presence(entity_id, entity_type, reg_date, random.randint(1, 3))
                    if presence_records:
                        jurisdiction_presence.extend(presence_records)
                except Exception as e:
                    print(f"Error generating jurisdiction presence for {entity_id}: {e}")
        
        # Generate compliance events for each entity
        print("Generating compliance events...")
        compliance_events = []
        for entity_id, entity_type, _ in entities:
            # Get the appropriate date for compliance events
            event_start_date = None
            if entity_type == 'institution':
                inst = next(i for i in institutions if i['institution_id'] == entity_id)
                event_start_date = inst.get('onboarding_date')
            else:
                sub = next(s for s in subsidiaries if s['subsidiary_id'] == entity_id)
                event_start_date = sub.get('acquisition_date')
            
            if event_start_date:
                try:
                    event_records = self.generate_compliance_events(entity_id, entity_type, event_start_date, random.randint(2, 5))
                    if event_records:
                        compliance_events.extend(event_records)
                except Exception as e:
                    print(f"Error generating compliance events for {entity_id}: {e}")
        
        # Generate accounts for each entity
        print("Generating accounts...")
        accounts = []
        for entity_id, entity_type, _ in entities:
            accounts.extend(self.generate_accounts(entity_id, entity_type, random.randint(1, 5)))
        
        # Generate transactions for each account
        print("Generating transactions...")
        transactions = []
        
        # Create accounts DataFrame
        accounts_df = pd.DataFrame(accounts)
        
        # Generate transactions using the transaction generator
        transactions = await self.transaction_generator.generate_transactions_for_accounts(
            accounts_df=accounts_df,
            batch_size=1000
        )
        
        # Convert to DataFrames
        print("\nConverting to DataFrames...")
        data = {
            'institutions': pd.DataFrame(institutions),
            'subsidiaries': pd.DataFrame(subsidiaries),
            'addresses': pd.DataFrame(addresses),
            'beneficial_owners': pd.DataFrame(beneficial_owners),
            'jurisdiction_presence': pd.DataFrame(jurisdiction_presence),
            'compliance_events': pd.DataFrame(compliance_events),
            'accounts': pd.DataFrame(accounts),
            'transactions': pd.DataFrame(transactions)
        }
        
        return data

    async def generate_transactions(self, accounts: List[dict], start_date: str = None, end_date: str = None) -> List[dict]:
        """Generate transactions for a list of accounts."""
        if not accounts:
            return []

        # Convert to DataFrame for efficient processing
        accounts_df = pd.DataFrame(accounts)
        
        # Generate transactions
        transactions = await self.transaction_generator.generate_transactions_for_accounts(
            accounts_df,
            start_date=start_date,
            end_date=end_date,
            batch_size=100
        )
        
        # Return transactions directly as a list of dictionaries
        return transactions

async def main():
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    parser.add_argument('--num-institutions', type=int, default=5, help='Number of institutions to generate')
    args = parser.parse_args()

    generator = TestDataGenerator()
    db_manager = DatabaseManager()

    try:
        # Clean up existing data
        await db_manager.cleanup_postgres()
        await db_manager.cleanup_neo4j()
        
        # Generate data
        data = await generator.generate_all_data(args.num_institutions)
        
        # Save to databases
        await db_manager.save_to_postgres(data)
        await db_manager.save_to_neo4j(data)
        
        # Save to CSV files
        output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
        os.makedirs(output_dir, exist_ok=True)
        
        for name, df in data.items():
            if not df.empty:
                output_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(output_path, index=False)
                print(f"Saved {name} to {output_path}")
        
        print("\n=== Data Generation Complete ===")
        print("Summary of generated data:")
        for name, df in data.items():
            print(f"- {name}: {len(df)} records")
        
    except Exception as e:
        print(f"Error during data generation: {e}")
        raise
    finally:
        # Close database connections
        await db_manager.postgres_handler.close()
        await db_manager.neo4j_handler.close()

if __name__ == '__main__':
    asyncio.run(main())
