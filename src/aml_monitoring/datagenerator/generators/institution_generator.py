import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import Institution, BusinessType, RiskRating, OperationalStatus
from .base_generator import BaseGenerator

class InstitutionGenerator(BaseGenerator):
    """Generator for institution data."""

    async def generate(self) -> AsyncGenerator[Institution, None]:
        """Generate institutions based on configuration."""
        num_institutions = self.config.get('num_institutions', 5)
        for _ in range(num_institutions):
            business_type = random.choice(list(BusinessType))
            risk_rating = random.choice(list(RiskRating))
            operational_status = random.choice(list(OperationalStatus))
            
            # Generate dates
            incorporation_date = self.fake.date_between(start_date='-30y', end_date='-1y')
            onboarding_date = self.fake.date_between(start_date=incorporation_date, end_date='today')
            
            institution = Institution(
                institution_id=str(uuid4()),
                legal_name=self.fake.company(),
                business_type=business_type,
                incorporation_country=self.fake.country_code(),
                incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                onboarding_date=onboarding_date.strftime('%Y-%m-%d'),
                risk_rating=risk_rating,
                operational_status=operational_status,
                primary_currency=self.fake.currency_code(),
                regulatory_status=random.choice(['regulated', 'unregulated']),
                primary_business_activity=self.fake.bs(),
                primary_regulator=f"{self.fake.country()} Financial Authority",
                licenses=[f"License {i+1}" for i in range(random.randint(1, 3))],
                aml_program_status=random.choice(['compliant', 'in_review', 'remediation']),
                kyc_refresh_date=(datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
                last_audit_date=(datetime.now() - timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
                next_audit_date=(datetime.now() + timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
                relationship_manager=self.fake.name(),
                relationship_status=random.choice(['active', 'under_review', 'suspended']),
                swift_code=self.fake.bothify(text='????##??###'),
                lei_code=self.fake.bothify(text='#?#?#?#?#?#?#?#?#?#?#?'),
                tax_id=self.fake.bothify(text='??####????'),
                website=self.fake.url(),
                primary_contact_name=self.fake.name(),
                primary_contact_email=self.fake.email(),
                primary_contact_phone=self.fake.phone_number(),
                annual_revenue=round(random.uniform(1000000, 1000000000), 2),
                employee_count=random.randint(50, 10000),
                year_established=incorporation_date.year,
                customer_status=random.choice(['active', 'inactive', 'pending']),
                last_review_date=(datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
                industry_codes=[f"IND{random.randint(1000, 9999)}" for _ in range(random.randint(1, 3))],
                public_company=random.choice([True, False]),
                stock_symbol=self.fake.bothify(text='???') if random.choice([True, False]) else None,
                stock_exchange=random.choice(['NYSE', 'NASDAQ', 'LSE', None])
            )
            
            yield institution
