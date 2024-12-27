import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import Subsidiary
from .base_generator import BaseGenerator

class SubsidiaryGenerator(BaseGenerator):
    """Generator for subsidiary data."""

    async def generate(self, parent_institution_id: str) -> AsyncGenerator[Subsidiary, None]:
        """Generate subsidiaries for a given parent institution."""
        num_subsidiaries = random.randint(
            self.config.get('min_subsidiaries_per_institution', 1),
            self.config.get('max_subsidiaries_per_institution', 5)
        )
        
        for _ in range(num_subsidiaries):
            # Generate dates
            incorporation_date = self.fake.date_between(start_date='-20y', end_date='-1y')
            acquisition_date = self.fake.date_between(start_date=incorporation_date, end_date='today')
            current_time = datetime.now()
            
            # Determine if subsidiary is also a customer
            is_customer = random.choice([True, False])
            customer_id = str(uuid4()) if is_customer else None
            customer_onboarding_date = self.fake.date_between(start_date=acquisition_date, end_date='today').strftime('%Y-%m-%d') if is_customer else None
            customer_risk_rating = random.choice(['low', 'medium', 'high']) if is_customer else None
            customer_status = random.choice(['active', 'inactive', 'suspended']) if is_customer else None
            
            subsidiary = Subsidiary(
                subsidiary_id=str(uuid4()),
                parent_institution_id=parent_institution_id,
                legal_name=self.fake.company(),
                tax_id=self.fake.bothify(text='??####????'),
                incorporation_country=self.fake.country_code(),
                incorporation_date=incorporation_date.strftime('%Y-%m-%d'),
                acquisition_date=acquisition_date.strftime('%Y-%m-%d'),
                business_type=random.choice(['trading', 'investment', 'operations', 'holding']),
                operational_status=random.choice(['active', 'dormant', 'liquidating']),
                parent_ownership_percentage=round(random.uniform(51, 100), 2),
                consolidation_status=random.choice(['full', 'partial', 'none']),
                capital_investment=round(random.uniform(100000, 10000000), 2),
                functional_currency=random.choice(['USD', 'EUR', 'GBP', 'JPY']),
                material_subsidiary=random.choice([True, False]),
                risk_classification=random.choice(['low', 'medium', 'high']),
                regulatory_status=random.choice(['regulated', 'unregulated']),
                local_licenses=self.fake.words(nb=random.randint(1, 3)),
                integration_status=random.choice(['full', 'partial', 'none']),
                revenue=round(random.uniform(1000000, 100000000), 2),
                assets=round(random.uniform(5000000, 500000000), 2),
                liabilities=round(random.uniform(500000, 50000000), 2),
                reporting_frequency=random.choice(['monthly', 'quarterly', 'annually']),
                requires_local_audit=random.choice([True, False]),
                corporate_governance_model=random.choice(['board', 'committee', 'single_director']),
                is_customer=is_customer,
                is_regulated=random.choice([True, False]),
                industry_codes=self.fake.words(nb=random.randint(1, 3)),
                financial_metrics={
                    'revenue_growth': round(random.uniform(-0.2, 0.5), 2),
                    'profit_margin': round(random.uniform(0.05, 0.3), 2),
                    'debt_to_equity': round(random.uniform(0.5, 2.0), 2),
                    'current_ratio': round(random.uniform(1.0, 3.0), 2),
                    'return_on_assets': round(random.uniform(0.02, 0.15), 2)
                },
                customer_id=customer_id,
                customer_onboarding_date=customer_onboarding_date,
                customer_risk_rating=customer_risk_rating,
                customer_status=customer_status,
                created_at=current_time.isoformat(),
                updated_at=current_time.isoformat()
            )
            
            yield subsidiary
