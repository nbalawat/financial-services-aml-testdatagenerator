import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import Account, RiskRating
from .base_generator import BaseGenerator

class AccountGenerator(BaseGenerator):
    """Generator for account data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Account, None]:
        """Generate accounts for a given entity."""
        num_accounts = random.randint(
            self.config.get('min_accounts_per_entity', 1),
            self.config.get('max_accounts_per_entity', 5)
        )
        
        for _ in range(num_accounts):
            # Generate dates
            opening_date = self.fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
            last_activity_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
            
            account = Account(
                account_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                account_type=random.choice(['current', 'savings', 'investment', 'trading']),
                account_number=self.fake.bothify(text='??####?????###'),
                currency=self.fake.currency_code(),
                status=random.choice(['active', 'dormant', 'closed']),
                opening_date=opening_date,
                balance=round(random.uniform(1000, 10000000), 2),
                risk_rating=random.choice(list(RiskRating)),
                last_activity_date=last_activity_date,
                purpose=random.choice(['business', 'investment', 'trading', 'operational']),
                average_monthly_balance=round(random.uniform(1000, 10000000), 2),
                custodian_bank=self.fake.company() if random.random() > 0.5 else None,
                account_officer=self.fake.name() if random.random() > 0.5 else None,
                custodian_country=self.fake.country_code() if random.random() > 0.5 else None
            )
            
            yield account
