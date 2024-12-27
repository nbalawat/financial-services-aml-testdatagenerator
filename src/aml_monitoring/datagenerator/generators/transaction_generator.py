import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import Transaction, TransactionType, TransactionStatus, Account
from .base_generator import BaseGenerator

class TransactionGenerator(BaseGenerator):
    """Generator for transaction data."""

    async def generate(self, account: Account) -> AsyncGenerator[Transaction, None]:
        """Generate transactions for a given account."""
        num_transactions = random.randint(
            self.config.get('min_transactions_per_account', 10),
            self.config.get('max_transactions_per_account', 50)
        )
        
        for _ in range(num_transactions):
            # Generate dates
            current_time = datetime.now()
            transaction_date = self.fake.date_time_between(
                start_date='-30d',
                end_date='now'
            ).isoformat()
            
            value_date = (datetime.strptime(transaction_date.split('T')[0], '%Y-%m-%d') + 
                         timedelta(days=random.randint(0, 2))).strftime('%Y-%m-%d')
            
            is_debit = random.choice([True, False])
            amount = round(random.uniform(100, 1000000), 2)
            
            # Set debit and credit account IDs based on is_debit
            if is_debit:
                debit_account_id = account.account_id
                credit_account_id = str(uuid4())
            else:
                debit_account_id = str(uuid4())
                credit_account_id = account.account_id
            
            transaction = Transaction(
                transaction_id=str(uuid4()),
                transaction_type=random.choice(list(TransactionType)),
                transaction_date=transaction_date,
                amount=amount,
                currency=account.currency,
                transaction_status=random.choice(list(TransactionStatus)),
                is_debit=is_debit,
                account_id=account.account_id,
                entity_id=account.entity_id,
                entity_type=account.entity_type,
                debit_account_id=debit_account_id,
                credit_account_id=credit_account_id,
                counterparty_account=str(uuid4()),
                counterparty_name=self.fake.company(),
                counterparty_bank=self.fake.company(),
                counterparty_entity_name=self.fake.company(),
                originating_country=self.fake.country_code(),
                destination_country=self.fake.country_code(),
                purpose=random.choice(['payment', 'transfer', 'investment', 'fee']),
                reference_number=self.fake.bothify(text='REF####????###'),
                screening_alert=random.random() < 0.05,  # 5% chance of alert
                alert_details='Suspicious activity detected' if random.random() < 0.05 else None,
                risk_score=random.randint(1, 100) if random.random() < 0.1 else None,
                processing_fee=round(random.uniform(1, 100), 2) if random.random() < 0.8 else None,
                exchange_rate=round(random.uniform(0.5, 2), 4) if random.random() < 0.3 else None,
                value_date=value_date,
                batch_id=f"BATCH{random.randint(1000, 9999)}" if random.random() < 0.2 else None,
                check_number=f"CHK{random.randint(1000, 9999)}" if random.random() < 0.1 else None,
                wire_reference=f"WIRE{random.randint(1000, 9999)}" if random.random() < 0.15 else None,
                created_at=current_time.isoformat(),
                updated_at=current_time.isoformat()
            )
            
            yield transaction
