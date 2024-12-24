import asyncio
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from faker import Faker
from .models import Transaction, TransactionType, TransactionStatus

fake = Faker()

class TransactionGenerator:
    COUNTRIES = ['US', 'GB', 'DE', 'FR', 'JP', 'CN', 'IN', 'BR', 'AU', 'CA']
    HIGH_RISK_COUNTRIES = [
        'IR', 'KP', 'SY', 'CU', 'VE',  # Sanctioned
        'AF', 'MM', 'LA', 'KH', 'YE'    # High-risk
    ]
    
    def __init__(self):
        self.transaction_patterns = {
            'current': {'daily': (1, 5), 'weekly': (5, 15), 'monthly': (20, 50)},
            'savings': {'weekly': (0, 2), 'monthly': (1, 5)},
            'investment': {'monthly': (1, 3), 'quarterly': (1, 2)},
            'loan': {'monthly': (1, 1)}
        }

    async def generate_transactions_for_accounts(
        self, accounts_df: pd.DataFrame,
        start_date: str = None, end_date: str = None,
        batch_size: int = 100
    ) -> List[dict]:
        """Generate transactions for multiple accounts."""
        if accounts_df.empty:
            return []

        # Convert dates to datetime
        current_date = datetime.now().date()
        if start_date:
            start_dt = pd.to_datetime(start_date).date()
        else:
            start_dt = current_date - timedelta(days=730)  # 2 years ago
            
        if end_date:
            end_dt = pd.to_datetime(end_date).date()
        else:
            end_dt = current_date

        print(f"Generating transactions for {len(accounts_df)} accounts...")
        
        # Create account map for efficient lookups
        account_map = accounts_df.set_index('account_id').to_dict('index')
        all_transactions = []

        # Process accounts in batches
        total_batches = (len(accounts_df) + batch_size - 1) // batch_size
        for i in range(0, len(accounts_df), batch_size):
            batch_accounts = accounts_df.iloc[i:i + batch_size]
            print(f"Processing account batch {i//batch_size + 1} of {total_batches}...")
            
            # Process each account in the batch concurrently
            async def process_account(account):
                num_transactions = random.randint(50, 200)
                return await self._generate_account_transactions(
                    account, account_map, num_transactions,
                    start_dt, end_dt
                )

            # Create tasks for all accounts in the batch
            tasks = [process_account(account) for _, account in batch_accounts.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            
            # Extend transactions list with batch results
            for transactions in batch_results:
                if transactions:
                    all_transactions.extend(transactions)
            
            print(f"Generated {len(all_transactions)} transactions so far...")

        print(f"Finished generating {len(all_transactions)} transactions for all accounts")
        return all_transactions

    async def _generate_account_transactions(
        self, account: dict, account_map: dict, num_transactions: int,
        start_dt: datetime, end_dt: datetime
    ) -> List[dict]:
        """Generate transactions for a single account."""
        transactions = []
        account_type = account.get('account_type', 'current').lower()

        # Get transaction patterns for this account type
        patterns = self.transaction_patterns.get(account_type, {'monthly': (1, 5)})
        
        # Generate transactions for each frequency pattern
        tasks = []
        for frequency, (min_txns, max_txns) in patterns.items():
            num_freq_transactions = random.randint(
                min_txns * (num_transactions // len(patterns)),
                max_txns * (num_transactions // len(patterns))
            )
            task = self._generate_frequency_transactions(
                account, account_map, num_freq_transactions,
                start_dt, end_dt, frequency
            )
            tasks.append(task)
        
        # Wait for all frequency patterns to complete
        results = await asyncio.gather(*tasks)
        for freq_transactions in results:
            transactions.extend(freq_transactions)

        return transactions

    async def _generate_frequency_transactions(
        self, account: dict, account_map: dict, num_transactions: int,
        start_dt: datetime, end_dt: datetime, frequency: str
    ) -> List[dict]:
        """Generate transactions for a specific frequency."""
        transactions = []
        account_type = account.get('account_type', 'current').lower()
        
        # Define balance ranges based on account type
        balance_ranges = {
            'current': (100, 10000),
            'savings': (1000, 50000),
            'investment': (5000, 100000),
            'loan': (500, 5000)
        }
        balance_range = balance_ranges.get(account_type, (100, 10000))

        # Generate base transactions
        base_transactions = []
        for _ in range(num_transactions):
            txn = await self._generate_base_transaction(
                account['account_id'], account_map, account_type,
                balance_range, start_dt, end_dt
            )
            if txn:
                base_transactions.append(txn)

        # Sort transactions by date
        base_transactions.sort(key=lambda x: x['transaction_date'])
        
        return base_transactions

    async def _generate_base_transaction(
        self, account_id: str, account_map: dict, account_type: str,
        balance_range: tuple, start_dt: datetime, end_dt: datetime,
        recurring_amount: Optional[float] = None
    ) -> Optional[dict]:
        """Generate a base transaction."""
        try:
            amount = recurring_amount if recurring_amount else random.uniform(*balance_range)
            is_debit = random.choice([True, False])
            
            # Determine if this should be an outlier (1% chance for amounts > 100,000)
            is_outlier = random.random() < 0.01 and amount > 100000
            
            if is_outlier:
                amount *= random.uniform(10, 20)  # Make the outlier more extreme
            
            # Select counterparty country
            if random.random() < 0.1:  # 10% chance of high-risk country
                country = random.choice(self.HIGH_RISK_COUNTRIES)
            else:
                country = random.choice(self.COUNTRIES)
            
            # Check for screening alerts
            screening_alert, risk_score, alert_details = self._check_screening_alerts(country)
            
            # For outliers, always set alerts
            if is_outlier:
                screening_alert = True
                risk_score = random.randint(80, 100)
                alert_details = f"Large amount transaction: ${amount:,.2f}"
            
            # Generate random date within range
            days_range = (end_dt - start_dt).days
            random_days = random.randint(0, days_range)
            transaction_date = start_dt + timedelta(days=random_days)
            
            transaction_id = uuid.uuid4()
            counterparty_account = uuid.uuid4()

            # Create transaction with all required fields
            transaction = {
                'transaction_id': str(transaction_id),
                'entity_id': str(account_map[account_id]['entity_id']),
                'entity_type': account_map[account_id]['entity_type'],
                'account_id': str(account_id),
                'transaction_date': transaction_date.isoformat(),
                'transaction_type': random.choice([t.value for t in TransactionType]),
                'amount': float(amount),  # Ensure amount is float
                'currency': account_map[account_id]['currency'],
                'transaction_status': random.choice([s.value for s in TransactionStatus]),
                'is_debit': bool(is_debit),  # Ensure boolean
                'counterparty_name': fake.company(),
                'counterparty_account': str(counterparty_account),
                'counterparty_bank': fake.company(),
                'counterparty_entity_name': fake.company(),
                'originating_country': str(country),
                'destination_country': str(random.choice(self.COUNTRIES)),
                'purpose': fake.text(max_nb_chars=50),
                'reference_number': str(uuid.uuid4()),
                'screening_alert': bool(screening_alert),  # Ensure boolean
                'alert_details': str(alert_details) if alert_details else None,
                'risk_score': int(risk_score),  # Ensure integer
                'processing_fee': float(random.uniform(0, 100)),
                'exchange_rate': float(random.uniform(0.8, 1.2)),
                'value_date': (transaction_date + timedelta(days=random.randint(1, 5))).isoformat(),
                'batch_id': str(uuid.uuid4()),
                'check_number': str(random.randint(1000, 9999)) if random.random() < 0.1 else None,
                'wire_reference': str(uuid.uuid4()) if random.random() < 0.2 else None
            }

            # Debug log
            print(f"Generated transaction {transaction_id} with status: {transaction['transaction_status']}")
            return transaction
        except Exception as e:
            print(f"Error generating transaction: {e}")
            return None

    def _generate_transaction_date(self, now: datetime) -> str:
        """Generate a random transaction date."""
        days_ago = random.randint(0, 730)  # Up to 2 years ago
        txn_date = now - timedelta(days=days_ago)
        return txn_date.date().isoformat()  # Return date in YYYY-MM-DD format

    def _check_screening_alerts(self, country: str) -> tuple:
        """Check if transaction should trigger alerts based on country."""
        screening_alert = False
        alert_details = None
        risk_score = random.randint(1, 100)
        
        if country in self.HIGH_RISK_COUNTRIES:
            screening_alert = random.random() < 0.7
            if screening_alert:
                alert_details = f"Transaction involves high-risk country: {country}"
                risk_score = random.randint(70, 100)
        
        return screening_alert, risk_score, alert_details
