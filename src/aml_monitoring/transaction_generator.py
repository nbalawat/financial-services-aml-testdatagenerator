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
        start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """Generate transactions for multiple accounts."""
        # Set date range
        end_dt = pd.to_datetime(end_date) if end_date else pd.Timestamp('2024-12-24')  # Current date
        start_dt = pd.to_datetime(start_date) if start_date else (end_dt - pd.Timedelta(days=730))
        
        transactions = []
        account_map = {acc['account_id']: acc for _, acc in accounts_df.iterrows()}
        
        # Generate transactions for each account
        for _, account in accounts_df.iterrows():
            # Generate base transactions
            num_transactions = random.randint(50, 200)
            account_transactions = await self._generate_account_transactions(
                account, account_map, num_transactions,
                start_dt, end_dt
            )
            transactions.extend(account_transactions)
        
        # Convert to DataFrame and ensure all dates are within range
        df = pd.DataFrame(transactions)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Filter transactions to ensure they're within the date range
        mask = (df['transaction_date'] >= start_dt) & (df['transaction_date'] <= end_dt)
        df = df[mask].copy()
        
        # Ensure minimum transaction amount and handle outliers
        df.loc[df['amount'] < 100, 'amount'] = df.loc[df['amount'] < 100, 'amount'] + 100
        
        # Mark large transactions as outliers
        large_txns = df['amount'] > 100000
        df.loc[large_txns, 'screening_alert'] = True
        df.loc[large_txns, 'risk_score'] = df.loc[large_txns, 'risk_score'].apply(lambda x: max(x, 70))
        df.loc[large_txns, 'alert_details'] = df.loc[large_txns, 'amount'].apply(
            lambda x: f"Large transaction amount: ${x:,.2f}"
        )
        
        return df

    async def _generate_account_transactions(
        self, account: dict, account_map: dict, num_transactions: int,
        start_dt: datetime, end_dt: datetime
    ) -> List[dict]:
        """Generate transactions for a single account."""
        transactions = []
        
        # Get account type and balance range
        account_type = account.get('account_type', 'retail')
        balance_range = self._get_balance_range(account_type)
        
        # Generate transaction dates
        # Ensure dates are within the valid range
        account_start = pd.to_datetime(account['opening_date'])
        valid_start = max(start_dt, account_start)
        valid_end = min(end_dt, pd.Timestamp('2024-12-24'))  # Current date
        
        # Generate dates within the valid range
        date_range = pd.date_range(
            start=valid_start,
            end=valid_end,
            periods=num_transactions
        )
        date_range = sorted(date_range)  # Ensure dates are sorted
        
        # Generate some recurring transactions (20% chance)
        recurring_amount = None
        if random.random() < 0.2:
            recurring_amount = random.uniform(*balance_range)
        
        # Generate transactions
        for i in range(num_transactions):
            transaction = self._generate_base_transaction(
                account['account_id'],
                account_map,
                account_type,
                balance_range,
                recurring_amount if i % 5 == 0 else None  # Use recurring amount every 5th transaction
            )
            transaction['transaction_date'] = date_range[i]
            
            # Check if this should be an outlier (5% chance)
            if random.random() < 0.05:
                transaction = self._generate_outlier_transaction(transaction, account_type)
            elif transaction['amount'] > 100000:  # Force outlier for large amounts
                transaction['screening_alert'] = True
                transaction['risk_score'] = random.randint(70, 100)
                transaction['alert_details'] = f"Large transaction amount: ${transaction['amount']:,.2f}"
            
            transactions.append(transaction)
        
        return transactions

    def _get_balance_range(self, account_type: str) -> tuple:
        """Get balance range based on account type."""
        balance_ranges = {
            'retail': (100, 10000),
            'corporate': (1000, 50000),
            'investment': (5000, 100000)
        }
        return balance_ranges.get(account_type, balance_ranges['retail'])

    def _generate_base_transaction(
        self, account_id: str, account_map: dict, account_type: str,
        balance_range: tuple, recurring_amount: Optional[float] = None
    ) -> dict:
        """Generate a base transaction."""
        counterparty, counterparty_country = self._select_counterparty(account_id, account_map)
        amount = recurring_amount or round(random.uniform(*balance_range), 2)
        
        screening_data = self._check_screening_alerts(counterparty_country)
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'account_id': account_id,
            'transaction_type': random.choice(list(TransactionType)),
            'amount': amount,
            'currency': account_map[account_id]['currency'],
            'is_debit': random.choice([True, False]),
            'counterparty_name': fake.company(),
            'counterparty_account': counterparty['account_number'],
            'screening_alert': screening_data['screening_alert'],
            'risk_score': screening_data['risk_score'],
            'alert_details': screening_data['alert_details']
        }
        
        return transaction

    def _generate_outlier_transaction(self, base_transaction: dict, account_type: str) -> dict:
        """Generate an outlier transaction."""
        transaction = base_transaction.copy()
        outlier_type = random.choice(['amount', 'frequency', 'pattern'])
        
        # Set base outlier properties
        transaction['screening_alert'] = True
        transaction['risk_score'] = random.randint(70, 100)
        
        # Generate outlier amount if needed
        if outlier_type == 'amount':
            base_amount = transaction['amount']
            outlier_amount = self._get_outlier_amount(base_amount, account_type)
            transaction['amount'] = outlier_amount
            transaction['alert_details'] = f"Large transaction amount: ${outlier_amount:,.2f}"
        
        elif outlier_type == 'frequency':
            transaction['alert_details'] = "Unusual transaction frequency pattern detected"
            # Make amount slightly larger than normal
            transaction['amount'] *= random.uniform(1.5, 3.0)
        
        else:  # pattern
            transaction['alert_details'] = "Suspicious transaction pattern identified"
            # Make amount slightly different than normal
            transaction['amount'] *= random.uniform(0.8, 1.2)
        
        return transaction

    def _get_outlier_amount(self, base_amount: float, account_type: str) -> float:
        """Generate an outlier amount based on account type and base amount."""
        multiplier = random.uniform(3, 5)  # Generate 3-5x normal amount
        outlier_amount = base_amount * multiplier
        
        # Cap the amount based on account type
        max_amounts = {
            'retail': 100000,
            'corporate': 500000,
            'investment': 1000000
        }
        max_amount = max_amounts.get(account_type, max_amounts['retail'])
        
        return min(outlier_amount, max_amount)

    def _select_counterparty(self, account_id: str, account_map: dict) -> tuple:
        """Select a counterparty for the transaction."""
        other_accounts = [acc for acc_id, acc in account_map.items() 
                         if acc_id != account_id and acc['currency'] == account_map[account_id]['currency']]
        
        if other_accounts:
            counterparty = random.choice(other_accounts)
            counterparty_country = counterparty.get('custodian_country', random.choice(self.COUNTRIES))
        else:
            counterparty = {
                'account_number': fake.bothify(text='#########'),
                'custodian_bank': fake.company(),
                'entity_id': None,
                'entity_type': None
            }
            counterparty_country = random.choice(self.COUNTRIES)
        
        return counterparty, counterparty_country

    def _check_screening_alerts(self, country: str) -> dict:
        """Check if transaction should trigger alerts based on country."""
        screening_alert = False
        alert_details = None
        risk_score = random.randint(1, 100)
        
        if country in self.HIGH_RISK_COUNTRIES:
            screening_alert = random.random() < 0.7
            if screening_alert:
                alert_details = f"Transaction involves high-risk country: {country}"
                risk_score = random.randint(70, 100)
        
        return {
            'screening_alert': screening_alert,
            'alert_details': alert_details,
            'risk_score': risk_score
        }

    async def _generate_frequency_transactions(
        self, account: dict, account_map: dict, num_transactions: int,
        start_dt: datetime, end_dt: datetime, frequency: str
    ) -> List[dict]:
        """Generate transactions for a specific frequency."""
        transactions = []
        
        for _ in range(num_transactions):
            txn_date = self._get_transaction_date(frequency, start_dt, end_dt)
            is_debit = random.choice([True, False])
            
            counterparty, counterparty_country = self._select_counterparty(account, account_map)
            amount = self._generate_amount(frequency)
            
            screening_data = self._check_screening_alerts(counterparty_country)
            
            try:
                transaction = Transaction(
                    transaction_id=str(uuid.uuid4()),
                    transaction_type=random.choice(list(TransactionType)),
                    transaction_date=txn_date.strftime('%Y-%m-%d'),
                    amount=amount,
                    currency=account['currency'],
                    status=TransactionStatus.COMPLETED,
                    is_debit=is_debit,
                    
                    account_id=account['account_id'],
                    counterparty_account=counterparty['account_number'],
                    counterparty_name=fake.company(),
                    counterparty_bank=counterparty.get('custodian_bank', fake.company()),
                    
                    entity_id=account['entity_id'],
                    entity_type=account['entity_type'],
                    counterparty_entity_name=fake.company(),
                    
                    originating_country=account.get('custodian_country', random.choice(self.COUNTRIES)) if is_debit else counterparty_country,
                    destination_country=counterparty_country if is_debit else account.get('custodian_country', random.choice(self.COUNTRIES)),
                    
                    purpose=random.choice([
                        'Payment for services', 'Goods purchase', 'Investment',
                        'Transfer', 'Subscription', 'Loan payment'
                    ]),
                    reference_number=fake.bothify(text='TX#########'),
                    
                    **screening_data,
                    
                    processing_fee=round(amount * 0.001, 2) if random.random() < 0.8 else None,
                    exchange_rate=round(random.uniform(0.8, 1.2), 4) if random.random() < 0.2 else None,
                    value_date=(txn_date + timedelta(days=1)).strftime('%Y-%m-%d') if random.random() < 0.3 else None,
                    
                    batch_id=str(uuid.uuid4()) if random.random() < 0.3 else None,
                    check_number=fake.bothify(text='########') if TransactionType.CHECK else None,
                    wire_reference=fake.bothify(text='WIRE########') if TransactionType.WIRE else None
                )
                transactions.append(transaction.model_dump())
            except Exception as e:
                print(f"Error generating transaction: {e}")
                continue
        
        return transactions

    def _get_transaction_date(self, frequency: str, start_dt: datetime, end_dt: datetime) -> datetime:
        """Generate appropriate transaction date based on frequency."""
        if frequency == 'daily':
            return fake.date_time_between(start_dt, end_dt)
        elif frequency == 'weekly':
            date = fake.date_time_between(start_dt, end_dt)
            while date.weekday() > 4:  # Skip weekends
                date = fake.date_time_between(start_dt, end_dt)
            return date
        else:  # monthly/quarterly
            date = fake.date_time_between(start_dt, end_dt)
            day = random.choice([*range(1, 6), *range(15, 21)])
            return date.replace(day=day)

    def _generate_amount(self, frequency: str) -> float:
        """Generate appropriate amount based on frequency."""
        base_ranges = {
            'daily': (100, 1000),
            'weekly': (1000, 10000),
            'monthly': (5000, 50000),
            'quarterly': (10000, 100000)
        }
        min_amount, max_amount = base_ranges.get(frequency, base_ranges['daily'])
        base_amount = random.uniform(min_amount, max_amount)
        return round(base_amount * random.uniform(0.8, 1.2), 2)
