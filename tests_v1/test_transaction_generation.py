import asyncio
import os
import sys
import unittest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.aml_monitoring.transaction_generator import TransactionGenerator
from src.aml_monitoring.generate_test_data import TestDataGenerator
from src.aml_monitoring.db_handlers import Neo4jHandler, PostgresHandler

class TestTransactionGeneration(unittest.IsolatedAsyncioTestCase):
    """Test transaction generation functionality."""

    async def asyncSetUp(self):
        """Set up test environment."""
        self.num_test_institutions = 2
        self.test_data_generator = TestDataGenerator()

        # Set up SQLite database for testing
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        
        # Initialize PostgresHandler with test engine
        self.pg_handler = PostgresHandler()
        self.pg_handler.engine = self.engine  # Override the engine for testing

        # Create transactions table
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    transaction_date TIMESTAMP NOT NULL,
                    transaction_type VARCHAR(50) NOT NULL,
                    amount DECIMAL NOT NULL,
                    currency VARCHAR(3) NOT NULL,
                    is_debit BOOLEAN NOT NULL,
                    counterparty_name VARCHAR(255),
                    counterparty_account VARCHAR(50),
                    counterparty_country VARCHAR(2),
                    screening_alert BOOLEAN,
                    risk_score INTEGER,
                    alert_details TEXT
                )
            """))

        # Set up Neo4j mocking
        self.mock_session = AsyncMock()
        self.mock_session.__aenter__ = AsyncMock(return_value=self.mock_session)
        self.mock_session.__aexit__ = AsyncMock()
        self.mock_session.run = AsyncMock()

        self.mock_driver = AsyncMock()
        self.mock_driver.session = MagicMock(return_value=self.mock_session)

        # Initialize Neo4jHandler and set mocked driver
        self.neo4j_handler = Neo4jHandler()
        self.neo4j_handler.driver = self.mock_driver  # Override the driver for testing

    async def asyncTearDown(self):
        """Clean up test environment."""
        if self.engine:
            await self.engine.dispose()

    async def test_transaction_generation(self):
        """Test basic transaction generation."""
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)

        # Check if transactions were generated
        self.assertFalse(data['transactions'].empty)
        self.assertGreater(len(data['transactions']), 0)

        # Check required columns
        required_columns = [
            'transaction_id', 'account_id', 'transaction_date',
            'transaction_type', 'amount', 'currency', 'is_debit'
        ]
        for col in required_columns:
            self.assertIn(col, data['transactions'].columns)

        # Check date range
        two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
        dates = pd.to_datetime(data['transactions']['transaction_date'])
        self.assertTrue(all(dates >= two_years_ago))

    async def test_database_storage(self):
        """Test storing transactions in databases."""
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)

        # Store in PostgreSQL
        await self.pg_handler.save_transactions(data['transactions'].to_dict('records'))

        # Store in Neo4j
        await self.neo4j_handler.save_transactions_rollup(data['transactions'].to_dict('records'))

        # Verify Neo4j calls
        self.mock_session.run.assert_called()

    async def test_transaction_patterns(self):
        """Test transaction pattern generation."""
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)

        # Check for transaction patterns
        transactions = data['transactions']
        self.assertFalse(transactions.empty)

        # Check for batch transactions
        batch_transactions = transactions[transactions['batch_id'].notna()]
        self.assertTrue(len(batch_transactions) > 0)

        # Check for recurring transactions
        accounts = transactions['account_id'].unique()
        for account in accounts:
            account_txns = transactions[transactions['account_id'] == account]
            amounts = account_txns['amount'].value_counts()
            # Check if there are any recurring amounts
            self.assertTrue(any(count > 1 for count in amounts))

    async def test_outlier_generation(self):
        """Test outlier transaction generation."""
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)

        # Check for outlier transactions
        transactions = data['transactions']
        outliers = transactions[transactions['screening_alert'] == True]
        self.assertGreater(len(outliers), 0)

        # Check outlier details
        for _, outlier in outliers.iterrows():
            self.assertIsNotNone(outlier['alert_details'])
            self.assertIn('large', outlier['alert_details'].lower())
            self.assertGreater(outlier['risk_score'], 50)

    async def test_transaction_generation(self):
        """Test basic transaction generation."""
        # Generate base data
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)
        
        # Verify transactions were generated
        self.assertIn('transactions', data)
        transactions_df = data['transactions']
        self.assertFalse(transactions_df.empty)
        
        # Check transaction properties
        required_columns = [
            'transaction_id', 'transaction_type', 'amount', 'currency',
            'account_id', 'is_debit'
        ]
        for col in required_columns:
            self.assertIn(col, transactions_df.columns)
        
        # Verify transaction dates are within expected range
        dates = pd.to_datetime(transactions_df['transaction_date'])
        current_date = pd.Timestamp('2024-12-24')  # Fixed current date
        two_years_ago = current_date - pd.Timedelta(days=730)
        self.assertTrue(all(dates >= two_years_ago))
        self.assertTrue(all(dates <= current_date))

    async def test_database_storage(self):
        """Test storing transactions in databases."""
        # Generate data
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)
        
        # Save to PostgreSQL
        await self.pg_handler.save_transactions(data['transactions'].to_dict('records'))
        
        # Save to Neo4j
        await self.neo4j_handler.save_transactions_rollup(data['transactions'].to_dict('records'))
        
        # Verify PostgreSQL storage
        transactions_df = data['transactions']
        postgres_count = await self._get_postgres_transaction_count()
        self.assertEqual(postgres_count, len(transactions_df))
        
        # Verify Neo4j calls were made
        self.mock_session.run.assert_called()

    async def test_transaction_patterns(self):
        """Test transaction pattern generation."""
        # Generate data
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)
        transactions_df = data['transactions']
        
        # Test frequency patterns
        daily_pattern = transactions_df.groupby('transaction_date').size()
        self.assertTrue(len(daily_pattern) > 0)
        
        # Test amount distributions
        amount_stats = transactions_df['amount'].describe()
        self.assertTrue(amount_stats['min'] >= 100)  # Minimum transaction amount
        self.assertTrue(amount_stats['max'] <= 1000000)  # Maximum including outliers
        
        # Test risk scoring
        risk_scores = transactions_df[transactions_df['screening_alert']]['risk_score']
        self.assertTrue(all(risk_scores >= 70))  # High-risk transactions
    
    async def test_outlier_generation(self):
        """Test outlier transaction generation."""
        # Generate data
        data = await self.test_data_generator.generate_all_data(self.num_test_institutions)
        transactions_df = data['transactions']
        
        # Find outlier transactions
        outliers = transactions_df[
            (transactions_df['amount'] > 100000) |  # Large amounts
            (transactions_df['screening_alert'] == True)  # Risk alerts
        ]
        
        self.assertTrue(len(outliers) > 0)
        
        # Verify outlier properties
        for _, outlier in outliers.iterrows():
            if outlier['amount'] > 100000:
                self.assertIn('large', outlier['alert_details'].lower())
            if outlier['screening_alert']:
                self.assertTrue(outlier['risk_score'] >= 70)

    async def _get_postgres_transaction_count(self) -> int:
        """Get transaction count from PostgreSQL."""
        async with self.engine.connect() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM transactions"))
            return result.scalar()

if __name__ == '__main__':
    unittest.main()
