import os
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text
from neo4j import AsyncGraphDatabase
from enum import Enum

class PostgresHandler:
    def __init__(self):
        """Initialize PostgreSQL handler."""
        self.engine = None
    
    async def initialize(self):
        """Initialize database connection."""
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        db = os.getenv('POSTGRES_DB', 'aml_monitoring')
        user = os.getenv('POSTGRES_USER', 'aml_user')
        password = os.getenv('POSTGRES_PASSWORD', 'aml_password')
        
        db_url = f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'
        self.engine = create_async_engine(db_url, echo=True)
        
        # Create tables if they don't exist
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id UUID PRIMARY KEY,
                    account_id UUID NOT NULL,
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
    
    async def save_transactions(self, transactions: List[Dict[str, Any]]):
        """Save transactions to PostgreSQL."""
        if not transactions:
            return
            
        # Convert timestamps to strings
        for txn in transactions:
            if isinstance(txn['transaction_date'], pd.Timestamp):
                txn['transaction_date'] = txn['transaction_date'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(txn['transaction_type'], Enum):
                txn['transaction_type'] = txn['transaction_type'].value
        
        # Convert transactions to SQL format
        values = []
        for txn in transactions:
            values.append({
                'transaction_id': txn['transaction_id'],
                'account_id': txn['account_id'],
                'transaction_date': txn['transaction_date'],
                'transaction_type': txn['transaction_type'],
                'amount': txn['amount'],
                'currency': txn['currency'],
                'is_debit': txn['is_debit'],
                'counterparty_name': txn.get('counterparty_name'),
                'counterparty_account': txn.get('counterparty_account'),
                'counterparty_country': txn.get('counterparty_country'),
                'screening_alert': txn.get('screening_alert', False),
                'risk_score': txn.get('risk_score'),
                'alert_details': txn.get('alert_details')
            })
        
        # Insert transactions
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO transactions (
                        transaction_id, account_id, transaction_date,
                        transaction_type, amount, currency, is_debit,
                        counterparty_name, counterparty_account,
                        counterparty_country, screening_alert,
                        risk_score, alert_details
                    ) VALUES (
                        :transaction_id, :account_id, :transaction_date,
                        :transaction_type, :amount, :currency, :is_debit,
                        :counterparty_name, :counterparty_account,
                        :counterparty_country, :screening_alert,
                        :risk_score, :alert_details
                    )
                """),
                values
            )
    
    async def close(self):
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()

class Neo4jHandler:
    def __init__(self):
        """Initialize Neo4j handler."""
        self.driver = None
    
    async def initialize(self):
        """Initialize database connection."""
        uri = os.getenv('NEO4J_URI', 'neo4j://localhost:7687')
        user = os.getenv('NEO4J_USER', 'neo4j')
        password = os.getenv('NEO4J_PASSWORD', 'password')
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        
        # Create constraints
        async with self.driver.session() as session:
            await session.run("""
                CREATE CONSTRAINT business_date_id IF NOT EXISTS
                FOR (d:BusinessDate) REQUIRE d.date IS UNIQUE
            """)
            
            await session.run("""
                CREATE CONSTRAINT institution_id IF NOT EXISTS
                FOR (i:Institution) REQUIRE i.institution_id IS UNIQUE
            """)
    
    async def save_transactions_rollup(self, transactions: List[Dict[str, Any]]):
        """Save rolled-up transaction data to Neo4j."""
        if not transactions:
            return
            
        # Convert transactions to DataFrame for aggregation
        df = pd.DataFrame(transactions)
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        # Group by date and calculate metrics
        daily_stats = df.groupby(['transaction_date', 'account_id']).agg({
            'amount': ['sum', 'count'],
            'screening_alert': ['sum'],  # Count of alerts
            'risk_score': ['mean']  # Average risk score
        }).reset_index()
        
        # Create Neo4j nodes and relationships
        async with self.driver.session() as session:
            for _, row in daily_stats.iterrows():
                # Create BusinessDate node and relationship
                await session.run("""
                    MERGE (d:BusinessDate {date: $date})
                    WITH d
                    MATCH (a:Account {account_id: $account_id})
                    MERGE (a)-[r:TRANSACTED_ON]->(d)
                    SET r.total_amount = $total_amount,
                        r.transaction_count = $transaction_count,
                        r.alert_count = $alert_count,
                        r.avg_risk_score = $avg_risk_score
                """, {
                    'date': str(row['transaction_date']),
                    'account_id': row['account_id'],
                    'total_amount': float(row[('amount', 'sum')]),
                    'transaction_count': int(row[('amount', 'count')]),
                    'alert_count': int(row[('screening_alert', 'sum')]),
                    'avg_risk_score': float(row[('risk_score', 'mean')])
                })
    
    async def close(self):
        """Close database connection."""
        if self.driver:
            await self.driver.close()


class DatabaseManager:
    def __init__(self):
        """Initialize database handlers."""
        self.postgres_handler = PostgresHandler()
        self.neo4j_handler = Neo4jHandler()

    async def initialize_postgres(self):
        """Initialize PostgreSQL database tables."""
        await self.postgres_handler.initialize()

    async def cleanup_postgres(self):
        """Clean up PostgreSQL database."""
        async with self.postgres_handler.engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS transactions"))

    async def cleanup_neo4j(self):
        """Clean up Neo4j database."""
        async with self.neo4j_handler.driver.session() as session:
            await session.run("MATCH (n) DETACH DELETE n")

    async def save_to_postgres(self, data: Dict[str, pd.DataFrame]):
        """Save data to PostgreSQL."""
        for entity_type, df in data.items():
            if not df.empty:
                print(f"Saving {entity_type} to PostgreSQL...")
                if entity_type == 'transactions':
                    await self.postgres_handler.save_transactions(df.to_dict('records'))
                else:
                    # TO DO: implement save_data for other entity types
                    pass

    async def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j."""
        for entity_type, df in data.items():
            if not df.empty:
                print(f"Saving {entity_type} to Neo4j...")
                if entity_type == 'transactions':
                    await self.neo4j_handler.save_transactions_rollup(df.to_dict('records'))
                else:
                    # TO DO: implement save_data for other entity types
                    pass

    async def save_data(self, data: Dict[str, pd.DataFrame]):
        """Save all dataframes to both PostgreSQL and Neo4j."""
        # Save to PostgreSQL
        await self.save_to_postgres(data)

        # Save to Neo4j
        await self.save_to_neo4j(data)

    async def close(self):
        """Close all database connections."""
        await self.postgres_handler.close()
        await self.neo4j_handler.close()
