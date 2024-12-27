"""PostgreSQL database handler for AML monitoring flows."""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncpg
from .base import FlowDatabaseHandler, FlowDatabaseError

class PostgresFlowHandler(FlowDatabaseHandler):
    """Handler for PostgreSQL database operations specific to AML monitoring flows."""

    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the handler with connection parameters.
        
        Args:
            connection_params: Dictionary containing:
                - host: database host
                - port: database port
                - user: database user
                - password: database password
                - database: database name
        """
        self.connection_params = connection_params
        self.pool = None

    async def initialize(self):
        """Initialize the connection pool."""
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(**self.connection_params)
            except Exception as e:
                raise FlowDatabaseError(f"Failed to initialize database pool: {str(e)}")

    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()

    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return the results asynchronously."""
        if not self.pool:
            await self.initialize()
            
        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(query)
                return [dict(row) for row in results]
        except Exception as e:
            raise FlowDatabaseError(f"Error executing query: {str(e)}")

    def execute_query_sync(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw SQL query synchronously."""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.execute_query(query))

    async def execute_query_batch(self, queries: List[str]) -> List[List[Dict[str, Any]]]:
        """Execute multiple SQL queries in parallel."""
        if not self.pool:
            await self.initialize()
            
        try:
            async with self.pool.acquire() as conn:
                tasks = [conn.fetch(query) for query in queries]
                results = await asyncio.gather(*tasks)
                return [[dict(row) for row in result] for result in results]
        except Exception as e:
            raise FlowDatabaseError(f"Error executing batch queries: {str(e)}")

    async def get_transaction_details(self, transaction_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific transaction."""
        query = """
            SELECT 
                t.*,
                s.legal_name as sender_name,
                r.legal_name as receiver_name
            FROM transactions t
            LEFT JOIN entities s ON t.sender_id = s.entity_id
            LEFT JOIN entities r ON t.receiver_id = r.entity_id
            WHERE t.transaction_id = $1
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, transaction_id)
                return dict(result) if result else {}
        except Exception as e:
            raise FlowDatabaseError(f"Error fetching transaction details: {str(e)}")

    async def get_customer_profile(self, customer_id: str) -> Dict[str, Any]:
        """Get customer profile information."""
        query = """
            SELECT 
                e.*,
                c.risk_rating,
                c.kyc_status,
                c.last_review_date
            FROM entities e
            JOIN customers c ON e.entity_id = c.customer_id
            WHERE e.entity_id = $1
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, customer_id)
                return dict(result) if result else {}
        except Exception as e:
            raise FlowDatabaseError(f"Error fetching customer profile: {str(e)}")

    async def get_alert_details(self, alert_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific alert."""
        query = """
            SELECT 
                a.*,
                e.legal_name as entity_name,
                e.entity_type
            FROM alerts a
            JOIN entities e ON a.entity_id = e.entity_id
            WHERE a.alert_id = $1
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, alert_id)
                return dict(result) if result else {}
        except Exception as e:
            raise FlowDatabaseError(f"Error fetching alert details: {str(e)}")

    async def get_related_transactions(
        self, entity_id: str, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get transactions related to a specific entity within a date range."""
        query = """
            SELECT t.*
            FROM transactions t
            WHERE (t.sender_id = $1 OR t.receiver_id = $1)
            AND t.transaction_date BETWEEN $2 AND $3
            ORDER BY t.transaction_date DESC
        """
        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(query, entity_id, start_date, end_date)
                return [dict(row) for row in results]
        except Exception as e:
            raise FlowDatabaseError(f"Error fetching related transactions: {str(e)}")

    async def get_entity_relationships(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get relationships for a specific entity."""
        query = """
            SELECT 
                r.*,
                e1.legal_name as from_entity_name,
                e2.legal_name as to_entity_name
            FROM entity_relationships r
            JOIN entities e1 ON r.from_entity_id = e1.entity_id
            JOIN entities e2 ON r.to_entity_id = e2.entity_id
            WHERE r.from_entity_id = $1 OR r.to_entity_id = $1
        """
        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(query, entity_id)
                return [dict(row) for row in results]
        except Exception as e:
            raise FlowDatabaseError(f"Error fetching entity relationships: {str(e)}")

    async def save_investigation_result(self, investigation_id: str, result: Dict[str, Any]) -> None:
        """Save the results of an investigation."""
        query = """
            INSERT INTO investigation_results (
                investigation_id, result_data, created_at
            ) VALUES ($1, $2, NOW())
            ON CONFLICT (investigation_id) 
            DO UPDATE SET result_data = $2, updated_at = NOW()
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, investigation_id, result)
        except Exception as e:
            raise FlowDatabaseError(f"Error saving investigation result: {str(e)}")

    async def update_alert_status(self, alert_id: str, status: str, notes: Optional[str] = None) -> None:
        """Update the status of an alert."""
        query = """
            UPDATE alerts 
            SET status = $1, 
                notes = COALESCE($2, notes),
                updated_at = NOW()
            WHERE alert_id = $3
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, status, notes, alert_id)
        except Exception as e:
            raise FlowDatabaseError(f"Error updating alert status: {str(e)}")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
