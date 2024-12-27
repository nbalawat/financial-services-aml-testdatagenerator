"""Base database handler for AML monitoring flows."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime

class FlowDatabaseError(Exception):
    """Base exception for flow database errors."""
    pass

class FlowDatabaseHandler(ABC):
    """Abstract base class for flow database handlers."""

    @abstractmethod
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return the results asynchronously."""
        pass

    @abstractmethod
    def execute_query_sync(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw SQL query synchronously."""
        pass

    @abstractmethod
    async def execute_query_batch(self, queries: List[str]) -> List[List[Dict[str, Any]]]:
        """Execute multiple SQL queries in parallel."""
        pass

    @abstractmethod
    async def get_transaction_details(self, transaction_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific transaction."""
        pass

    @abstractmethod
    async def get_customer_profile(self, customer_id: str) -> Dict[str, Any]:
        """Get customer profile information."""
        pass

    @abstractmethod
    async def get_alert_details(self, alert_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific alert."""
        pass

    @abstractmethod
    async def get_related_transactions(self, entity_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get transactions related to a specific entity within a date range."""
        pass

    @abstractmethod
    async def get_entity_relationships(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get relationships for a specific entity."""
        pass

    @abstractmethod
    async def save_investigation_result(self, investigation_id: str, result: Dict[str, Any]) -> None:
        """Save the results of an investigation."""
        pass

    @abstractmethod
    async def update_alert_status(self, alert_id: str, status: str, notes: Optional[str] = None) -> None:
        """Update the status of an alert."""
        pass
