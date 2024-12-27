"""Example usage of SQL tools for AML monitoring."""

import os
import asyncio
from typing import List, Dict, Any
from ..sql_tools import (
    TextToSQLTool,
    DataDictionary,
    QueryTransactionsTool,
    AsyncQueryTransactionsTool,
    AsyncAnalyzePatternsTool,
    AsyncBatchQueryTool
)

async def main():
    """Demonstrate SQL tools usage."""
    # Initialize tools
    data_dictionary = DataDictionary("path/to/data_dictionary.csv")
    db_handler = FlowDatabaseHandler()  # Initialize your database handler
    
    # Create base SQL tool
    text_to_sql = TextToSQLTool(data_dictionary, db_handler)
    
    # Create specialized tools
    query_tool = QueryTransactionsTool(text_to_sql)
    async_query_tool = AsyncQueryTransactionsTool(text_to_sql)
    pattern_tool = AsyncAnalyzePatternsTool(text_to_sql)
    batch_tool = AsyncBatchQueryTool(text_to_sql)
    
    # Example 1: Query transactions (Sync)
    print("\n=== Example 1: Query transactions (Sync) ===")
    query = "Find all transactions above $10,000 in the last week"
    results = query_tool._run(query)
    print(f"High value transactions: {results}")
    
    # Example 2: Query transactions (Async)
    print("\n=== Example 2: Query transactions (Async) ===")
    query = "Show me suspicious transactions with round dollar amounts"
    results = await async_query_tool._arun(query)
    print(f"Suspicious transactions: {results}")
    
    # Example 3: Analyze patterns
    print("\n=== Example 3: Analyze patterns ===")
    pattern_query = "Find customers with unusual transaction patterns in the last month"
    patterns = await pattern_tool._arun(pattern_query)
    print(f"Transaction patterns: {patterns}")
    
    # Example 4: Batch queries
    print("\n=== Example 4: Batch queries ===")
    queries = [
        "Find transactions over $50,000",
        "Show wire transfers to high-risk countries",
        "List customers with multiple large cash deposits"
    ]
    batch_results = await batch_tool._arun(queries)
    print("\nBatch query results:")
    for i, result in enumerate(batch_results):
        print(f"\nQuery {i+1}: {queries[i]}")
        print(f"Results: {result}")

if __name__ == "__main__":
    asyncio.run(main())
