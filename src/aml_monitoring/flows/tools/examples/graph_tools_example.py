"""Example usage of Graph tools for AML monitoring."""

import os
import asyncio
from typing import List, Dict, Any
from ..graph_tools import (
    TextToCypherTool,
    GraphDataDictionary,
    AsyncGraphQueryTool,
    AsyncGraphPatternTool,
    AsyncGraphBatchQueryTool
)

async def main():
    """Demonstrate graph tools usage."""
    # Initialize tools
    graph_dictionary = GraphDataDictionary("path/to/graph_schema.csv")
    db_handler = FlowDatabaseHandler()  # Initialize your database handler
    
    # Create base Cypher tool
    text_to_cypher = TextToCypherTool(graph_dictionary, db_handler)
    
    # Create specialized tools
    query_tool = AsyncGraphQueryTool(text_to_cypher)
    pattern_tool = AsyncGraphPatternTool(text_to_cypher)
    batch_tool = AsyncGraphBatchQueryTool(text_to_cypher)
    
    # Example 1: Query graph
    print("\n=== Example 1: Query graph ===")
    query = "Find all customers who made transactions over $10,000 to high-risk countries"
    results = await query_tool._arun(query)
    print(f"High-risk transactions: {results}")
    
    # Example 2: Analyze patterns
    print("\n=== Example 2: Analyze patterns ===")
    pattern_query = "Identify transaction patterns that suggest money laundering"
    patterns = await pattern_tool._arun(pattern_query)
    print(f"Suspicious patterns: {patterns}")
    
    # Example 3: Batch queries
    print("\n=== Example 3: Batch queries ===")
    queries = [
        "Find customers with multiple high-value transactions",
        "Show transaction paths to known high-risk entities",
        "Identify circular transaction patterns"
    ]
    batch_results = await batch_tool._arun(queries)
    print("\nBatch query results:")
    for i, result in enumerate(batch_results):
        print(f"\nQuery {i+1}: {queries[i]}")
        print(f"Results: {result}")
    
    # Example 4: Graph schema information
    print("\n=== Example 4: Graph schema information ===")
    print("\nNode types:")
    for node_type, info in graph_dictionary.nodes.items():
        print(f"\n{node_type}:")
        print(f"Labels: {', '.join(info['labels'])}")
        print(f"Properties: {[p['name'] for p in info['properties']]}")
        print(f"Description: {info['description']}")
    
    print("\nRelationship types:")
    for rel_type, info in graph_dictionary.relationships.items():
        print(f"\n{rel_type}:")
        print(f"From: {info['from_node']}")
        print(f"To: {info['to_node']}")
        print(f"Properties: {[p['name'] for p in info['properties']]}")

if __name__ == "__main__":
    asyncio.run(main())
