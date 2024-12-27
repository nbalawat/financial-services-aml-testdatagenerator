"""Example usage of API tools for AML monitoring."""

import os
import asyncio
from typing import Dict, Any
from ..api_tools import (
    SwaggerAPITool,
    AsyncAPIBatchTool,
    SwaggerParser
)

async def main():
    """Demonstrate API tools usage."""
    # Initialize API tool
    api_tool = SwaggerAPITool(
        swagger_url="https://api.example.com/swagger.json",
        base_url="https://api.example.com",
        auth_header={"Authorization": f"Bearer {os.environ.get('API_TOKEN')}"},
        rate_limit=10
    )
    
    # Create batch tool
    batch_tool = AsyncAPIBatchTool(api_tool)
    
    # Example 1: Parse Swagger documentation
    print("\n=== Example 1: Parse Swagger documentation ===")
    endpoint = "/transactions/search"
    method = "post"
    endpoint_info = api_tool.parser.get_endpoint_info(endpoint, method)
    print(f"Endpoint info: {endpoint_info}")
    
    required_params = api_tool.parser.get_required_parameters(endpoint_info)
    print(f"Required parameters: {required_params}")
    
    # Example 2: Validate parameters
    print("\n=== Example 2: Validate parameters ===")
    params = {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "min_amount": 10000
    }
    is_valid = api_tool.parser.validate_parameters(endpoint, method, params)
    print(f"Parameters valid: {is_valid}")
    
    # Example 3: Execute batch queries
    print("\n=== Example 3: Execute batch queries ===")
    queries = [
        "Find transactions over $50,000",
        "Show suspicious activity reports",
        "List high-risk customers"
    ]
    batch_results = await batch_tool._arun(queries)
    print("\nBatch query results:")
    for i, result in enumerate(batch_results):
        print(f"\nQuery {i+1}: {queries[i]}")
        print(f"Results: {result}")
    
    # Example 4: Error handling
    print("\n=== Example 4: Error handling ===")
    try:
        invalid_params = {
            "start_date": "invalid-date",
            "min_amount": "not-a-number"
        }
        api_tool.parser.validate_parameters(endpoint, method, invalid_params)
    except Exception as e:
        print(f"Caught expected validation error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
