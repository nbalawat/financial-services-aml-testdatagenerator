"""Example script demonstrating how to use the data generator."""

import asyncio
import os
import sys
from pathlib import Path
import logging

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.aml_monitoring.data_generator import generate_test_data
from src.aml_monitoring.database import PostgresHandler, Neo4jHandler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    # Configuration for data generation
    config = {
        'num_institutions': 100,  # Number of institutions to generate
        'min_transactions_per_account': 50,  # Minimum transactions per account
        'max_transactions_per_account': 200,  # Maximum transactions per account
        'batch_size': 1000,  # Number of records to persist at once
    }
    
    try:
        # Initialize database handlers
        postgres = PostgresHandler()
        neo4j = Neo4jHandler()
        
        # Connect to databases
        print("\nConnecting to databases...")
        await postgres.connect()
        await neo4j.connect()
        
        try:
            # Create/validate schema
            print("\nCreating/validating database schemas...")
            await postgres.create_schema()
            await neo4j.create_schema()
            
            # Generate and persist data
            print("\nGenerating and persisting data...")
            await generate_test_data(config)
            
            print("\nData generation and persistence completed successfully!")
            
        finally:
            # Clean up database connections
            await postgres.disconnect()
            await neo4j.disconnect()
            
    except Exception as e:
        print(f"\nError during data generation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
