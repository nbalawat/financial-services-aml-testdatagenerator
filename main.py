#!/usr/bin/env python3
import os
import asyncio
import argparse
from dotenv import load_dotenv
from aml_monitoring.generate_test_data import TestDataGenerator
from aml_monitoring.db_handlers import DatabaseManager

async def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='AML Transaction Monitoring Data Generator')
    parser.add_argument('--num-institutions', type=int, default=2,
                      help='Number of institutions to generate (default: 2)')
    parser.add_argument('--output-dir', type=str, default='test_data',
                      help='Directory to store output CSV files (default: test_data)')
    args = parser.parse_args()

    # Load environment variables from config directory
    load_dotenv('config/.env')

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Initialize database handlers
    print("Initializing database connections...")
    db_manager = DatabaseManager()
    await db_manager.initialize_postgres()
    await db_manager.neo4j_handler.initialize()

    print("Cleaning up PostgreSQL database...")
    await db_manager.cleanup_postgres()
    print("Successfully cleaned up PostgreSQL database")

    print("Cleaning up Neo4j database...")
    await db_manager.cleanup_neo4j()
    print("Successfully cleaned up Neo4j database")

    print("Generating test data...")
    print(f"Generating data for {args.num_institutions} institutions...")
    
    # Generate data
    generator = TestDataGenerator()
    data = await generator.generate_all_data(args.num_institutions)

    # Save to CSV files
    print("\nSaving to CSV files...")
    for entity_type, entity_data in data.items():
        if entity_data is not None and not entity_data.empty:
            csv_path = os.path.join(args.output_dir, f"{entity_type}.csv")
            entity_data.to_csv(csv_path, index=False)
            print(f"Saved {entity_type} to {csv_path}")

    # Save to databases
    print("\nSaving to PostgreSQL...")
    await db_manager.save_to_postgres(data)

    print("\nSaving to Neo4j...")
    await db_manager.save_to_neo4j(data)

    # Close database connections
    await db_manager.postgres_handler.close()
    await db_manager.neo4j_handler.close()

    print("\nData generation and saving complete!")

if __name__ == '__main__':
    asyncio.run(main())
