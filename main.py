#!/usr/bin/env python3
import os
import asyncio
import argparse
from dotenv import load_dotenv
from aml_monitoring.generate_test_data import TestDataGenerator
from aml_monitoring.db_handlers import DatabaseManager
import uuid
import pandas as pd
from datetime import datetime

async def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    parser.add_argument('--num-institutions', type=int, default=5, help='Number of institutions to generate')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for database operations')
    args = parser.parse_args()

    # Load environment variables from config directory
    load_dotenv('config/.env')

    # Initialize database manager
    db_manager = DatabaseManager()
    
    try:
        print("\n=== Initializing Databases ===")
        print("Connecting to PostgreSQL...")
        await db_manager.initialize_postgres()
        print("Connecting to Neo4j...")
        await db_manager.neo4j_handler.initialize()
        
        print("\n=== Cleaning Up Databases ===")
        print("Cleaning PostgreSQL tables...")
        await db_manager.cleanup_postgres()
        print("Cleaning Neo4j database...")
        await db_manager.cleanup_neo4j()

        print("\n=== Generating Test Data ===")
        generator = TestDataGenerator()
        
        print(f"\nGenerating data for {args.num_institutions} institutions...")
        data = await generator.generate_all_data(args.num_institutions)
        
        # Convert all data for database storage
        print("\n=== Processing Data for Databases ===")
        processed_data = {}
        
        for key, df in data.items():
            if not df.empty:
                # Convert DataFrame to proper format
                converted_df = df.copy()
                for col in df.columns:
                    # Convert UUID objects to strings
                    if isinstance(df[col].iloc[0], uuid.UUID):
                        converted_df[col] = df[col].astype(str)
                    # Convert datetime objects to ISO format strings
                    elif isinstance(df[col].iloc[0], (pd.Timestamp, datetime)):
                        converted_df[col] = df[col].apply(lambda x: x.isoformat())
                processed_data[key] = converted_df
        
        # Save all data to PostgreSQL
        print("\n=== Saving Data to PostgreSQL ===")
        print(f"Found {len(processed_data)} data collections to process:")
        for key, df in processed_data.items():
            print(f"- {key}: {len(df)} records")
        
        await db_manager.save_to_postgres(processed_data)
        print("✓ PostgreSQL data saved successfully")
        
        # Save all data to Neo4j
        print("\n=== Saving Data to Neo4j ===")
        await db_manager.save_to_neo4j(processed_data)
        print("✓ Neo4j data saved successfully")
        
        print("\n=== Data Generation Complete ===")
        print("Summary of generated data:")
        for key, df in data.items():
            print(f"- {key}: {len(df)} records")
        
    except Exception as e:
        print(f"\n❌ Error during data generation: {str(e)}")
        raise
    finally:
        print("\n=== Cleaning Up ===")
        print("Closing database connections...")
        await db_manager.postgres_handler.close()
        await db_manager.neo4j_handler.close()
        print("✓ All connections closed")

if __name__ == '__main__':
    asyncio.run(main())
