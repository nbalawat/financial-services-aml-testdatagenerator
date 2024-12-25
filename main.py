#!/usr/bin/env python3
import os
import asyncio
import argparse
from dotenv import load_dotenv
from aml_monitoring.data_generator import DataGenerator
import uuid
import pandas as pd
from datetime import datetime, timedelta

async def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    
    # Data generation parameters
    group_gen = parser.add_argument_group('Data Generation Parameters')
    group_gen.add_argument('--num-institutions', type=int, default=5, 
                          help='Number of institutions to generate')
    group_gen.add_argument('--min-accounts', type=int, default=1,
                          help='Minimum number of accounts per institution')
    group_gen.add_argument('--max-accounts', type=int, default=10,
                          help='Maximum number of accounts per institution')
    group_gen.add_argument('--min-transactions', type=int, default=50,
                          help='Minimum number of transactions per account')
    group_gen.add_argument('--max-transactions', type=int, default=200,
                          help='Maximum number of transactions per account')
    group_gen.add_argument('--min-beneficial-owners', type=int, default=1,
                          help='Minimum number of beneficial owners per institution')
    group_gen.add_argument('--max-beneficial-owners', type=int, default=5,
                          help='Maximum number of beneficial owners per institution')
    group_gen.add_argument('--min-documents', type=int, default=2,
                          help='Minimum number of documents per entity')
    group_gen.add_argument('--max-documents', type=int, default=10,
                          help='Maximum number of documents per entity')
    group_gen.add_argument('--date-start', type=str, default='2020-01-01',
                          help='Start date for data generation (YYYY-MM-DD)')
    group_gen.add_argument('--date-end', type=str, default='2024-12-31',
                          help='End date for data generation (YYYY-MM-DD)')
    group_gen.add_argument('--high-risk-percentage', type=float, default=0.1,
                          help='Percentage of high-risk institutions (0.0-1.0)')
    
    # Database parameters
    group_db = parser.add_argument_group('Database Parameters')
    group_db.add_argument('--batch-size', type=int, default=1000,
                         help='Batch size for database operations')
    group_db.add_argument('--skip-postgres', action='store_true',
                         help='Skip PostgreSQL data generation')
    group_db.add_argument('--skip-neo4j', action='store_true',
                         help='Skip Neo4j data generation')
    
    # Utility parameters
    group_util = parser.add_argument_group('Utility Parameters')
    group_util.add_argument('--cleanup-only', action='store_true',
                           help='Only cleanup databases without generating new data')
    group_util.add_argument('--dry-run', action='store_true',
                           help='Generate data but do not save to databases')
    group_util.add_argument('--verbose', action='store_true',
                           help='Enable verbose output')
    
    args = parser.parse_args()

    # Load environment variables from config directory
    load_dotenv('config/.env')

    try:
        # Configure the data generator
        config = {
            'num_institutions': args.num_institutions,
            'min_accounts_per_institution': args.min_accounts,
            'max_accounts_per_institution': args.max_accounts,
            'min_transactions_per_account': args.min_transactions,
            'max_transactions_per_account': args.max_transactions,
            'min_beneficial_owners': args.min_beneficial_owners,
            'max_beneficial_owners': args.max_beneficial_owners,
            'min_documents': args.min_documents,
            'max_documents': args.max_documents,
            'date_start': datetime.strptime(args.date_start, '%Y-%m-%d'),
            'date_end': datetime.strptime(args.date_end, '%Y-%m-%d'),
            'high_risk_percentage': args.high_risk_percentage,
            'batch_size': args.batch_size,
            'skip_postgres': args.skip_postgres,
            'skip_neo4j': args.skip_neo4j,
            'dry_run': args.dry_run,
            'verbose': args.verbose
        }

        if not args.cleanup_only:
            print("\n=== Initializing Data Generator ===")
            print("\nConfiguration:")
            for key, value in config.items():
                print(f"- {key}: {value}")

            # Initialize the generator
            generator = DataGenerator(config)
            
            if not args.dry_run:
                print("\n=== Initializing Database Connections ===")
                await generator.initialize_db()

            print("\n=== Starting Data Generation ===")
            await generator.generate_all()
            
            print("\n=== Data Generation Complete ===")
            
        print("\n=== Cleaning Up ===")
        if not args.dry_run:
            await generator.close_db()
        print("✓ All connections closed")
        
    except Exception as e:
        print(f"\n❌ Error during data generation: {str(e)}")
        raise

if __name__ == '__main__':
    asyncio.run(main())
