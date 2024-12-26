#!/usr/bin/env python3
import os
import asyncio
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv
from aml_monitoring.data_generator import DataGenerator
from aml_monitoring.database import PostgresHandler, Neo4jHandler

# Configure simple logging
logging.basicConfig(
    level=logging.WARNING,  # Set default level to WARNING to reduce noise
    format='%(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Keep main logger at INFO

# Disable other loggers
for name in ['aml_monitoring', 'asyncio', 'neo4j']:
    logging.getLogger(name).setLevel(logging.WARNING)

def parse_args():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate test data for AML monitoring')
    
    # Data generation parameters
    group_gen = parser.add_argument_group('Data Generation Parameters')
    group_gen.add_argument('--num-institutions', type=int, default=5, 
                          help='Number of institutions to generate')
    group_gen.add_argument('--max-accounts', type=int, default=3,
                          help='Maximum number of accounts per institution')
    group_gen.add_argument('--min-transactions', type=int, default=5,
                          help='Minimum number of transactions per account')
    group_gen.add_argument('--max-transactions', type=int, default=10,
                          help='Maximum number of transactions per account')
    group_gen.add_argument('--max-beneficial-owners', type=int, default=2,
                          help='Maximum number of beneficial owners per institution')
    group_gen.add_argument('--date-start', type=str, default='2023-01-01',
                          help='Start date for data generation (YYYY-MM-DD)')
    group_gen.add_argument('--date-end', type=str, default='2024-12-25',
                          help='End date for data generation (YYYY-MM-DD)')
    
    # Batch processing parameters
    group_batch = parser.add_argument_group('Batch Processing Parameters')
    group_batch.add_argument('--institutions-per-batch', type=int, default=100,
                          help='Number of institutions to process in each batch')
    group_batch.add_argument('--transactions-per-batch', type=int, default=10000,
                          help='Maximum number of transactions to save in each batch')
    
    # Utility parameters
    group_util = parser.add_argument_group('Utility Parameters')
    group_util.add_argument('--cleanup-only', action='store_true',
                           help='Only cleanup databases without generating new data')
    group_util.add_argument('--env-file', type=str, default='test.env',
                           help='Environment file path')
    group_util.add_argument('--verbose', action='store_true',
                           help='Enable verbose output')
    
    return parser.parse_args()

async def async_main():
    args = parse_args()

    # Load environment variables
    load_dotenv(args.env_file)

    generator = None
    try:
        # Configure database handlers
        postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'aml_monitoring'),
            'user': os.getenv('POSTGRES_USER', 'aml_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'aml_password')
        }
        
        neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
        neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')

        postgres_handler = PostgresHandler(postgres_config)
        neo4j_handler = Neo4jHandler(neo4j_uri, neo4j_user, neo4j_password)

        if args.cleanup_only:
            logger.info("Cleaning databases...")
            await postgres_handler.initialize()
            await neo4j_handler.initialize()
            await postgres_handler.wipe_clean()
            await neo4j_handler.wipe_clean()
            logger.info("Done")
            return

        # Configure the data generator
        config = {
            'num_institutions': args.num_institutions,
            'max_accounts_per_institution': args.max_accounts,
            'min_transactions_per_account': args.min_transactions,
            'max_transactions_per_account': args.max_transactions,
            'max_beneficial_owners_per_institution': args.max_beneficial_owners,
            'date_range': {
                'start': args.date_start,
                'end': args.date_end
            },
            'batch_size': {
                'institutions': args.institutions_per_batch,
                'transactions': args.transactions_per_batch
            }
        }

        logger.info(f"Generating {args.num_institutions} institutions")
        
        generator = DataGenerator(config, postgres_handler, neo4j_handler)
        await generator.initialize_db()
        await postgres_handler.wipe_clean()
        await neo4j_handler.wipe_clean()
        await generator.generate_all()
        
        logger.info("Done")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        if generator:
            await generator.close_db()

def main():
    asyncio.run(async_main())

if __name__ == '__main__':
    main()
