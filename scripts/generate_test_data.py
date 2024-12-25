"""Script to generate test data for AML monitoring system."""

import os
import asyncio
import logging
from dotenv import load_dotenv
from aml_monitoring.data_generator import DataGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('test.env')

async def main():
    """Generate test data for AML monitoring system."""
    generator = None
    try:
        logger.info("Initializing databases...")
        generator = DataGenerator(config={
            'num_institutions': 5,
            'max_accounts_per_institution': 3,
            'min_transactions_per_account': 5,
            'max_transactions_per_account': 10,
            'max_beneficial_owners_per_institution': 2,
            'date_range': {
                'start': '2023-01-01',
                'end': '2024-12-25'
            }
        })
        await generator.initialize_db()

        # Clean existing data
        logger.info("Cleaning existing data...")
        await generator.postgres_handler.wipe_clean()
        await generator.neo4j_handler.wipe_clean()

        logger.info("Generating test data...")
        await generator.generate_all()

        logger.info("Data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating data: {str(e)}")
        raise
    finally:
        if generator:
            await generator.close_db()

if __name__ == "__main__":
    asyncio.run(main())
