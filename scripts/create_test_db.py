"""Script to create test database for AML monitoring tests."""

import asyncio
import asyncpg
import logging
import os
from dotenv import load_dotenv

# Load environment variables from config/.env
load_dotenv('config/.env')

async def create_test_database():
    """Create test database and user for AML monitoring tests."""
    conn = None
    try:
        # Get postgres credentials from environment variables
        postgres_user = os.getenv('POSTGRES_USER')
        postgres_password = os.getenv('POSTGRES_PASSWORD')
        postgres_host = os.getenv('POSTGRES_HOST', 'localhost')
        postgres_port = os.getenv('POSTGRES_PORT', '5432')
        
        if not postgres_user or not postgres_password:
            raise ValueError("POSTGRES_USER and POSTGRES_PASSWORD must be set in config/.env")
        
        # First try connecting as the aml_user
        try:
            conn = await asyncpg.connect(
                host=postgres_host,
                port=int(postgres_port),
                user=postgres_user,
                password=postgres_password,
                database='postgres'
            )
            logging.info("Successfully connected as aml_user")
        except asyncpg.exceptions.InvalidCatalogNameError:
            # Database doesn't exist yet, this is expected
            pass
        except Exception as e:
            logging.warning(f"Could not connect as {postgres_user}: {str(e)}")
            logging.info("Attempting to connect as superuser...")
            
            # Try connecting as postgres superuser
            try:
                conn = await asyncpg.connect(
                    host=postgres_host,
                    port=int(postgres_port),
                    user='postgres',
                    database='postgres'
                )
                logging.info("Connected as postgres superuser")
                
                # Create aml_user if it doesn't exist
                await conn.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'aml_user') THEN
                            CREATE USER aml_user WITH PASSWORD 'aml_password';
                        END IF;
                    END
                    $$;
                """)
                logging.info("User aml_user created or already exists")
            except Exception as e:
                logging.error(f"Could not create user: {str(e)}")
                raise

        # Create test database if it doesn't exist
        try:
            await conn.execute("""
                CREATE DATABASE aml_monitoring_test
                    WITH 
                    OWNER = aml_user
                    ENCODING = 'UTF8'
                    CONNECTION LIMIT = -1;
            """)
            logging.info("Database aml_monitoring_test created")
        except asyncpg.exceptions.DuplicateDatabaseError:
            logging.info("Database aml_monitoring_test already exists")
        except Exception as e:
            logging.error(f"Error creating database: {str(e)}")
            raise

        # Grant privileges to aml_user
        try:
            await conn.execute("""
                GRANT ALL PRIVILEGES ON DATABASE aml_monitoring_test TO aml_user;
            """)
            logging.info("Privileges granted to aml_user")
        except Exception as e:
            logging.error(f"Error granting privileges: {str(e)}")
            raise

    except Exception as e:
        logging.error(f"Failed to create test database: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(create_test_database())
