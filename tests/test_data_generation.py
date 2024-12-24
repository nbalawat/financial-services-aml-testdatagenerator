import os
import sys
from dotenv import load_dotenv
from aml_monitoring.generate_test_data import TestDataGenerator
from aml_monitoring.db_handlers import DatabaseManager

def main():
    # Load environment variables from config directory
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
    load_dotenv(os.path.join(config_dir, '.env'))

    # Create data directory if it doesn't exist
    os.makedirs('test_data', exist_ok=True)

    # Initialize database handlers
    db_manager = DatabaseManager()
    
    try:
        # Clean up databases before generating new data
        db_manager.cleanup_databases()
        
        # Initialize data generator
        generator = TestDataGenerator()
        
        # Generate test data for 2 institutions
        print("Generating test data...")
        data = generator.generate_all_data(num_institutions=2)
        
        # Save to CSV files
        print("\nSaving to CSV files...")
        for table_name, df in data.items():
            csv_path = f'test_data/{table_name}.csv'
            df.to_csv(csv_path, index=False)
            print(f"Saved {table_name} to {csv_path}")
        
        # Save data to PostgreSQL
        print("\nSaving to PostgreSQL...")
        db_manager.postgres._create_tables()  # Create tables if they don't exist
        db_manager.save_data(data)
        
    finally:
        # Close database connections
        db_manager.neo4j.close()
        db_manager.close()
    
    print("\nData generation and saving complete!")

if __name__ == "__main__":
    main()
