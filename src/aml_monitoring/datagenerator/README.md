# AML Transaction Monitoring Data Generator

A comprehensive system for generating and managing test data for Anti-Money Laundering (AML) transaction monitoring. This system helps financial institutions test and validate their AML monitoring systems by generating realistic customer data, transactions, and risk assessments.

## Features

- Generate realistic test data for:
  - Financial institutions and their subsidiaries
  - Customer profiles and relationships
  - Beneficial owners and their relationships
  - Account information and transactions
  - Risk assessments and compliance events
  - Documents and jurisdiction presence
- Dual database support:
  - PostgreSQL for relational data storage
  - Neo4j for graph-based relationship analysis and traversal
- Rich relationship modeling:
  - Transaction flows (SENT, RECEIVED, TRANSACTED)
  - Entity ownership (OWNED_BY, HAS_SUBSIDIARY)
  - Document associations (HAS_DOCUMENT, ISSUED_ON)
  - Risk and compliance (HAS_RISK_ASSESSMENT, HAS_COMPLIANCE_EVENT)
  - Personnel relationships (HAS_AUTHORIZED_PERSON, CITIZEN_OF)
- Data consistency validation across databases
- Configurable data generation parameters
- Comprehensive test suite with pytest
- Asynchronous data generation and database operations

## Installation

1. Set up your Python environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Configure your databases:

Create a `.env` file with your database credentials:
```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=aml_monitoring
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
```

3. Initialize the databases:
```bash
# PostgreSQL
psql -U postgres
CREATE DATABASE aml_monitoring;

# Neo4j
# Make sure Neo4j is running and accessible
```

## Running the Data Generator

### Basic Usage

Generate a small dataset for testing:
```bash
python -m aml_monitoring.datagenerator.main \
    --num-institutions 5 \
    --max-accounts 3 \
    --min-transactions 5 \
    --max-transactions 10 \
    --max-beneficial-owners 2
```

### Advanced Usage

Generate a larger dataset with specific date ranges:
```bash
python -m aml_monitoring.datagenerator.main \
    --num-institutions 100 \
    --max-accounts 10 \
    --min-transactions 50 \
    --max-transactions 200 \
    --max-beneficial-owners 5 \
    --date-start 2023-01-01 \
    --date-end 2024-12-31 \
    --institutions-per-batch 20 \
    --transactions-per-batch 5000 \
    --verbose
```

### Command Line Options

```bash
Options:
  --num-institutions INT      Number of institutions to generate
  --max-accounts INT         Maximum accounts per institution
  --min-transactions INT     Minimum transactions per account
  --max-transactions INT     Maximum transactions per account
  --max-beneficial-owners INT Maximum beneficial owners per institution
  --date-start DATE         Start date for transactions (YYYY-MM-DD)
  --date-end DATE           End date for transactions (YYYY-MM-DD)
  --institutions-per-batch INT Number of institutions per batch
  --transactions-per-batch INT Maximum transactions per batch
  --wipe-clean             Wipe databases before generating new data
  --verbose                Enable detailed logging
```

## Development

### Running Tests

Run the test suite:
```bash
# Run all tests
pytest tests/datagenerator/

# Run specific test files
pytest tests/datagenerator/test_models.py
pytest tests/datagenerator/database/test_postgres.py
pytest tests/datagenerator/database/test_neo4j.py

# Run with coverage
pytest tests/datagenerator/ --cov=aml_monitoring.datagenerator
```

### Common Issues

1. Database Connection Issues:
```bash
# Check PostgreSQL status
pg_isready -h localhost -p 5432

# Check Neo4j status
curl -I http://localhost:7474
```

2. Permission Issues:
```bash
# Grant necessary permissions in PostgreSQL
psql -U postgres -d aml_monitoring -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_user;"
```

3. Memory Issues:
- Reduce batch sizes using `--institutions-per-batch` and `--transactions-per-batch`
- Monitor memory usage during generation

## Monitoring Progress

The data generator provides progress updates:
- Institution creation progress
- Batch processing status
- Database operation status
- Error reporting and handling

Use the `--verbose` flag for detailed progress information.
