# AML Transaction Monitoring System

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

## Project Structure

```
aml_txnmonitoring_agents/
├── scripts/             # Utility scripts
│   └── generate_test_data.py
├── src/                 # Source code
│   └── aml_monitoring/
│       ├── models.py    # Pydantic models
│       ├── data_generator.py
│       └── database/
│           ├── base.py
│           ├── postgres.py
│           ├── neo4j.py
│           └── exceptions.py
├── tests/              # Test files
└── test.env           # Test environment variables
```

## Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Neo4j 5+
- Docker (optional, for containerized databases)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/aml_txnmonitoring_agents.git
   cd aml_txnmonitoring_agents
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

4. Configure environment variables:
   ```bash
   cp test.env.example test.env
   # Edit test.env with your database credentials
   ```

## Usage

### Command-Line Interface

The system provides two ways to generate test data:

1. Using the simplified script (`scripts/generate_test_data.py`):
   ```bash
   python scripts/generate_test_data.py
   ```

2. Using the main CLI (`main.py`) with more configuration options:
   ```bash
   # Basic usage with default settings
   python main.py

   # Generate data for a large institution with many accounts
   python main.py --num-institutions 1 \
                 --max-accounts 50 \
                 --min-transactions 100 \
                 --max-transactions 500 \
                 --max-beneficial-owners 10

   # Generate data for multiple small institutions
   python main.py --num-institutions 20 \
                 --max-accounts 3 \
                 --min-transactions 5 \
                 --max-transactions 15 \
                 --max-beneficial-owners 2

   # Generate data for a specific date range
   python main.py --date-start 2023-01-01 \
                 --date-end 2023-12-31 \
                 --num-institutions 5

   # Clean up databases without generating new data
   python main.py --cleanup-only

   # Use verbose logging for debugging
   python main.py --verbose

   # Use a custom environment file
   python main.py --env-file prod.env
   ```

Available Options:
```
Data Generation Parameters:
  --num-institutions INT    Number of institutions to generate (default: 5)
  --max-accounts INT       Maximum number of accounts per institution (default: 3)
  --min-transactions INT   Minimum number of transactions per account (default: 5)
  --max-transactions INT   Maximum number of transactions per account (default: 10)
  --max-beneficial-owners INT  Maximum beneficial owners per institution (default: 2)
  --date-start YYYY-MM-DD  Start date for data generation (default: 2023-01-01)
  --date-end YYYY-MM-DD    End date for data generation (default: 2024-12-25)

Utility Parameters:
  --cleanup-only          Only cleanup databases without generating new data
  --env-file PATH        Environment file path (default: test.env)
  --verbose              Enable verbose output
```

Common Use Cases:

1. **Development Testing**:
   ```bash
   python main.py --num-institutions 3 \
                 --max-accounts 2 \
                 --min-transactions 2 \
                 --max-transactions 5 \
                 --verbose
   ```
   Generates a small dataset quickly for rapid development cycles.

2. **Integration Testing**:
   ```bash
   python main.py --num-institutions 10 \
                 --max-accounts 10 \
                 --min-transactions 20 \
                 --max-transactions 50 \
                 --max-beneficial-owners 5 \
                 --date-start 2023-01-01 \
                 --date-end 2023-12-31
   ```
   Creates a medium-sized dataset with varied relationships for testing integrations.

3. **Performance Testing**:
   ```bash
   python main.py --num-institutions 50 \
                 --max-accounts 100 \
                 --min-transactions 1000 \
                 --max-transactions 5000 \
                 --max-beneficial-owners 20
   ```
   Generates a large dataset to test system performance and scalability.

4. **Compliance Testing**:
   ```bash
   python main.py --num-institutions 5 \
                 --max-beneficial-owners 10 \
                 --max-accounts 20 \
                 --min-transactions 100 \
                 --max-transactions 200 \
                 --date-start 2023-01-01 \
                 --date-end 2023-12-31
   ```
   Creates a dataset focused on beneficial ownership relationships for testing compliance scenarios.

### Generating Test Data

1. Using the command-line script:
   ```bash
   python scripts/generate_test_data.py
   ```

   This will:
   - Generate 5 institutions with subsidiaries
   - Create beneficial owners for each institution
   - Generate accounts and transactions
   - Save data to both PostgreSQL and Neo4j
   - Create temporal relationships for incorporation dates

2. Using the Python API:
   ```python
   from aml_monitoring.data_generator import DataGenerator

   # Initialize with custom configuration
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

   # Initialize databases
   await generator.initialize_db()

   # Generate data
   await generator.generate_all()

   # Close connections
   await generator.close_db()
   ```

## Data Models

The system includes the following main data models:

1. Institution
   - Basic information (name, type, incorporation details)
   - Regulatory information
   - Risk ratings and compliance status
   - Relationships with subsidiaries

2. Beneficial Owner
   - Personal information
   - Ownership details
   - Relationships with institutions

3. Account
   - Account details and status
   - Balance and currency information
   - Relationships with institutions

4. Transaction
   - Transaction details
   - Amount and currency
   - Temporal relationships

## Database Schema

### PostgreSQL Tables
- `institutions`: Financial institutions and their core details
- `subsidiaries`: Subsidiary entities and their relationships
- `accounts`: Account information and balances
- `transactions`: Financial transactions with debit/credit account tracking
- `beneficial_owners`: Beneficial ownership information
- `documents`: Identity and verification documents
- `risk_assessments`: Risk assessment records
- `compliance_events`: Compliance-related events and actions
- `authorized_persons`: Authorized signatories and key personnel

### Neo4j Graph Model
The Neo4j graph model represents entities as nodes and their relationships as edges:

#### Node Types
- Institution
- Subsidiary
- Account
- Transaction
- BeneficialOwner
- Document
- RiskAssessment
- ComplianceEvent
- AuthorizedPerson
- Country
- BusinessDate

#### Key Relationships
- Transaction Flow:
  - `SENT`: Account to Transaction (with amount and currency)
  - `RECEIVED`: Transaction to Account (with amount and currency)
  - `TRANSACTED`: Account to Transaction (with transaction date)
  - `TRANSACTED_ON`: Transaction to BusinessDate
- Entity Structure:
  - `OWNS_SUBSIDIARY`: Institution to Subsidiary
  - `IS_CUSTOMER`: Subsidiary to Institution
  - `OWNED_BY`: Institution/Subsidiary to BeneficialOwner
- Document Management:
  - `HAS_DOCUMENT`: Institution/Subsidiary to Document
  - `ISSUED_ON`: Document to BusinessDate
- Risk and Compliance:
  - `HAS_RISK_ASSESSMENT`: Institution/Subsidiary to RiskAssessment
  - `HAS_COMPLIANCE_EVENT`: Institution/Subsidiary to ComplianceEvent
- Personnel:
  - `HAS_AUTHORIZED_PERSON`: Institution/Subsidiary to AuthorizedPerson
  - `CITIZEN_OF`: BeneficialOwner/AuthorizedPerson to Country

## Recent Improvements

### Neo4j Data Handling
- Improved relationship creation with proper null value handling using Neo4j's `COALESCE` function
- Enhanced transaction node creation with atomic operations
- Fixed account-transaction relationships to ensure proper linking
- Added robust error handling and diagnostics for database operations

### Data Generation
- Enhanced data consistency between PostgreSQL and Neo4j
- Improved transaction generation with proper relationship tracking
- Added validation for required fields and relationships
- Optimized batch processing for better performance

### Testing
To run the test suite:

1. First, ensure your databases are running and properly configured in `test.env`:
   ```bash
   # Required PostgreSQL environment variables
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_USER=your_user
   POSTGRES_PASSWORD=your_password
   POSTGRES_DB=your_test_db

   # Required Neo4j environment variables
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=your_password
   ```

2. Install test dependencies:
   ```bash
   pip install -e ".[test]"
   ```

3. Run the tests:
   ```bash
   # Run all tests
   pytest

   # Run specific test file
   pytest tests/test_data_generator.py

   # Run with verbose output
   pytest -v

   # Run with coverage report
   pytest --cov=src/aml_monitoring
   ```

4. Common test scenarios:
   - `test_neo4j_relationship_completeness`: Verifies all required relationships are created
   - `test_transaction_generation`: Validates transaction data and relationships
   - `test_subsidiary_generation`: Tests subsidiary creation and hierarchies

### Troubleshooting
If you encounter database-related issues:

1. Neo4j Relationship Errors:
   - Ensure your Neo4j instance is running and accessible
   - Check that all required properties are non-null
   - Verify that referenced nodes exist before creating relationships

2. PostgreSQL Connection Issues:
   - Verify database credentials in `test.env`
   - Ensure PostgreSQL service is running
   - Check database permissions

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

The test suite includes:
- Unit tests for data generation
- Integration tests for database operations
- Data integrity tests across databases
- Relationship validation tests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the test suite
5. Submit a pull request

## License

MIT License
