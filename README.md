# AML Transaction Monitoring Test Data Generator

This project provides tools for generating synthetic financial transaction data for testing Anti-Money Laundering (AML) monitoring systems.

## Features

- Generate realistic financial institution data
- Create synthetic customer accounts with beneficial ownership structures
- Generate transaction patterns that simulate normal and suspicious activities
- Support for both PostgreSQL and Neo4j databases
- Configurable parameters for data generation

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Generate test data with custom parameters:

```bash
python -m aml_monitoring.datagenerator.main \
    --num-institutions <number> \
    --max-accounts <number> \
    --min-transactions <number> \
    --max-transactions <number> \
    --max-beneficial-owners <number> \
    --institutions-per-batch <number> \
    --transactions-per-batch <number>
```

Example:
```bash
python -m aml_monitoring.datagenerator.main \
    --num-institutions 15 \
    --max-accounts 5 \
    --min-transactions 20 \
    --max-transactions 300 \
    --max-beneficial-owners 3 \
    --institutions-per-batch 3 \
    --transactions-per-batch 1000
```

## Configuration

The application uses environment variables for database configuration. Create a `.env` file with:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

## Development

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install development dependencies:
```bash
pip install -e .
```

## Testing

Run tests using pytest:
```bash
pytest
```
