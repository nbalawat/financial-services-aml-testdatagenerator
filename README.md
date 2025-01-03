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
see the docker-compose file in config folder

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

## Docker Setup

The project uses Docker Compose to set up the required database infrastructure. The following services are included:
- PostgreSQL (15.0)
- pgAdmin4 (for PostgreSQL monitoring)
- Neo4j (5.0) with APOC plugins

### Starting the Services

1. Navigate to the config directory:
```bash
cd config
```

2. Start all services:
```bash
docker-compose up -d
```

### Accessing Database Management Tools

#### PostgreSQL (pgAdmin4)
- URL: http://localhost:5050
- Login credentials:
  - Email: admin@admin.com
  - Password: admin
- To connect to PostgreSQL server:
  - Host: postgres
  - Port: 5432
  - Database: aml_monitoring
  - Username: aml_user
  - Password: aml_password

#### Neo4j Browser
- URL: http://localhost:7474
- Connection URL: bolt://localhost:7687
- Default credentials:
  - Username: neo4j
  - Password: aml_password

### Monitoring Services

#### PostgreSQL Status
Check if PostgreSQL is healthy:
```bash
docker exec aml_postgres pg_isready -U aml_user -d aml_monitoring
```

View PostgreSQL logs:
```bash
docker logs aml_postgres
```

#### Neo4j Status
View Neo4j logs:
```bash
docker logs aml_neo4j
```

### Stopping Services
```bash
docker-compose down
```

To remove all data volumes:
```bash
docker-compose down -v
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
