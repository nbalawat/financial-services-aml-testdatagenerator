# AML Transaction Monitoring Flows

The flows module is responsible for executing AML monitoring workflows, processing transactions, generating alerts, and managing the monitoring lifecycle. It works in conjunction with the data generator module to provide a complete AML transaction monitoring solution.

## Features

- Real-time transaction monitoring
- Configurable alert rules and thresholds
- Risk scoring and assessment
- Alert management and workflow
- Transaction pattern analysis
- Customer profiling
- Reporting and analytics
- Asynchronous processing
- Integration with PostgreSQL and Neo4j

## Installation

1. Set up your Python environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Configure your environment:

Create a `.env` file with your configuration:
```bash
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=aml_monitoring
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j

# Flow Configuration
FLOW_BATCH_SIZE=1000
FLOW_WORKERS=4
ALERT_THRESHOLD=0.75
RISK_SCORE_THRESHOLD=0.8
```

## Running the Flows

### Basic Usage

1. Start the flow processor:
```bash
python -m aml_monitoring.flows.main \
    --batch-size 1000 \
    --workers 4 \
    --alert-threshold 0.75
```

2. Run specific flows:
```bash
# Run transaction monitoring flow
python -m aml_monitoring.flows.main --flow transaction_monitoring

# Run customer risk assessment flow
python -m aml_monitoring.flows.main --flow customer_risk

# Run alert generation flow
python -m aml_monitoring.flows.main --flow alert_generation
```

### Advanced Usage

1. Run with custom configuration:
```bash
python -m aml_monitoring.flows.main \
    --flow transaction_monitoring \
    --batch-size 5000 \
    --workers 8 \
    --alert-threshold 0.85 \
    --risk-threshold 0.9 \
    --lookback-days 30 \
    --verbose
```

2. Run multiple flows:
```bash
python -m aml_monitoring.flows.main \
    --flows transaction_monitoring customer_risk alert_generation \
    --batch-size 2000 \
    --workers 6
```

### Command Line Options

```bash
Options:
  --flows LIST             List of flows to run [transaction_monitoring, customer_risk, alert_generation]
  --batch-size INT        Number of transactions to process per batch
  --workers INT          Number of worker processes
  --alert-threshold FLOAT Alert generation threshold (0.0-1.0)
  --risk-threshold FLOAT  Risk score threshold (0.0-1.0)
  --lookback-days INT    Number of days to look back for analysis
  --verbose              Enable detailed logging
```

## Development

### Running Tests

Run the test suite:
```bash
# Run all flow tests
pytest tests/flows/

# Run specific test files
pytest tests/flows/test_flow_models.py
pytest tests/flows/database/test_postgres_flow.py

# Run with coverage
pytest tests/flows/ --cov=aml_monitoring.flows
```

### Flow Components

1. Transaction Monitoring Flow:
- Pattern detection
- Threshold monitoring
- Velocity checks
- Amount analysis

2. Customer Risk Flow:
- Risk scoring
- Profile updates
- Relationship analysis
- Historical behavior

3. Alert Generation Flow:
- Alert creation
- Risk level assignment
- Notification handling
- Case management

### Common Issues

1. Performance Issues:
```bash
# Optimize batch processing
python -m aml_monitoring.flows.main \
    --batch-size 1000 \
    --workers 4 \
    --optimize-memory
```

2. Database Connection Issues:
```bash
# Test database connectivity
python -m aml_monitoring.flows.utils.db_check
```

3. Flow Processing Issues:
- Check logs in `logs/flows/`
- Verify database connections
- Monitor worker processes
- Check memory usage

## Monitoring

The flows module provides extensive monitoring:

1. Real-time Metrics:
- Transaction processing rate
- Alert generation rate
- Worker status
- Queue lengths

2. Performance Monitoring:
- Database operation latency
- Processing time per transaction
- Memory usage per worker
- Queue wait times

3. Error Monitoring:
- Failed transactions
- Database errors
- Processing timeouts
- Worker crashes

Use the `--verbose` flag for detailed monitoring information.
