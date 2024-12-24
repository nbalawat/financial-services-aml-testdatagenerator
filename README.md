# AML Transaction Monitoring System

A comprehensive system for generating and managing test data for Anti-Money Laundering (AML) transaction monitoring. This system helps financial institutions test and validate their AML monitoring systems by generating realistic customer data, transactions, and risk assessments.

## Features

- Generate realistic test data for:
  - Financial institutions and their subsidiaries
  - Customer profiles and relationships
  - Risk assessments and compliance events
  - Beneficial owners and authorized persons
  - Documents and jurisdiction presence
  - Account information
- Save data to both PostgreSQL and Neo4j databases
- Export data to CSV files for further analysis
- Configurable data generation parameters
- Validation using Pydantic models

## Project Structure

```
aml_txnmonitoring_agents/
├── config/               # Configuration files
│   ├── .env
│   ├── .env.example
│   └── docker-compose.yml
├── docs/                # Documentation
├── src/                 # Source code
│   └── aml_monitoring/
│       ├── models.py
│       ├── db_handlers.py
│       └── generate_test_data.py
├── tests/              # Test files
├── data/              # Data directory
├── test_data/         # Test data output
└── setup.py
```

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

4. Set up the databases:
   ```bash
   docker-compose -f config/docker-compose.yml up -d
   ```

5. Configure environment variables:
   ```bash
   cp config/.env.example config/.env
   # Edit config/.env with your database credentials
   ```

## Usage

1. Run from command line:
   ```bash
   # Generate data with default settings (2 institutions)
   python main.py

   # Generate data with custom number of institutions
   python main.py --num-institutions 5

   # Specify custom output directory
   python main.py --output-dir custom_data
   ```

2. Use as a library:
   ```python
   from aml_monitoring.generate_test_data import TestDataGenerator
   from aml_monitoring.db_handlers import DatabaseManager

   # Initialize
   generator = TestDataGenerator()
   db_manager = DatabaseManager()

   # Generate data
   data = generator.generate_all_data(num_institutions=2)

   # Save to databases and CSV
   db_manager.save_data(data)
   ```

## Data Models

The system includes the following main data models:

- `Institution`: Financial institutions with their core attributes
- `Subsidiary`: Related entities owned by institutions
- `RiskAssessment`: Risk evaluation records
- `ComplianceEvent`: Compliance-related activities and changes
- `BeneficialOwner`: Ultimate beneficial ownership information
- `AuthorizedPerson`: Individuals with authorization rights
- `Document`: Required documentation and verifications
- `Account`: Financial account information

## Development

- Run tests: `python -m pytest tests/`
- Format code: `black src/ tests/`
- Check types: `mypy src/`

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
