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

- **Institution**: Core financial institution data
- **Subsidiary**: Related subsidiary entities
- **Account**: Financial accounts with currency and custodian relationships
- **BeneficialOwner**: Ultimate beneficial owners with nationality and residence info
- **AuthorizedPerson**: Authorized signatories and key personnel
- **Address**: Physical location information
- **Document**: Identity and regulatory documents
- **JurisdictionPresence**: Regulatory presence in different jurisdictions
- **RiskAssessment**: Risk evaluation records
- **ComplianceEvent**: Compliance-related events and actions

## Neo4j Graph Structure

The system maintains the following key relationships in Neo4j:

### Entity Relationships
- Institution/Subsidiary `-[HAS_ACCOUNT]->` Account
- Institution/Subsidiary `-[HAS_ADDRESS]->` Address
- Institution/Subsidiary `-[HAS_DOCUMENT]->` Document
- Institution/Subsidiary `-[HAS_JURISDICTION_PRESENCE]->` JurisdictionPresence

### Country-Based Relationships
- Institution `-[INCORPORATED_IN]->` Country
- Address `-[LOCATED_IN]->` Country
- JurisdictionPresence `-[PRESENT_IN]->` Country
- BeneficialOwner `-[RESIDES_IN]->` Country
- BeneficialOwner `-[CITIZEN_OF]->` Country
- Account `-[CUSTODIED_IN]->` Country

### Financial Relationships
- Account `-[DENOMINATED_IN]->` Currency
- BeneficialOwner `-[RELATED_TO]->` BeneficialOwner
- Address `-[NEAR]->` Address (for geographically close addresses)

## Development

- Run tests: `python -m pytest tests/`
- Format code: `black src/ tests/`
- Check types: `mypy src/`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions and feedback, please open an issue on GitHub.
