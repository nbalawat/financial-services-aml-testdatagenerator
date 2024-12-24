# AML Transaction Monitoring Data Generator

This project provides a comprehensive data generation tool for Anti-Money Laundering (AML) transaction monitoring systems. It generates realistic test data for financial institutions, their subsidiaries, compliance events, and related entities.

## Overview

The data generator creates interconnected datasets that model the complex relationships between financial institutions and their compliance-related activities. The generated data includes:

- Financial Institutions
- Subsidiaries
- Compliance Events
- Risk Assessments
- Beneficial Owners
- Authorized Persons
- Addresses
- Documents
- Jurisdiction Presences
- Accounts

## Data Models

### Institution
- Core entity representing financial institutions
- Includes fields for:
  - Legal and business information
  - Regulatory status and licenses
  - Risk ratings and compliance status
  - Business activities and industry codes
  - Key dates (onboarding, reviews)

### Subsidiary
- Represents entities owned by main institutions
- Includes:
  - Ownership and control information
  - Financial metrics
  - Regulatory status
  - Integration status
  - Corporate governance details
  - Customer relationship status (if applicable)

### ComplianceEvent
- Tracks compliance-related activities
- Types include:
  - Onboarding events
  - Risk rating changes
  - Periodic reviews
  - Other compliance activities
- Includes decision tracking and next review dates

### RiskAssessment
- Documents risk evaluation activities
- Captures:
  - Risk scores and ratings
  - Assessment types
  - Risk factors
  - Findings and recommendations
  - Review schedules

### BeneficialOwner
- Records ultimate beneficial owners
- Includes:
  - Identification information
  - Ownership percentages
  - Verification details
  - PEP status

### AuthorizedPerson
- Tracks individuals with authorization
- Includes:
  - Authorization types and levels
  - Validity periods
  - Verification status

### Address
- Stores location information
- Includes:
  - Address details
  - Verification status
  - Effective dates
  - Primary status

## Data Generation Features

- Realistic data generation using Faker library
- Consistent relationships between entities
- Proper handling of dates and time periods
- Realistic business rules and constraints
- Configurable number of records
- Data validation using Pydantic models

## Usage

To generate test data, run:

```bash
python generate_test_data.py --num-institutions <number> --seed <seed> --output-dir <directory>
```

Parameters:
- `num-institutions`: Number of primary institutions to generate
- `seed`: Random seed for reproducible data generation
- `output-dir`: Directory to save the generated CSV files

## Generated Files

The script generates the following CSV files in the specified output directory:
- `institutions.csv`
- `subsidiaries.csv`
- `compliance_events.csv`
- `risk_assessments.csv`
- `beneficial_owners.csv`
- `authorized_persons.csv`
- `addresses.csv`
- `documents.csv`
- `jurisdiction_presences.csv`
- `accounts.csv`

## Dependencies

- Python 3.8+
- Pydantic
- Pandas
- Faker
- NumPy

## Data Validation

All generated data is validated using Pydantic models to ensure:
- Required fields are present
- Data types are correct
- Relationships between entities are maintained
- Business rules are followed

## Future Enhancements

Potential areas for enhancement include:
1. Additional entity types and relationships
2. More complex compliance scenarios
3. Enhanced validation rules
4. Additional data generation patterns
5. Support for different regulatory frameworks
6. Integration with transaction generation systems

## Contributing

Contributions are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.

## License

[Add appropriate license information]
