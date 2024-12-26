# Database Documentation

## Overview
This document provides detailed information about the database schema and relationships used in the AML Transaction Monitoring system. The system uses a hybrid database approach with PostgreSQL for transactional data and Neo4j for relationship mapping and graph-based analytics.

## Table Descriptions

### Entities
The `entities` table serves as the foundation for both institutions and subsidiaries. It implements a hierarchical structure where institutions can have multiple subsidiaries.
- Primary purpose: Track all financial entities in the system
- Key features:
  - Soft deletion support through `deleted_at`
  - Hierarchical relationship tracking through `parent_entity_id`
  - Temporal tracking with `created_at` and `updated_at`

### Institutions
The `institutions` table stores detailed information about financial institutions.
- Primary purpose: Maintain comprehensive institutional profiles
- Key features:
  - Risk assessment tracking
  - Regulatory compliance information
  - Operational status monitoring
  - Business classification and activity tracking

### Accounts
The `accounts` table manages all financial accounts in the system.
- Primary purpose: Track financial accounts and their relationships to entities
- Key features:
  - Multi-currency support
  - Risk rating at account level
  - Balance tracking
  - Activity monitoring
  - Custodian information

### Transactions
The `transactions` table records all financial transactions.
- Primary purpose: Comprehensive transaction tracking for AML monitoring
- Key features:
  - Bi-directional transaction tracking (debit/credit)
  - Multiple status support
  - Counterparty information
  - Cross-border transaction tracking
  - Alert flagging and details
  - Transaction purpose tracking

### Beneficial Owners
The `beneficial_owners` table tracks ownership information for entities.
- Primary purpose: KYC and ownership transparency
- Key features:
  - Ownership percentage tracking
  - PEP status monitoring
  - Sanctions screening
  - Verification tracking
  - Demographic information

## Neo4j Relationships

### Entity Relationships
1. `OWNS_SUBSIDIARY`
   - Direction: Institution -> Subsidiary
   - Properties:
     - ownership_date
     - ownership_percentage
   - Purpose: Track corporate structure and ownership hierarchy

2. `HAS_ACCOUNT`
   - Direction: Entity -> Account
   - Properties:
     - opening_date
     - relationship_type
   - Purpose: Link entities to their financial accounts

3. `HAS_BENEFICIAL_OWNER`
   - Direction: Entity -> Beneficial Owner
   - Properties:
     - ownership_percentage
     - verification_date
   - Purpose: Track ultimate beneficial ownership

### Transaction Relationships
1. `SENDS_MONEY`
   - Direction: Account -> Account
   - Properties:
     - transaction_id
     - amount
     - currency
     - timestamp
   - Purpose: Track money flow between accounts

2. `COUNTERPARTY_OF`
   - Direction: Entity <-> Entity
   - Properties:
     - relationship_start_date
     - transaction_count
     - total_volume
   - Purpose: Track business relationships between entities

### Risk Relationships
1. `SHARES_ADDRESS_WITH`
   - Direction: Entity <-> Entity
   - Properties:
     - address
     - date_identified
   - Purpose: Identify potential connections through shared addresses

2. `SHARES_BENEFICIAL_OWNER_WITH`
   - Direction: Entity <-> Entity
   - Properties:
     - owner_id
     - identification_date
   - Purpose: Track entities with common beneficial owners

## Data Flow and Relationships
1. Hierarchical Flow:
   - Institutions can have multiple subsidiaries
   - Both institutions and subsidiaries can have multiple accounts
   - Accounts can have multiple transactions

2. Transaction Flow:
   - Each transaction connects two accounts
   - Transactions create implicit relationships between entities
   - Transaction patterns form the basis for risk assessment

3. Ownership Flow:
   - Entities can have multiple beneficial owners
   - Beneficial owners can own multiple entities
   - Cross-ownership creates complex relationship networks

## Usage Guidelines
1. Entity Creation:
   - Always create the entity record before creating institution/subsidiary details
   - Ensure parent-child relationships are valid for subsidiaries

2. Transaction Recording:
   - Both debit and credit accounts must exist
   - Currency information must be consistent
   - Transaction status transitions should be properly tracked

3. Relationship Management:
   - Regularly update relationship properties for accurate risk assessment
   - Maintain temporal aspects of relationships
   - Verify circular relationship prevention
