import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, String, ForeignKey, Float, JSON
from sqlalchemy.sql import insert
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
import pandas as pd
from typing import Dict, Any, List

# Load environment variables from config directory
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config')
load_dotenv(os.path.join(config_dir, '.env'))

class PostgresHandler:
    def __init__(self):
        """Initialize PostgreSQL connection."""
        self.engine = create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        self.metadata = MetaData()
        self._create_tables()

    def cleanup(self):
        """Clean up all tables in the database."""
        with self.engine.connect() as connection:
            # Drop all tables
            self.metadata.drop_all(self.engine)
            # Recreate tables
            self.metadata.create_all(self.engine)

    def _create_tables(self):
        """Create database tables if they don't exist."""
        # Create tables for each entity type
        Table('institutions', self.metadata,
              Column('institution_id', String, primary_key=True),
              Column('data', JSON))

        Table('subsidiaries', self.metadata,
              Column('subsidiary_id', String, primary_key=True),
              Column('parent_institution_id', String, ForeignKey('institutions.institution_id')),
              Column('data', JSON))

        Table('compliance_events', self.metadata,
              Column('event_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('risk_assessments', self.metadata,
              Column('assessment_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('beneficial_owners', self.metadata,
              Column('owner_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('authorized_persons', self.metadata,
              Column('person_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('addresses', self.metadata,
              Column('address_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('documents', self.metadata,
              Column('document_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('jurisdiction_presences', self.metadata,
              Column('presence_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        Table('accounts', self.metadata,
              Column('account_id', String, primary_key=True),
              Column('entity_id', String),
              Column('data', JSON))

        # Create tables
        self.metadata.create_all(self.engine)

    def save_data(self, table_name: str, df: pd.DataFrame):
        """Save a dataframe to PostgreSQL."""
        print(f"Saving {table_name} to PostgreSQL...")
        table = Table(table_name, self.metadata, extend_existing=True)
        
        # Convert dataframe to list of dictionaries
        records = df.to_dict('records')
        
        # Insert records
        try:
            with self.engine.connect() as conn:
                # Delete existing records first since we've already cleaned up
                conn.execute(table.delete())
                # Insert new records
                if records:
                    conn.execute(insert(table), records)
                conn.commit()
        except Exception as e:
            print(f"Error saving {table_name} to PostgreSQL: {e}")
            raise


class Neo4jHandler:
    def __init__(self):
        """Initialize Neo4j connection."""
        neo4j_host = os.getenv('NEO4J_HOST', 'localhost')
        neo4j_port = os.getenv('NEO4J_PORT', '7687')
        neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
        neo4j_password = os.getenv('NEO4J_PASSWORD', 'neo4j')

        uri = f"bolt://{neo4j_host}:{neo4j_port}"
        try:
            self.driver = GraphDatabase.driver(
                uri,
                auth=(neo4j_user, neo4j_password)
            )
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
        except Exception as e:
            print(f"Failed to connect to Neo4j at {uri}: {str(e)}")
            raise

    def close(self):
        """Close the Neo4j connection."""
        if hasattr(self, 'driver'):
            self.driver.close()

    def cleanup(self):
        """Clean up the Neo4j database."""
        try:
            with self.driver.session() as session:
                # Delete all nodes and relationships
                session.run("MATCH (n) DETACH DELETE n")
                print("Successfully cleaned up Neo4j database")
        except Exception as e:
            print(f"Error cleaning up Neo4j database: {str(e)}")
            raise

    def save_data(self, entity_type: str, df: pd.DataFrame):
        """Save data to Neo4j based on entity type."""
        save_methods = {
            'institutions': self.save_institutions,
            'subsidiaries': self.save_subsidiaries,
            'compliance_events': self.save_compliance_events,
            'risk_assessments': self.save_risk_assessments,
            'beneficial_owners': self.save_beneficial_owners,
            'authorized_persons': self.save_authorized_persons,
            'addresses': self.save_addresses,
            'documents': self.save_documents,
            'jurisdiction_presences': self.save_jurisdiction_presences,
            'accounts': self.save_accounts
        }
        
        if entity_type in save_methods:
            try:
                print(f"Saving {entity_type} to Neo4j...")
                save_methods[entity_type](df)
            except Exception as e:
                print(f"Error saving {entity_type} to Neo4j: {str(e)}")
                raise
        else:
            print(f"Warning: No save method found for entity type {entity_type}")

    def save_institutions(self, df: pd.DataFrame):
        """Save institutions to Neo4j."""
        with self.driver.session() as session:
            for _, row in df.iterrows():
                data = row.to_dict()
                cypher = """
                // Create country node if it doesn't exist
                MERGE (c:Country {code: $incorporation_country})
                WITH c
                // Create institution
                CREATE (i:Institution {
                    institution_id: $institution_id,
                    legal_name: $legal_name,
                    business_type: $business_type,
                    risk_rating: $risk_rating
                })
                // Create relationship to incorporation country
                CREATE (i)-[:INCORPORATED_IN]->(c)
                """
                session.run(cypher, data)

    def save_subsidiaries(self, df: pd.DataFrame):
        """Save subsidiaries to Neo4j."""
        with self.driver.session() as session:
            for _, row in df.iterrows():
                data = row.to_dict()
                cypher = """
                // Match parent institution
                MATCH (i:Institution {institution_id: $parent_institution_id})
                // Create country node if it doesn't exist
                MERGE (c:Country {code: $incorporation_country})
                WITH i, c
                // Create subsidiary
                CREATE (s:Subsidiary {
                    subsidiary_id: $subsidiary_id,
                    legal_name: $legal_name,
                    business_type: $business_type
                })
                // Create relationships
                CREATE (i)-[:OWNS {ownership_percentage: $parent_ownership_percentage}]->(s)
                CREATE (s)-[:INCORPORATED_IN]->(c)
                """
                session.run(cypher, data)

    def save_compliance_events(self, df: pd.DataFrame) -> None:
        """Save compliance events to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                # Create event node
                query = """
                MERGE (e:ComplianceEvent {event_id: $event_id})
                SET e += $event_props
                WITH e
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_EVENT]->(e)
                """
                
                # Remove entity-specific fields from properties
                event_props = data.copy()
                entity_id = event_props.pop('entity_id')
                event_props.pop('entity_type', None)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        event_id=data['event_id'],
                        event_props=event_props,
                        entity_id=entity_id
                    )
            except Exception as e:
                print(f"Error saving event {data.get('event_id', 'unknown')}: {e}")

    def save_risk_assessments(self, df: pd.DataFrame) -> None:
        """Save risk assessments to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                
                # Convert risk_factors to string
                if 'risk_factors' in data:
                    data['risk_factors'] = json.dumps(data['risk_factors'])
                
                # Create risk assessment node
                query = """
                MERGE (r:RiskAssessment {assessment_id: $assessment_id})
                SET r += $assessment_props
                WITH r
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_RISK_ASSESSMENT]->(r)
                """
                
                # Remove entity-specific fields from properties
                assessment_props = data.copy()
                entity_id = assessment_props.pop('entity_id')
                assessment_props.pop('entity_type', None)
                
                # Convert any remaining dictionaries or lists to strings
                for key, value in assessment_props.items():
                    if isinstance(value, (list, dict)):
                        assessment_props[key] = json.dumps(value)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        assessment_id=data['assessment_id'],
                        assessment_props=assessment_props,
                        entity_id=entity_id
                    )
            except Exception as e:
                print(f"Error saving assessment {data.get('assessment_id', 'unknown')}: {e}")

    def save_beneficial_owners(self, df: pd.DataFrame) -> None:
        """Save beneficial owners to Neo4j."""
        # First pass: Create all owner nodes and their country relationships
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                # Create owner node and country relationships
                query = """
                // Create country nodes if they don't exist
                MERGE (residence:Country {code: $country_of_residence})
                MERGE (nationality:Country {code: $nationality})
                WITH residence, nationality
                
                // Create or merge the beneficial owner node
                MERGE (o:BeneficialOwner {owner_id: $owner_id})
                SET o += $owner_props
                
                // Create relationships to countries
                MERGE (o)-[:RESIDES_IN]->(residence)
                MERGE (o)-[:CITIZEN_OF]->(nationality)
                
                WITH o
                // Link to entity
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_BENEFICIAL_OWNER]->(o)
                """
                
                # Remove entity-specific fields from properties
                owner_props = data.copy()
                entity_id = owner_props.pop('entity_id')
                owner_props.pop('entity_type', None)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        owner_id=data['owner_id'],
                        owner_props=owner_props,
                        entity_id=entity_id,
                        country_of_residence=data['country_of_residence'],
                        nationality=data['nationality']
                    )
            except Exception as e:
                print(f"Error saving owner {data.get('owner_id', 'unknown')}: {e}")
        
        # Second pass: Create relationships between related owners
        with self.driver.session() as session:
            # Find owners with same nationality and residence
            query = """
            MATCH (o1:BeneficialOwner)-[:CITIZEN_OF]->(c:Country)
            MATCH (o1)-[:RESIDES_IN]->(r:Country)
            MATCH (o2:BeneficialOwner)-[:CITIZEN_OF]->(c)
            MATCH (o2)-[:RESIDES_IN]->(r)
            WHERE o1.owner_id < o2.owner_id  // Prevent duplicate relationships
            AND abs(o1.ownership_percentage - o2.ownership_percentage) < 10  // Similar ownership levels
            MERGE (o1)-[:RELATED_TO {reason: 'Same nationality and residence, similar ownership'}]->(o2)
            """
            session.run(query)
            
            # Find owners with PEP or sanctions status
            query = """
            MATCH (o1:BeneficialOwner)
            MATCH (o2:BeneficialOwner)
            WHERE o1.owner_id < o2.owner_id  // Prevent duplicate relationships
            AND (
                (o1.pep_status = true AND o2.pep_status = true) OR
                (o1.sanctions_status = true AND o2.sanctions_status = true)
            )
            MERGE (o1)-[:RELATED_TO {
                reason: CASE 
                    WHEN o1.pep_status = true THEN 'Both PEPs'
                    ELSE 'Both sanctioned'
                END
            }]->(o2)
            """
            session.run(query)

    def save_authorized_persons(self, df: pd.DataFrame) -> None:
        """Save authorized persons to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                
                # Convert contact_info to string
                if 'contact_info' in data:
                    data['contact_info'] = json.dumps(data['contact_info'])
                
                # Create authorized person node
                query = """
                MERGE (p:AuthorizedPerson {person_id: $person_id})
                SET p += $person_props
                WITH p
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_AUTHORIZED_PERSON]->(p)
                """
                
                # Remove entity-specific fields from properties
                person_props = data.copy()
                entity_id = person_props.pop('entity_id')
                person_props.pop('entity_type', None)
                
                # Convert any remaining dictionaries or lists to strings
                for key, value in person_props.items():
                    if isinstance(value, (list, dict)):
                        person_props[key] = json.dumps(value)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        person_id=data['person_id'],
                        person_props=person_props,
                        entity_id=entity_id
                    )
            except Exception as e:
                print(f"Error saving person {data.get('person_id', 'unknown')}: {e}")

    def save_addresses(self, df: pd.DataFrame) -> None:
        """Save addresses to Neo4j."""
        # First pass: Create all address nodes with country relationships
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                
                # Convert geo_coordinates to string
                if 'geo_coordinates' in data:
                    data['geo_coordinates'] = json.dumps(data['geo_coordinates'])
                
                # Create address node and country relationship
                query = """
                // Create country node if it doesn't exist
                MERGE (c:Country {code: $country})
                WITH c
                
                // Create address node
                MERGE (a:Address {address_id: $address_id})
                SET a += $address_props
                
                // Create relationship to country
                MERGE (a)-[:LOCATED_IN]->(c)
                
                WITH a
                // Link to entity
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_ADDRESS]->(a)
                """
                
                # Remove entity-specific fields from properties
                address_props = data.copy()
                entity_id = address_props.pop('entity_id')
                address_props.pop('entity_type', None)
                
                # Convert any remaining dictionaries or lists to strings
                for key, value in address_props.items():
                    if isinstance(value, (list, dict)):
                        address_props[key] = json.dumps(value)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        address_id=data['address_id'],
                        address_props=address_props,
                        entity_id=entity_id,
                        country=data['country']
                    )
            except Exception as e:
                print(f"Error saving address {data.get('address_id', 'unknown')}: {e}")
        
        # Second pass: Create NEAR relationships between addresses
        with self.driver.session() as session:
            # Find addresses in close proximity (within same city or with similar coordinates)
            query = """
            MATCH (a1:Address)
            MATCH (a2:Address)
            WHERE a1.address_id < a2.address_id  // Prevent duplicate relationships
            AND (
                // Same city and postal code
                (a1.city = a2.city AND a1.postal_code = a2.postal_code)
                OR
                // Same street
                (a1.street_address = a2.street_address AND a1.country = a2.country)
            )
            WITH a1, a2,
                CASE 
                    WHEN a1.city = a2.city AND a1.postal_code = a2.postal_code 
                    THEN 'Same city and postal code'
                    ELSE 'Same street address'
                END as reason
            MERGE (a1)-[:NEAR {reason: reason}]->(a2)
            """
            session.run(query)
            
            # Find addresses used by the same entity
            query = """
            MATCH (e)-[:HAS_ADDRESS]->(a1:Address)
            MATCH (e)-[:HAS_ADDRESS]->(a2:Address)
            WHERE a1.address_id < a2.address_id  // Prevent duplicate relationships
            AND a1.address_type <> a2.address_type  // Different address types
            MERGE (a1)-[:NEAR {reason: 'Used by same entity'}]->(a2)
            """
            session.run(query)

    def save_documents(self, df: pd.DataFrame) -> None:
        """Save documents to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                # Create document node
                query = """
                MERGE (d:Document {document_id: $document_id})
                SET d += $document_props
                WITH d
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_DOCUMENT]->(d)
                """
                
                # Remove entity-specific fields from properties
                document_props = data.copy()
                entity_id = document_props.pop('entity_id')
                document_props.pop('entity_type', None)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        document_id=data['document_id'],
                        document_props=document_props,
                        entity_id=entity_id
                    )
            except Exception as e:
                print(f"Error saving document {data.get('document_id', 'unknown')}: {e}")

    def save_jurisdiction_presences(self, df: pd.DataFrame) -> None:
        """Save jurisdiction presences to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                
                # Convert reporting_requirements to string
                if 'reporting_requirements' in data:
                    data['reporting_requirements'] = json.dumps(data['reporting_requirements'])
                
                # Create presence node
                query = """
                // Create country node if it doesn't exist
                MERGE (c:Country {code: $jurisdiction})
                WITH c
                // Create presence node
                MERGE (p:JurisdictionPresence {presence_id: $presence_id})
                SET p += $presence_props
                WITH p, c
                // Create relationship to country
                MERGE (p)-[:PRESENT_IN]->(c)
                WITH p
                // Link to entity
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_JURISDICTION_PRESENCE]->(p)
                """
                
                # Remove entity-specific fields from properties
                presence_props = data.copy()
                entity_id = presence_props.pop('entity_id')
                presence_props.pop('entity_type', None)
                
                # Convert lists to strings
                for key, value in presence_props.items():
                    if isinstance(value, (list, dict)):
                        presence_props[key] = json.dumps(value)
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        presence_id=data['presence_id'],
                        presence_props=presence_props,
                        entity_id=entity_id,
                        jurisdiction=data['jurisdiction']
                    )
            except Exception as e:
                print(f"Error saving presence {data.get('presence_id', 'unknown')}: {e}")

    def save_accounts(self, df: pd.DataFrame) -> None:
        """Save accounts to Neo4j."""
        for _, row in df.iterrows():
            try:
                data = row.to_dict()
                
                query = """
                // Create currency node if it doesn't exist
                MERGE (curr:Currency {code: $currency})
                WITH curr
                
                // Create account node
                MERGE (a:Account {account_id: $account_id})
                SET a += $account_props
                
                // Create relationship to currency
                MERGE (a)-[:DENOMINATED_IN]->(curr)
                
                // Create custodian country relationship only if custodian_country exists
                WITH a
                OPTIONAL MATCH (c:Country {code: $custodian_country})
                FOREACH (_ IN CASE WHEN $custodian_country IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (c:Country {code: $custodian_country})
                    MERGE (a)-[:CUSTODIED_IN]->(c)
                )
                
                WITH a
                // Link to entity
                MATCH (entity)
                WHERE (entity:Institution AND entity.institution_id = $entity_id) OR 
                      (entity:Subsidiary AND entity.subsidiary_id = $entity_id)
                MERGE (entity)-[:HAS_ACCOUNT]->(a)
                """
                
                # Remove entity-specific fields from properties
                account_props = data.copy()
                entity_id = account_props.pop('entity_id')
                account_props.pop('entity_type', None)
                account_props.pop('currency', None)  # Remove currency since it's handled separately
                account_props.pop('custodian_country', None)  # Remove custodian_country since it's handled separately
                
                with self.driver.session() as session:
                    session.run(
                        query,
                        account_id=data['account_id'],
                        account_props=account_props,
                        entity_id=entity_id,
                        currency=data['currency'],
                        custodian_country=data.get('custodian_country')  # Use get() to handle None safely
                    )
            except Exception as e:
                print(f"Error saving account {data.get('account_id', 'unknown')}: {e}")


class DatabaseManager:
    def __init__(self):
        """Initialize database handlers."""
        self.postgres_handler = PostgresHandler()
        self.neo4j_handler = Neo4jHandler()

    def cleanup_postgres(self):
        """Clean up PostgreSQL database."""
        self.postgres_handler.cleanup()

    def cleanup_neo4j(self):
        """Clean up Neo4j database."""
        self.neo4j_handler.cleanup()

    def save_to_postgres(self, data: Dict[str, pd.DataFrame]):
        """Save data to PostgreSQL."""
        for entity_type, df in data.items():
            if not df.empty:
                print(f"Saving {entity_type} to PostgreSQL...")
                self.postgres_handler.save_data(entity_type, df)

    def save_to_neo4j(self, data: Dict[str, pd.DataFrame]):
        """Save data to Neo4j."""
        for entity_type, df in data.items():
            if not df.empty:
                print(f"Saving {entity_type} to Neo4j...")
                self.neo4j_handler.save_data(entity_type, df)

    def save_data(self, data: Dict[str, pd.DataFrame]):
        """Save all dataframes to both PostgreSQL and Neo4j."""
        # Save to PostgreSQL
        self.save_to_postgres(data)

        # Save to Neo4j
        self.save_to_neo4j(data)

    def close(self):
        """Close all database connections."""
        self.neo4j_handler.close()
