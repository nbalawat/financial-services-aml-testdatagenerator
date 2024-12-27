import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import Document
from .base_generator import BaseGenerator

class DocumentGenerator(BaseGenerator):
    """Generator for document data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Document, None]:
        """Generate documents for a given entity."""
        num_documents = random.randint(
            self.config.get('min_documents_per_entity', 1),
            self.config.get('max_documents_per_entity', 5)
        )
        
        document_types = [
            'certificate_of_incorporation',
            'business_license',
            'tax_certificate',
            'regulatory_approval',
            'board_resolution',
            'financial_statement',
            'compliance_certificate',
            'kyc_documentation'
        ]
        
        for _ in range(num_documents):
            # Generate dates
            issue_date = self.fake.date_between(start_date='-5y', end_date='-1y').strftime('%Y-%m-%d')
            expiry_date = (datetime.strptime(issue_date, '%Y-%m-%d') + 
                          timedelta(days=random.randint(365, 3650))).strftime('%Y-%m-%d')
            verification_date = (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d')
            
            document = Document(
                document_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                document_type=random.choice(document_types),
                document_number=self.fake.bothify(text='DOC####????###'),
                issuing_authority=f"{self.fake.country()} {random.choice(['Registry', 'Authority', 'Commission'])}",
                issuing_country=self.fake.country_code(),
                issue_date=issue_date,
                expiry_date=expiry_date,
                verification_status=random.choice(['verified', 'pending', 'expired']),
                verification_date=verification_date,
                document_category=random.choice(['legal', 'regulatory', 'financial', 'compliance']),
                notes=self.fake.text() if random.random() > 0.5 else None
            )
            
            yield document
