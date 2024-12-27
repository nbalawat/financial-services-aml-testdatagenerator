import random
from typing import AsyncGenerator
from uuid import uuid4, UUID
from datetime import datetime, timedelta

from ..models import BeneficialOwner
from .base_generator import BaseGenerator

class BeneficialOwnerGenerator(BaseGenerator):
    """Generator for beneficial owner data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[BeneficialOwner, None]:
        """Generate beneficial owners for a given entity."""
        num_owners = random.randint(
            self.config.get('min_owners_per_entity', 1),
            self.config.get('max_owners_per_entity', 5)
        )
        
        for _ in range(num_owners):
            # Generate dates
            dob = self.fake.date_between(start_date='-80y', end_date='-25y').strftime('%Y-%m-%d')
            verification_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
            
            # Convert UUID to string if needed
            str_entity_id = str(entity_id) if isinstance(entity_id, UUID) else entity_id

            owner = BeneficialOwner(
                owner_id=str(uuid4()),
                entity_id=str_entity_id,
                entity_type=entity_type,
                name=self.fake.name(),
                nationality=self.fake.country_code(),
                country_of_residence=self.fake.country_code(),
                ownership_percentage=round(random.uniform(5, 100), 2),
                dob=dob,
                verification_date=verification_date,
                pep_status=random.choice([True, False]),
                sanctions_status=random.choice([True, False])
            )
            
            yield owner
