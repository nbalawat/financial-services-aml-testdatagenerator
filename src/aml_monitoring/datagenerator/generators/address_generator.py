import random
from typing import AsyncGenerator
from uuid import uuid4, UUID
from datetime import datetime, timedelta

from ..models import Address
from .base_generator import BaseGenerator

class AddressGenerator(BaseGenerator):
    """Generator for address data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[Address, None]:
        """Generate addresses for a given entity."""
        num_addresses = random.randint(
            self.config.get('min_addresses_per_entity', 1),
            self.config.get('max_addresses_per_entity', 3)
        )
        
        for _ in range(num_addresses):
            # Generate dates
            effective_from = (datetime.now() - timedelta(days=random.randint(365, 730))).strftime('%Y-%m-%d')
            effective_to = None if random.random() > 0.2 else (
                datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            last_verified = (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d')
            
            # Convert UUID to string if needed
            str_entity_id = str(entity_id) if isinstance(entity_id, UUID) else entity_id

            address = Address(
                address_id=str(uuid4()),
                entity_id=str_entity_id,
                entity_type=entity_type,
                address_type=random.choice(['registered', 'business', 'mailing']),
                address_line1=self.fake.street_address(),
                address_line2=self.fake.secondary_address() if random.random() > 0.5 else None,
                city=self.fake.city(),
                state_province=self.fake.state(),
                postal_code=self.fake.postcode(),
                country=self.fake.country_code(),
                status=random.choice(['active', 'inactive', 'pending']),
                effective_from=effective_from,
                effective_to=effective_to,
                primary_address=random.choice([True, False]),
                validation_status=random.choice(['validated', 'pending', 'failed']),
                last_verified=last_verified,
                latitude=float(self.fake.latitude()),
                longitude=float(self.fake.longitude()),
                timezone=self.fake.timezone()
            )
            
            yield address
