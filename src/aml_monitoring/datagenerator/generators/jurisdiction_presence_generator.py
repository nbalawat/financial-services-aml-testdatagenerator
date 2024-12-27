import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import JurisdictionPresence
from .base_generator import BaseGenerator

class JurisdictionPresenceGenerator(BaseGenerator):
    """Generator for jurisdiction presence data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[JurisdictionPresence, None]:
        """Generate a jurisdiction presence for a given entity."""
        while True:
            # Generate dates
            registration_date = self.fake.date_between(start_date='-5y', end_date='-1y').strftime('%Y-%m-%d')
            effective_from = registration_date
            effective_to = None if random.random() > 0.1 else (
                datetime.now() + timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d')
            local_registration_date = (datetime.strptime(registration_date, '%Y-%m-%d') + 
                                     timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
            
            presence = JurisdictionPresence(
                presence_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                jurisdiction=self.fake.country_code(),
                registration_date=registration_date,
                effective_from=effective_from,
                status=random.choice(['active', 'pending', 'terminated']),
                local_registration_id=self.fake.bothify(text='REG####????###'),
                effective_to=effective_to,
                local_registration_date=local_registration_date,
                local_registration_authority=f"{self.fake.country()} Business Registry",
                notes=self.fake.text() if random.random() > 0.5 else None
            )
            
            yield presence
