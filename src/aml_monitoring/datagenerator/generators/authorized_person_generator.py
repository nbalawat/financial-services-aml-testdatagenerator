import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import AuthorizedPerson
from .base_generator import BaseGenerator

class AuthorizedPersonGenerator(BaseGenerator):
    """Generator for authorized person data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[AuthorizedPerson, None]:
        """Generate authorized persons for a given entity."""
        num_persons = random.randint(
            self.config.get('min_authorized_persons_per_entity', 1),
            self.config.get('max_authorized_persons_per_entity', 5)
        )
        
        for _ in range(num_persons):
            # Generate dates
            authorization_start = self.fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            authorization_end = None if random.random() > 0.2 else (
                datetime.now() + timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d')
            last_verification_date = (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d')
            
            person = AuthorizedPerson(
                person_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                name=self.fake.name(),
                title=random.choice([
                    'Chief Executive Officer',
                    'Chief Financial Officer',
                    'Chief Risk Officer',
                    'Chief Compliance Officer',
                    'Director',
                    'Senior Manager',
                    'Authorized Representative'
                ]),
                authorization_level=random.choice(['full', 'limited', 'restricted']),
                authorization_type=random.choice(['signatory', 'trader', 'administrator']),
                authorization_start=authorization_start,
                authorization_end=authorization_end,
                contact_info={
                    'email': self.fake.email(),
                    'phone': self.fake.phone_number(),
                    'office': self.fake.phone_number()
                },
                is_active=random.choice([True, False]),
                last_verification_date=last_verification_date,
                nationality=self.fake.country_code()
            )
            
            yield person
