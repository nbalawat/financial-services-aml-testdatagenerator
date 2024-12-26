import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import ComplianceEvent, ComplianceEventType
from .base_generator import BaseGenerator

class ComplianceEventGenerator(BaseGenerator):
    """Generator for compliance event data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[ComplianceEvent, None]:
        """Generate compliance events for a given entity."""
        num_events = random.randint(
            self.config.get('min_events_per_entity', 1),
            self.config.get('max_events_per_entity', 5)
        )
        
        for _ in range(num_events):
            # Generate dates
            event_date = self.fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
            decision_date = (datetime.strptime(event_date, '%Y-%m-%d') + 
                           timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
            next_review_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            
            event_type = random.choice(list(ComplianceEventType))
            
            # Generate appropriate old and new states based on event type
            old_state = None
            new_state = None
            if event_type == ComplianceEventType.RISK_LEVEL_CHANGE:
                old_state = random.choice(['low', 'medium', 'high'])
                new_state = random.choice(['low', 'medium', 'high'])
                while new_state == old_state:
                    new_state = random.choice(['low', 'medium', 'high'])
            elif event_type == ComplianceEventType.KYC_UPDATE:
                old_state = 'outdated'
                new_state = 'updated'
            else:
                new_state = random.choice(['completed', 'pending', 'failed'])
            
            event = ComplianceEvent(
                event_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                event_date=event_date,
                event_type=event_type,
                event_description=self.fake.sentence(),
                old_state=old_state,
                new_state=new_state,
                decision=random.choice(['approved', 'rejected', 'pending']) if random.random() > 0.3 else None,
                decision_date=decision_date if random.random() > 0.3 else None,
                decision_maker=self.fake.name() if random.random() > 0.3 else None,
                next_review_date=next_review_date if random.random() > 0.5 else None,
                related_account_id=str(uuid4()),
                notes=self.fake.text() if random.random() > 0.5 else None
            )
            
            yield event
