import random
from typing import AsyncGenerator
from uuid import uuid4
from datetime import datetime, timedelta

from ..models import RiskAssessment, RiskRating
from .base_generator import BaseGenerator

class RiskAssessmentGenerator(BaseGenerator):
    """Generator for risk assessment data."""

    async def generate(self, entity_id: str, entity_type: str) -> AsyncGenerator[RiskAssessment, None]:
        """Generate risk assessments for a given entity."""
        num_assessments = random.randint(
            self.config.get('min_assessments_per_entity', 1),
            self.config.get('max_assessments_per_entity', 3)
        )
        
        for _ in range(num_assessments):
            # Generate dates
            assessment_date = self.fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
            next_review_date = (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
            
            risk_factors = {
                'geographic': random.randint(1, 5),
                'product': random.randint(1, 5),
                'client': random.randint(1, 5),
                'transaction': random.randint(1, 5),
                'regulatory': random.randint(1, 5)
            }
            
            assessment = RiskAssessment(
                assessment_id=str(uuid4()),
                entity_id=str(entity_id),
                entity_type=entity_type,
                assessment_date=assessment_date,
                risk_rating=random.choice(list(RiskRating)),
                risk_score=str(sum(risk_factors.values())),
                assessment_type=random.choice(['initial', 'periodic', 'triggered']),
                risk_factors=risk_factors,
                conducted_by=self.fake.name(),
                approved_by=self.fake.name(),
                findings=random.choice([
                    'No significant findings',
                    'Minor compliance gaps identified',
                    'Requires enhanced monitoring',
                    'Significant risks identified'
                ]),
                assessor=self.fake.name(),
                next_review_date=next_review_date,
                notes=self.fake.text() if random.random() > 0.5 else None
            )
            
            yield assessment
