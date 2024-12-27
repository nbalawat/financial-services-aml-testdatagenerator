from abc import ABC, abstractmethod
from typing import Dict, Any, AsyncGenerator
from faker import Faker

class BaseGenerator(ABC):
    """Base class for all data generators."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the generator with configuration."""
        self.config = config
        self.fake = Faker()
    
    @abstractmethod
    async def generate(self, *args, **kwargs) -> AsyncGenerator:
        """Generate data based on the provided arguments."""
        pass
