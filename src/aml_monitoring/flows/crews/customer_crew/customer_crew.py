"""Customer due diligence crew implementation."""

from pathlib import Path
from crewai import Crew
from crewai.tasks import Task
from crewai.agents import Agent
import yaml

class CustomerCrew:
    def __init__(self):
        self.config_path = Path(__file__).parent / "config"
        self.agents = self._load_agents()
        self.tasks = self._load_tasks()
        self.crew = Crew(
            agents=self.agents,
            tasks=self.tasks,
            verbose=True
        )

    def _load_agents(self):
        """Load agent configurations from YAML."""
        with open(self.config_path / "agents.yaml", "r") as f:
            config = yaml.safe_load(f)
        return [Agent(**agent_config) for agent_config in config["agents"]]

    def _load_tasks(self):
        """Load task configurations from YAML."""
        with open(self.config_path / "tasks.yaml", "r") as f:
            config = yaml.safe_load(f)
        return [Task(**task_config) for task_config in config["tasks"]]

    def perform_due_diligence(self, customer_id: str):
        """Execute the customer due diligence process."""
        context = {"customer_id": customer_id}
        for task in self.tasks:
            task.context.update(context)
        
        return self.crew.kickoff()
