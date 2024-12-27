"""Main AML monitoring flow implementation."""

from crewai.flow.flow import Flow, listen, start
from .crews.transaction_crew.transaction_crew import TransactionCrew
from .crews.customer_crew.customer_crew import CustomerCrew
from .crews.alert_crew.alert_crew import AlertCrew
from .crews.document_crew.document_crew import DocumentCrew

class AMLMonitoringFlow(Flow):
    def __init__(self):
        super().__init__()
        self.transaction_crew = TransactionCrew()
        self.customer_crew = CustomerCrew()
        self.alert_crew = AlertCrew()
        self.document_crew = DocumentCrew()

    @start()
    def investigate_transaction(self, transaction_id: str):
        """Start transaction investigation"""
        return self.transaction_crew.investigate_transaction(transaction_id)

    @listen(investigate_transaction)
    def perform_customer_due_diligence(self, transaction_results):
        """Perform customer due diligence based on transaction results"""
        customer_id = transaction_results.get('customer_id')
        return self.customer_crew.perform_due_diligence(customer_id)

    @listen(investigate_transaction)
    def investigate_alerts(self, transaction_results):
        """Investigate related alerts"""
        alert_ids = transaction_results.get('alert_ids', [])
        results = []
        for alert_id in alert_ids:
            result = self.alert_crew.investigate_alert(alert_id)
            results.append(result)
        return results

    @listen([perform_customer_due_diligence, investigate_alerts])
    def analyze_related_documents(self, due_diligence_results, alert_results):
        """Analyze all related documents"""
        document_ids = self._gather_document_ids(due_diligence_results, alert_results)
        return self.document_crew.analyze_documents(document_ids)

    def _gather_document_ids(self, due_diligence_results, alert_results):
        """Helper method to gather document IDs from various results."""
        document_ids = set()
        
        # Extract document IDs from due diligence results
        if due_diligence_results:
            doc_ids = due_diligence_results.get('document_ids', [])
            document_ids.update(doc_ids)
        
        # Extract document IDs from alert results
        for alert_result in alert_results:
            if alert_result:
                doc_ids = alert_result.get('document_ids', [])
                document_ids.update(doc_ids)
        
        return list(document_ids)


def main():
    """Main entry point for the AML monitoring flow."""
    flow = AMLMonitoringFlow()
    result = flow.kickoff(transaction_id="TX123456")
    print(f"Investigation Results: {result}")


if __name__ == "__main__":
    main()
