# AML Monitoring Tools

This directory contains a collection of powerful tools designed for AML (Anti-Money Laundering) transaction monitoring. Each tool is specialized for different aspects of data access, querying, and API interactions.

## Table of Contents
- [SQL Tools](#sql-tools)
- [Graph Tools](#graph-tools)
- [API Tools](#api-tools)
- [RAG Tool](#rag-tool)
- [Installation](#installation)
- [Common Usage Patterns](#common-usage-patterns)
- [Contributing](#contributing)

## SQL Tools

The SQL tools provide natural language interfaces to SQL databases, making it easy to query and analyze transaction data.

### Capabilities
- Convert natural language to SQL queries
- Execute SQL queries safely
- Handle complex joins and aggregations
- Support batch query execution
- Validate query safety
- Handle query parameters

### Usage Example
```python
from aml_monitoring.flows.tools.sql_tools import TextToSQLTool, AsyncSQLBatchTool

# Initialize tool
sql_tool = TextToSQLTool(
    db_handler=your_db_handler,
    schema_path="path/to/schema.json"
)

# Single query
result = await sql_tool._arun(
    "Find all transactions above $10000 from last week"
)

# Batch queries
batch_tool = AsyncSQLBatchTool(sql_tool)
results = await batch_tool._arun([
    "Get total transaction amount by customer",
    "Find suspicious transactions",
    "List high-risk accounts"
])
```

## Graph Tools

The graph tools enable natural language interactions with Neo4j graph databases, perfect for relationship analysis and pattern detection.

### Capabilities
- Convert natural language to Cypher queries
- Execute graph queries
- Analyze graph patterns
- Handle batch operations
- Validate query safety
- Support complex graph traversals

### Usage Example
```python
from aml_monitoring.flows.tools.graph_tools import TextToCypherTool, AsyncGraphQueryTool

# Initialize tools
cypher_tool = TextToCypherTool(
    graph_dictionary=GraphDataDictionary("graph_schema.csv"),
    db_handler=your_neo4j_handler
)

# Query graph patterns
result = await cypher_tool._arun(
    "Find all accounts connected to high-risk customers"
)

# Pattern analysis
pattern_tool = AsyncGraphPatternTool(cypher_tool)
patterns = await pattern_tool._arun(
    "Analyze transaction patterns between suspicious accounts"
)
```

## API Tools

The API tools provide a generic interface for interacting with REST APIs using Swagger/OpenAPI documentation.

### Capabilities
- Map natural language to API endpoints
- Validate API parameters
- Handle authentication
- Support rate limiting
- Execute parallel requests
- Parse Swagger/OpenAPI docs
- Handle errors gracefully

### Usage Example
```python
from aml_monitoring.flows.tools.api_tools import SwaggerAPITool, AsyncAPIBatchTool

# Initialize tools
api_tool = SwaggerAPITool(
    swagger_url="https://api.example.com/swagger.json",
    base_url="https://api.example.com",
    auth_header={"Authorization": "Bearer token"},
    rate_limit=10
)

# Single API request
result = await api_tool._arun(
    "Get risk score for customer ID 12345"
)

# Batch API requests
batch_tool = AsyncAPIBatchTool(api_tool)
results = await batch_tool._arun([
    "Get all high-risk transactions",
    "Check customer watchlist status",
    "Get account activity summary"
])
```

## RAG Tool

The RAG (Retrieval-Augmented Generation) tool enables agents to leverage document search and querying capabilities.

### Features

- Document upload and processing
- Semantic search across documents
- Similar document finding
- Metadata management
- Secure authentication

### Configuration

Set the following environment variables:

```bash
# Required
RAG_PROJECT_ID=your-project-id
RAG_REGION=your-region
RAG_DOCUMENT_PROCESSOR_URL=https://document-processor-url
RAG_QUERY_SERVICE_URL=https://query-service-url
RAG_BUCKET_NAME=your-bucket-name
RAG_SERVICE_ACCOUNT_PATH=/path/to/service-account.json

# Optional
RAG_MAX_CHUNK_SIZE=1000
RAG_CHUNK_OVERLAP=200
RAG_TOP_K_DEFAULT=5
RAG_EMBEDDING_MODEL=textembedding-gecko@latest
RAG_MAX_RETRIES=3
RAG_TIMEOUT=30
```

### Usage Examples

1. **Initialize the Tool**
```python
from aml_monitoring.flows.tools.rag_tool import RAGTool
from aml_monitoring.flows.tools.rag_config import RAGConfig

config = RAGConfig.from_env()
rag_tool = RAGTool(config.project_id, config.region)
```

2. **Upload Document**
```python
response = rag_tool.upload_document(
    "path/to/document.pdf",
    metadata={
        "type": "policy",
        "department": "compliance"
    }
)
```

3. **Query Documents**
```python
results = rag_tool.query(
    query="What are the main compliance requirements?",
    filters={"type": "policy"},
    top_k=3
)
```

4. **Find Similar Documents**
```python
similar_docs = rag_tool.get_similar_documents(
    document_id="doc123",
    top_k=3
)
```

5. **Update Metadata**
```python
updated = rag_tool.update_metadata(
    document_id="doc123",
    metadata={
        "status": "reviewed",
        "reviewer": "compliance_team"
    }
)
```

### Error Handling

The tool includes built-in error handling for:
- Authentication failures
- API timeouts
- Rate limiting
- Invalid requests

Example:
```python
try:
    results = rag_tool.query("my query")
except requests.exceptions.RequestException as e:
    print(f"Error querying RAG system: {e}")
```

### Security

The tool implements several security features:
- Service account authentication
- Secure token management
- HTTPS communication
- Request signing

### Best Practices

1. **Document Management**
   - Use descriptive metadata
   - Clean up unused documents
   - Monitor storage usage

2. **Querying**
   - Be specific with queries
   - Use filters when possible
   - Monitor query performance

3. **Error Handling**
   - Implement proper retries
   - Log errors appropriately
   - Monitor API usage

### Integration with Agents

Agents can use the RAG tool to:
1. Search compliance documentation
2. Find similar cases
3. Extract relevant policies
4. Update case documentation

Example agent integration:
```python
def handle_compliance_query(query: str) -> str:
    rag_tool = RAGTool(project_id, region)
    
    # Search compliance docs
    results = rag_tool.query(
        query=query,
        filters={"type": "compliance"},
        top_k=3
    )
    
    # Process results
    response = analyze_results(results)
    
    return response
```

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Configure database connections:
```python
# SQL Database
sql_handler = SQLDatabaseHandler(
    connection_string="postgresql://user:pass@localhost:5432/db"
)

# Graph Database
graph_handler = Neo4jHandler(
    uri="neo4j://localhost:7687",
    user="neo4j",
    password="password"
)
```

## Common Usage Patterns

### 1. Combining Multiple Tools
```python
async def analyze_suspicious_activity(customer_id: str):
    # Get customer details from SQL
    customer = await sql_tool._arun(
        f"Get customer details for ID {customer_id}"
    )
    
    # Analyze transaction patterns in graph
    patterns = await graph_tool._arun(
        f"Find unusual transaction patterns for customer {customer_id}"
    )
    
    # Check external risk data via API
    risk_data = await api_tool._arun(
        f"Get risk assessment for customer {customer_id}"
    )
    
    return {
        "customer": customer,
        "patterns": patterns,
        "risk_data": risk_data
    }
```

### 2. Batch Processing
```python
async def batch_risk_analysis(customer_ids: List[str]):
    queries = [
        f"Get risk score for customer {cid}"
        for cid in customer_ids
    ]
    
    return await batch_tool._arun(queries)
```

### 3. Error Handling
```python
try:
    result = await api_tool._arun(query)
    if not result["success"]:
        logger.error(f"API error: {result['error']}")
        # Handle error appropriately
except Exception as e:
    logger.error(f"Execution error: {str(e)}")
    # Handle exception
```

## Contributing

### Adding New Tools
1. Create a new file in the tools directory
2. Implement the tool following the existing patterns
3. Add tests in the tests directory
4. Update this README with documentation

### Improving Existing Tools
1. Add new capabilities
2. Improve error handling
3. Enhance performance
4. Update documentation

### Best Practices
- Follow type hints and docstring conventions
- Add comprehensive error handling
- Include usage examples
- Write unit tests
- Update documentation

## License

This project is licensed under the MIT License - see the LICENSE file for details.
