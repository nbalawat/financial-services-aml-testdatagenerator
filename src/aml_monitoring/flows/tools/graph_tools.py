"""Tools for querying graph databases using natural language."""

import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from langchain.tools import BaseTool
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from ..database.base import FlowDatabaseHandler
import asyncio

class CypherQueryResult(BaseModel):
    """Structured output for Cypher query generation."""
    cypher: str = Field(description="The generated Cypher query")
    explanation: str = Field(description="Explanation of what the query does")
    nodes_used: List[str] = Field(description="List of node types used in the query")
    relationships_used: List[str] = Field(description="List of relationships used in the query")
    potential_risks: List[str] = Field(description="Potential risks or considerations")

class GraphDataDictionary:
    """Graph database schema and relationship information."""
    
    def __init__(self, csv_path: str):
        self.nodes = {}
        self.relationships = {}
        self.patterns = {}
        self._load_dictionary(csv_path)
        self._infer_patterns()
    
    def _load_dictionary(self, csv_path: str):
        """Load and parse the graph data dictionary CSV."""
        df = pd.read_csv(csv_path)
        
        for _, row in df.iterrows():
            if row['Type'] == 'Node':
                node_type = row['Name']
                if node_type not in self.nodes:
                    self.nodes[node_type] = {
                        'properties': [],
                        'labels': row.get('Labels', '').split(','),
                        'description': row.get('Description', '')
                    }
                self.nodes[node_type]['properties'].append({
                    'name': row['Property'],
                    'type': row['DataType'],
                    'description': row.get('Description', '')
                })
            elif row['Type'] == 'Relationship':
                rel_type = row['Name']
                self.relationships[rel_type] = {
                    'start_node': row['StartNode'],
                    'end_node': row['EndNode'],
                    'properties': row.get('Properties', '').split(','),
                    'description': row.get('Description', '')
                }
    
    def _infer_patterns(self):
        """Infer common graph patterns from nodes and relationships."""
        for rel_type, rel_info in self.relationships.items():
            start_node = rel_info['start_node']
            end_node = rel_info['end_node']
            
            # Create pattern for direct relationship
            pattern_key = f"{start_node}_TO_{end_node}"
            self.patterns[pattern_key] = {
                'cypher': f"MATCH (a:{start_node})-[r:{rel_type}]->(b:{end_node})",
                'description': f"Direct relationship from {start_node} to {end_node} via {rel_type}"
            }
            
            # Create pattern for reverse relationship
            pattern_key = f"{end_node}_FROM_{start_node}"
            self.patterns[pattern_key] = {
                'cypher': f"MATCH (b:{end_node})<-[r:{rel_type}]-(a:{start_node})",
                'description': f"Reverse relationship to {end_node} from {start_node} via {rel_type}"
            }
            
            # Create pattern for path finding
            pattern_key = f"PATH_{start_node}_TO_{end_node}"
            self.patterns[pattern_key] = {
                'cypher': f"MATCH p=shortestPath((a:{start_node})-[*..5]->(b:{end_node}))",
                'description': f"Find shortest path from {start_node} to {end_node}"
            }

class CypherValidator:
    """Cypher query validator."""
    
    FORBIDDEN_PATTERNS = [
        r'\bDELETE\b',
        r'\bREMOVE\b',
        r'\bSET\b',
        r'\bCREATE\b',
        r'\bMERGE\b',
        r'\bDROP\b',
        r';.*?;'  # Multiple statements
    ]
    
    @staticmethod
    def validate_query(query: str) -> Tuple[bool, str]:
        """
        Validate Cypher query for safety and correctness.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for forbidden patterns
        for pattern in CypherValidator.FORBIDDEN_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return False, f"Query contains forbidden pattern: {pattern}"
        
        # Ensure query starts with MATCH or CALL
        if not re.match(r'^\s*(MATCH|CALL)\b', query, re.IGNORECASE):
            return False, "Query must start with MATCH or CALL"
        
        # Basic syntax validation
        try:
            # Validate parentheses matching
            if query.count('(') != query.count(')'):
                return False, "Mismatched parentheses in query"
            
            # Validate square brackets matching
            if query.count('[') != query.count(']'):
                return False, "Mismatched square brackets in query"
            
            # Validate basic Cypher structure
            if not re.search(r'\bRETURN\b', query, re.IGNORECASE):
                return False, "Query must include RETURN clause"
        except Exception as e:
            return False, f"Syntax validation failed: {str(e)}"
        
        return True, ""

class TextToCypherTool(BaseTool):
    """Convert natural language to Cypher queries."""
    
    name = "TextToCypher"
    description = "Convert natural language queries to Cypher based on the graph data dictionary"
    
    def __init__(self, graph_dictionary: GraphDataDictionary, db_handler: FlowDatabaseHandler):
        super().__init__()
        self.graph_dictionary = graph_dictionary
        self.db_handler = db_handler
        self.llm = ChatOpenAI(temperature=0)
        self.output_parser = PydanticOutputParser(pydantic_object=CypherQueryResult)
        self.validator = CypherValidator()
    
    def _create_cypher_prompt(self, query: str) -> str:
        """Create a detailed prompt for Cypher generation."""
        nodes_info = "\n".join([
            f"Node Type: {node_type}\n"
            f"Labels: {', '.join(info['labels'])}\n"
            f"Properties: {', '.join(p['name'] for p in info['properties'])}\n"
            f"Description: {info['description']}\n"
            for node_type, info in self.graph_dictionary.nodes.items()
        ])
        
        relationships_info = "\n".join([
            f"Relationship: {rel_type}\n"
            f"From {info['start_node']} To {info['end_node']}\n"
            f"Properties: {', '.join(info['properties'])}\n"
            f"Description: {info['description']}\n"
            for rel_type, info in self.graph_dictionary.relationships.items()
        ])
        
        common_patterns = "\n".join([
            f"Pattern: {pattern}\n{info['description']}\nCypher: {info['cypher']}\n"
            for pattern, info in self.graph_dictionary.patterns.items()
        ])
        
        return f"""
        Convert this natural language query to a Cypher query for Neo4j.
        
        Available Node Types and Properties:
        {nodes_info}
        
        Available Relationships:
        {relationships_info}
        
        Common Patterns:
        {common_patterns}
        
        Natural Language Query: {query}
        
        Guidelines:
        1. Use MATCH patterns for graph traversal
        2. Use WHERE clauses for filtering
        3. Return specific properties using RETURN
        4. Use appropriate relationship directions
        5. Consider using common patterns when applicable
        6. Only use READ operations (no CREATE, DELETE, SET, etc.)
        7. Use parameters for values where appropriate
        8. Consider using aggregation functions when needed
        9. Add LIMIT clause for large result sets
        10. Use path finding when exploring connections
        
        {self.output_parser.get_format_instructions()}
        """
    
    def _format_results(self, results: List[Dict[str, Any]], cypher_result: CypherQueryResult) -> Dict[str, Any]:
        """Format query results with explanations."""
        return {
            "query": cypher_result.cypher,
            "explanation": cypher_result.explanation,
            "nodes_used": cypher_result.nodes_used,
            "relationships_used": cypher_result.relationships_used,
            "potential_risks": cypher_result.potential_risks,
            "results": results,
            "result_count": len(results),
            "metadata": {
                "has_more": len(results) >= 1000,  # Indicate if results might be truncated
                "execution_time": None  # Will be filled by the database handler
            }
        }
    
    async def _arun(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Async implementation of natural language to Cypher conversion and execution.
        
        Args:
            query: Natural language query
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict containing query results and metadata
        """
        prompt = ChatPromptTemplate.from_template(self._create_cypher_prompt(query))
        
        for attempt in range(max_retries):
            try:
                # Generate Cypher query
                messages = prompt.format_messages(query=query)
                response = await self.llm.agenerate([messages])
                cypher_result = self.output_parser.parse(response.generations[0][0].text)
                
                # Validate query
                is_valid, error_message = self.validator.validate_query(cypher_result.cypher)
                if not is_valid:
                    raise ValueError(f"Invalid Cypher query: {error_message}")
                
                # Execute query
                results = await self.db_handler.execute_cypher(cypher_result.cypher)
                
                return self._format_results(results, cypher_result)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                continue
    
    def _run(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Convert natural language to Cypher and execute it with retries.
        
        Args:
            query: Natural language query
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict containing query results and metadata
        """
        return asyncio.run(self._arun(query, max_retries))

class AsyncGraphQueryTool(BaseTool):
    """Asynchronously query graph database using natural language."""
    
    name = "AsyncGraphQuery"
    description = "Asynchronously query the graph database using natural language"
    
    def __init__(self, text_to_cypher: TextToCypherTool):
        super().__init__()
        self.text_to_cypher = text_to_cypher
    
    def _run(self, query: str) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun")
    
    async def _arun(self, query: str) -> Dict[str, Any]:
        """Asynchronously query graph database using natural language."""
        return await self.text_to_cypher._arun(query)

class AsyncGraphPatternTool(BaseTool):
    """Asynchronously analyze graph patterns using natural language."""
    
    name = "AsyncGraphPattern"
    description = "Asynchronously analyze patterns in the graph database using natural language"
    
    def __init__(self, text_to_cypher: TextToCypherTool):
        super().__init__()
        self.text_to_cypher = text_to_cypher
    
    def _run(self, query: str) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun")
    
    async def _arun(self, query: str) -> Dict[str, Any]:
        """Asynchronously analyze graph patterns using natural language."""
        # Enhance the query with pattern analysis directives
        enhanced_query = f"""
        Analyze patterns in the graph: {query}
        Consider:
        1. Path patterns
        2. Relationship frequencies
        3. Node centrality
        4. Common subgraphs
        5. Temporal patterns if applicable
        """
        return await self.text_to_cypher._arun(enhanced_query)

class AsyncGraphBatchQueryTool(BaseTool):
    """Execute multiple graph queries in parallel."""
    
    name = "AsyncGraphBatchQuery"
    description = "Execute multiple natural language graph queries in parallel"
    
    def __init__(self, text_to_cypher: TextToCypherTool):
        super().__init__()
        self.text_to_cypher = text_to_cypher
    
    def _run(self, queries: List[str]) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun")
    
    async def _arun(self, queries: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute multiple natural language queries in parallel.
        
        Args:
            queries: List of natural language queries
            
        Returns:
            Dict containing results for all queries
        """
        tasks = [self.text_to_cypher._arun(query) for query in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            "results": [
                result if not isinstance(result, Exception) else {"error": str(result)}
                for result in results
            ],
            "success_count": sum(1 for r in results if not isinstance(r, Exception)),
            "error_count": sum(1 for r in results if isinstance(r, Exception))
        }
