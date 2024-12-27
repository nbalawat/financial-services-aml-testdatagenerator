"""Database tools for AML monitoring with Text-to-SQL capabilities."""

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

class SQLValidationError(Exception):
    """Raised when SQL validation fails."""
    pass

class SQLQueryResult(BaseModel):
    """Structured output for SQL generation."""
    sql: str = Field(description="The generated SQL query")
    explanation: str = Field(description="Explanation of what the query does")
    tables_used: List[str] = Field(description="List of tables used in the query")
    potential_risks: List[str] = Field(description="Potential risks or considerations")

class DataDictionary:
    def __init__(self, csv_path: str):
        self.tables = {}
        self.relationships = {}
        self._load_dictionary(csv_path)
        self._infer_relationships()

    def _load_dictionary(self, csv_path: str):
        """Load and parse the data dictionary CSV."""
        df = pd.read_csv(csv_path)
        
        for _, row in df.iterrows():
            table = row['Table']
            if table not in self.tables:
                self.tables[table] = {'columns': []}
            
            self.tables[table]['columns'].append({
                'name': row['Column'],
                'type': row['Data Type'],
                'required': row['Required'],
                'description': row['Description'],
                'constraints': row['Constraints'],
                'examples': row['Examples']
            })

    def _infer_relationships(self):
        """Infer relationships between tables based on column names and types."""
        for table, info in self.tables.items():
            self.relationships[table] = []
            for column in info['columns']:
                # Look for foreign key patterns
                if column['name'].endswith('_id') and column['name'] != f"{table.lower()}_id":
                    referenced_table = column['name'].replace('_id', '')
                    if referenced_table in self.tables:
                        self.relationships[table].append({
                            'referenced_table': referenced_table,
                            'column': column['name'],
                            'type': 'many_to_one'
                        })

    def get_table_schema(self, table: str) -> Dict:
        """Get schema information for a specific table."""
        return self.tables.get(table, {})

    def get_all_tables(self) -> List[str]:
        """Get list of all tables."""
        return list(self.tables.keys())

    def get_related_tables(self, table: str) -> List[Dict]:
        """Get tables related to the specified table."""
        return self.relationships.get(table, [])

class SQLValidator:
    """SQL query validator."""
    
    FORBIDDEN_PATTERNS = [
        r'\bDROP\b',
        r'\bDELETE\b',
        r'\bTRUNCATE\b',
        r'\bUPDATE\b',
        r'\bINSERT\b',
        r'\bALTER\b',
        r'\bCREATE\b',
        r';.*?;'  # Multiple statements
    ]

    @staticmethod
    def validate_query(query: str) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL query for safety and correctness.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for forbidden patterns
        for pattern in SQLValidator.FORBIDDEN_PATTERNS:
            if re.search(pattern, query, re.IGNORECASE):
                return False, f"Query contains forbidden pattern: {pattern}"

        # Check for basic SQL injection patterns
        if "'" in query and "''" not in query:
            return False, "Query contains unescaped single quotes"

        # Check for balanced parentheses
        if query.count('(') != query.count(')'):
            return False, "Query contains unbalanced parentheses"

        return True, None

class TextToSQLTool(BaseTool):
    name = "TextToSQL"
    description = "Convert natural language queries to SQL based on the data dictionary"

    def __init__(self, data_dictionary: DataDictionary, db_handler: FlowDatabaseHandler):
        super().__init__()
        self.data_dictionary = data_dictionary
        self.db_handler = db_handler
        self.llm = ChatOpenAI(temperature=0)
        self.output_parser = PydanticOutputParser(pydantic_object=SQLQueryResult)

    def _create_sql_prompt(self, query: str) -> str:
        """Create a detailed prompt for SQL generation."""
        # Build schema information
        schema_info = []
        for table, info in self.data_dictionary.tables.items():
            schema_info.append(f"\nTable: {table}")
            for col in info['columns']:
                schema_info.append(
                    f"  - {col['name']} ({col['type']})"
                    f"\n    Description: {col['description']}"
                    f"\n    Required: {col['required']}"
                    f"\n    Constraints: {col['constraints']}"
                )

        # Build relationships information
        relationship_info = []
        for table, relationships in self.data_dictionary.relationships.items():
            for rel in relationships:
                relationship_info.append(
                    f"- {table} -> {rel['referenced_table']} "
                    f"through {rel['column']} ({rel['type']})"
                )

        prompt = f"""Given the following database schema and relationships:

Schema:
{chr(10).join(schema_info)}

Relationships:
{chr(10).join(relationship_info)}

Task: Convert this question to SQL: {query}

Requirements:
1. Use proper PostgreSQL syntax
2. Join tables when necessary using appropriate keys
3. Include relevant WHERE clauses
4. Use appropriate data types and operators
5. Consider performance implications
6. Include comments explaining complex parts
7. Format the SQL query for readability

Please provide:
1. The SQL query
2. An explanation of what the query does
3. List of tables used
4. Any potential risks or considerations

{self.output_parser.get_format_instructions()}
"""
        return prompt

    def _format_results(self, results: List[Dict[str, Any]], sql_result: SQLQueryResult) -> Dict[str, Any]:
        """Format query results with explanations."""
        try:
            # Convert results to pandas DataFrame for analysis
            df = pd.DataFrame(results)
            
            summary = {
                "sql_query": sql_result.sql,
                "explanation": sql_result.explanation,
                "tables_used": sql_result.tables_used,
                "potential_risks": sql_result.potential_risks,
                "row_count": len(results),
                "results": results,
                "summary_stats": {}
            }

            if not df.empty:
                # Generate summary statistics for numeric columns
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                if not numeric_cols.empty:
                    summary["summary_stats"] = {
                        col: {
                            "mean": df[col].mean(),
                            "min": df[col].min(),
                            "max": df[col].max(),
                            "null_count": df[col].isnull().sum()
                        } for col in numeric_cols
                    }

            return summary
        except Exception as e:
            return {
                "sql_query": sql_result.sql,
                "explanation": sql_result.explanation,
                "error": f"Error formatting results: {str(e)}",
                "raw_results": results
            }

    def _run(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Convert natural language to SQL and execute it with retries.
        
        Args:
            query: Natural language query
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict containing query results and metadata
        """
        for attempt in range(max_retries):
            try:
                # Generate SQL using LLM
                prompt = self._create_sql_prompt(query)
                response = self.llm.predict(prompt)
                sql_result = self.output_parser.parse(response)

                # Validate the SQL
                is_valid, error_message = SQLValidator.validate_query(sql_result.sql)
                if not is_valid:
                    raise SQLValidationError(error_message)

                # Execute the query
                results = self.db_handler.execute_query_sync(sql_result.sql)

                # Format and return results
                return self._format_results(results, sql_result)

            except Exception as e:
                if attempt == max_retries - 1:
                    return {
                        "error": str(e),
                        "sql": sql_result.sql if 'sql_result' in locals() else None,
                        "attempt": attempt + 1,
                        "max_retries": max_retries
                    }
                continue

    async def _arun(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """
        Async implementation of natural language to SQL conversion and execution.
        
        Args:
            query: Natural language query
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict containing query results and metadata
        """
        for attempt in range(max_retries):
            try:
                # Generate SQL using LLM
                prompt = self._create_sql_prompt(query)
                response = await self.llm.apredict(prompt)
                sql_result = self.output_parser.parse(response)

                # Validate the SQL
                is_valid, error_message = SQLValidator.validate_query(sql_result.sql)
                if not is_valid:
                    raise SQLValidationError(error_message)

                # Execute the query asynchronously
                results = await self.db_handler.execute_query(sql_result.sql)

                # Format and return results
                return self._format_results(results, sql_result)

            except Exception as e:
                if attempt == max_retries - 1:
                    return {
                        "error": str(e),
                        "sql": sql_result.sql if 'sql_result' in locals() else None,
                        "attempt": attempt + 1,
                        "max_retries": max_retries
                    }
                continue

class QueryTransactionsTool(BaseTool):
    name = "QueryTransactions"
    description = "Query transaction details from the database using natural language"
    
    def __init__(self, text_to_sql: TextToSQLTool):
        super().__init__()
        self.text_to_sql = text_to_sql

    def _run(self, query: str) -> Dict[str, Any]:
        """Query transaction information using natural language."""
        enhanced_query = (
            f"Regarding transactions: {query}. "
            "Focus on the Transaction table and its related tables. "
            "Consider transaction patterns, amounts, and frequencies."
        )
        return self.text_to_sql._run(enhanced_query)

class AnalyzePatternsTool(BaseTool):
    name = "AnalyzePatterns"
    description = "Analyze transaction patterns in the database using natural language queries"
    
    def __init__(self, text_to_sql: TextToSQLTool):
        super().__init__()
        self.text_to_sql = text_to_sql

    def _run(self, query: str) -> Dict[str, Any]:
        """Analyze transaction patterns using natural language."""
        enhanced_query = (
            f"Analyze patterns in transactions: {query}. "
            "Include statistical analysis where appropriate. "
            "Consider temporal patterns, amount distributions, and entity relationships."
        )
        return self.text_to_sql._run(enhanced_query)

class AsyncQueryTransactionsTool(BaseTool):
    name = "AsyncQueryTransactions"
    description = "Asynchronously query transaction details from the database using natural language"
    
    def __init__(self, text_to_sql: TextToSQLTool):
        super().__init__()
        self.text_to_sql = text_to_sql

    def _run(self, query: str) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun for async operation")

    async def _arun(self, query: str) -> Dict[str, Any]:
        """Asynchronously query transaction information using natural language."""
        enhanced_query = (
            f"Regarding transactions: {query}. "
            "Focus on the Transaction table and its related tables. "
            "Consider transaction patterns, amounts, and frequencies."
        )
        return await self.text_to_sql._arun(enhanced_query)

class AsyncAnalyzePatternsTool(BaseTool):
    name = "AsyncAnalyzePatterns"
    description = "Asynchronously analyze transaction patterns in the database using natural language queries"
    
    def __init__(self, text_to_sql: TextToSQLTool):
        super().__init__()
        self.text_to_sql = text_to_sql

    def _run(self, query: str) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun for async operation")

    async def _arun(self, query: str) -> Dict[str, Any]:
        """Asynchronously analyze transaction patterns using natural language."""
        enhanced_query = (
            f"Analyze patterns in transactions: {query}. "
            "Include statistical analysis where appropriate. "
            "Consider temporal patterns, amount distributions, and entity relationships."
        )
        return await self.text_to_sql._arun(enhanced_query)

class AsyncBatchQueryTool(BaseTool):
    name = "AsyncBatchQuery"
    description = "Execute multiple natural language queries in parallel"
    
    def __init__(self, text_to_sql: TextToSQLTool):
        super().__init__()
        self.text_to_sql = text_to_sql

    def _run(self, queries: List[str]) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun for async operation")

    async def _arun(self, queries: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute multiple natural language queries in parallel.
        
        Args:
            queries: List of natural language queries
            
        Returns:
            Dict containing results for all queries
        """
        try:
            # Process all queries in parallel
            tasks = [self.text_to_sql._arun(query) for query in queries]
            results = await asyncio.gather(*tasks)
            
            return {
                "batch_results": results,
                "successful_queries": sum(1 for r in results if "error" not in r),
                "failed_queries": sum(1 for r in results if "error" in r),
                "execution_time": None  # You can add timing information if needed
            }
            
        except Exception as e:
            return {
                "error": f"Batch query execution failed: {str(e)}",
                "batch_results": None
            }
