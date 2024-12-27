"""Tools for handling API interactions using Swagger/OpenAPI documentation."""

import json
import yaml
import re
import aiohttp
import asyncio
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from langchain.tools import BaseTool
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
import urllib.parse

class APIEndpointMatch(BaseModel):
    """Structured output for API endpoint matching."""
    path: str = Field(description="The matched API endpoint path")
    method: str = Field(description="HTTP method (GET, POST, etc.)")
    parameters: Dict[str, Any] = Field(description="Parameters to be used in the request")
    confidence_score: float = Field(description="Confidence score for the match (0-1)")
    required_params: List[str] = Field(description="List of required parameters")
    optional_params: List[str] = Field(description="List of optional parameters")
    description: str = Field(description="Description of what the endpoint does")

class APIResponse(BaseModel):
    """Structured API response."""
    status_code: int = Field(description="HTTP status code")
    success: bool = Field(description="Whether the request was successful")
    data: Any = Field(description="Response data")
    error_message: Optional[str] = Field(description="Error message if request failed")
    execution_time: float = Field(description="Time taken to execute the request")
    endpoint: str = Field(description="The endpoint that was called")
    method: str = Field(description="The HTTP method used")

class SwaggerParser:
    """Parser for Swagger/OpenAPI documentation."""
    
    def __init__(self, swagger_content: Dict[str, Any]):
        """Initialize parser with Swagger content."""
        self.swagger = swagger_content
        self.base_path = swagger_content.get('basePath', '')
        self.paths = swagger_content.get('paths', {})
        self.definitions = swagger_content.get('definitions', {})
        
    def get_endpoint_info(self, path: str, method: str) -> Dict[str, Any]:
        """Get detailed information about an endpoint."""
        if path not in self.paths:
            raise ValueError(f"Path {path} not found in API documentation")
            
        path_info = self.paths[path]
        if method.lower() not in path_info:
            raise ValueError(f"Method {method} not supported for path {path}")
            
        return path_info[method.lower()]
    
    def get_required_parameters(self, endpoint_info: Dict[str, Any]) -> List[str]:
        """Get list of required parameters for an endpoint."""
        required = []
        for param in endpoint_info.get('parameters', []):
            if param.get('required', False):
                required.append(param['name'])
        return required
    
    def get_parameter_schema(self, endpoint_info: Dict[str, Any]) -> Dict[str, Any]:
        """Get parameter schema for an endpoint."""
        schema = {}
        for param in endpoint_info.get('parameters', []):
            schema[param['name']] = {
                'type': param.get('type', 'string'),
                'required': param.get('required', False),
                'in': param.get('in', 'query'),
                'description': param.get('description', ''),
                'format': param.get('format', None)
            }
        return schema
    
    def validate_parameters(self, path: str, method: str, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate parameters against endpoint schema."""
        try:
            endpoint_info = self.get_endpoint_info(path, method)
            required_params = self.get_required_parameters(endpoint_info)
            schema = self.get_parameter_schema(endpoint_info)
            
            # Check required parameters
            for param in required_params:
                if param not in params:
                    return False, f"Missing required parameter: {param}"
            
            # Validate parameter types and formats
            for param_name, param_value in params.items():
                if param_name in schema:
                    param_schema = schema[param_name]
                    
                    # Type validation
                    expected_type = param_schema['type']
                    if not self._validate_type(param_value, expected_type):
                        return False, f"Invalid type for parameter {param_name}. Expected {expected_type}"
                    
                    # Format validation if specified
                    if param_schema['format']:
                        if not self._validate_format(param_value, param_schema['format']):
                            return False, f"Invalid format for parameter {param_name}"
                
            return True, ""
        except Exception as e:
            return False, str(e)
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate parameter type."""
        type_mapping = {
            'string': str,
            'integer': int,
            'number': (int, float),
            'boolean': bool,
            'array': list,
            'object': dict
        }
        
        if expected_type not in type_mapping:
            return True  # Skip validation for unknown types
            
        expected_python_type = type_mapping[expected_type]
        return isinstance(value, expected_python_type)
    
    def _validate_format(self, value: Any, format_type: str) -> bool:
        """Validate parameter format."""
        format_validators = {
            'date-time': self._validate_datetime,
            'date': self._validate_date,
            'email': self._validate_email,
            'uuid': self._validate_uuid,
            'uri': self._validate_uri
        }
        
        if format_type not in format_validators:
            return True  # Skip validation for unknown formats
            
        return format_validators[format_type](value)
    
    def _validate_datetime(self, value: str) -> bool:
        """Validate datetime format."""
        try:
            datetime.fromisoformat(value.replace('Z', '+00:00'))
            return True
        except ValueError:
            return False
    
    def _validate_date(self, value: str) -> bool:
        """Validate date format."""
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def _validate_email(self, value: str) -> bool:
        """Validate email format."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, value))
    
    def _validate_uuid(self, value: str) -> bool:
        """Validate UUID format."""
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, value, re.I))
    
    def _validate_uri(self, value: str) -> bool:
        """Validate URI format."""
        try:
            result = urllib.parse.urlparse(value)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

class SwaggerAPITool(BaseTool):
    """Tool for interacting with APIs using Swagger/OpenAPI documentation."""
    
    name = "SwaggerAPI"
    description = "Interact with APIs using Swagger/OpenAPI documentation"
    
    def __init__(
        self,
        swagger_url: str,
        base_url: str,
        auth_header: Optional[Dict[str, str]] = None,
        rate_limit: Optional[int] = None
    ):
        """Initialize SwaggerAPI tool.
        
        Args:
            swagger_url: URL or file path to Swagger/OpenAPI documentation
            base_url: Base URL for API requests
            auth_header: Optional authentication header
            rate_limit: Optional rate limit (requests per second)
        """
        super().__init__()
        self.swagger_url = swagger_url
        self.base_url = base_url.rstrip('/')
        self.auth_header = auth_header or {}
        self.rate_limit = rate_limit
        self.last_request_time = None
        self.parser = None
        self.llm = ChatOpenAI(temperature=0)
        self.output_parser = PydanticOutputParser(pydantic_object=APIEndpointMatch)
        
    async def _load_swagger(self):
        """Load and parse Swagger documentation."""
        try:
            if self.swagger_url.startswith(('http://', 'https://')):
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.swagger_url) as response:
                        content = await response.text()
            else:
                with open(self.swagger_url, 'r') as f:
                    content = f.read()
            
            # Try JSON first, then YAML
            try:
                swagger_content = json.loads(content)
            except json.JSONDecodeError:
                try:
                    swagger_content = yaml.safe_load(content)
                except yaml.YAMLError as e:
                    raise ValueError(f"Failed to parse Swagger documentation: {str(e)}")
            
            self.parser = SwaggerParser(swagger_content)
            
        except Exception as e:
            raise ValueError(f"Failed to load Swagger documentation: {str(e)}")
    
    def _create_endpoint_prompt(self, query: str) -> str:
        """Create prompt for endpoint matching."""
        paths_info = []
        for path, methods in self.parser.paths.items():
            for method, info in methods.items():
                paths_info.append(
                    f"Path: {path}\n"
                    f"Method: {method.upper()}\n"
                    f"Description: {info.get('description', 'No description')}\n"
                    f"Parameters: {', '.join(p['name'] for p in info.get('parameters', []))}\n"
                )
        
        return f"""
        Match this natural language query to the most appropriate API endpoint.
        
        Available Endpoints:
        {'\n'.join(paths_info)}
        
        Natural Language Query: {query}
        
        Guidelines:
        1. Choose the most semantically relevant endpoint
        2. Extract parameter values from the query
        3. Consider required vs optional parameters
        4. Match HTTP method appropriately
        5. Provide confidence score based on match quality
        
        {self.output_parser.get_format_instructions()}
        """
    
    async def _match_endpoint(self, query: str) -> APIEndpointMatch:
        """Match natural language query to API endpoint."""
        if not self.parser:
            await self._load_swagger()
        
        prompt = ChatPromptTemplate.from_template(self._create_endpoint_prompt(query))
        messages = prompt.format_messages(query=query)
        response = await self.llm.agenerate([messages])
        return self.output_parser.parse(response.generations[0][0].text)
    
    async def _execute_request(
        self,
        path: str,
        method: str,
        params: Dict[str, Any]
    ) -> APIResponse:
        """Execute API request."""
        # Handle rate limiting
        if self.rate_limit:
            if self.last_request_time:
                time_since_last = datetime.now() - self.last_request_time
                if time_since_last.total_seconds() < (1.0 / self.rate_limit):
                    await asyncio.sleep((1.0 / self.rate_limit) - time_since_last.total_seconds())
            self.last_request_time = datetime.now()
        
        # Prepare request
        url = f"{self.base_url}{path}"
        headers = {
            'Content-Type': 'application/json',
            **self.auth_header
        }
        
        start_time = datetime.now()
        
        try:
            async with aiohttp.ClientSession() as session:
                # Split parameters based on 'in' property
                query_params = {}
                body_params = {}
                path_params = {}
                
                endpoint_info = self.parser.get_endpoint_info(path, method)
                param_schema = self.parser.get_parameter_schema(endpoint_info)
                
                for name, value in params.items():
                    if name in param_schema:
                        param_type = param_schema[name]['in']
                        if param_type == 'query':
                            query_params[name] = value
                        elif param_type == 'body':
                            body_params[name] = value
                        elif param_type == 'path':
                            path_params[name] = value
                
                # Apply path parameters
                for name, value in path_params.items():
                    url = url.replace(f"{{{name}}}", str(value))
                
                # Make request
                async with getattr(session, method.lower())(
                    url,
                    params=query_params,
                    json=body_params if body_params else None,
                    headers=headers
                ) as response:
                    execution_time = (datetime.now() - start_time).total_seconds()
                    
                    try:
                        data = await response.json()
                    except:
                        data = await response.text()
                    
                    return APIResponse(
                        status_code=response.status,
                        success=response.status < 400,
                        data=data,
                        error_message=None if response.status < 400 else str(data),
                        execution_time=execution_time,
                        endpoint=path,
                        method=method.upper()
                    )
                    
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return APIResponse(
                status_code=500,
                success=False,
                data=None,
                error_message=str(e),
                execution_time=execution_time,
                endpoint=path,
                method=method.upper()
            )
    
    def _run(self, query: str) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun")
    
    async def _arun(self, query: str) -> Dict[str, Any]:
        """
        Execute API request based on natural language query.
        
        Args:
            query: Natural language query
            
        Returns:
            Dict containing API response
        """
        try:
            # Match endpoint
            match = await self._match_endpoint(query)
            
            # Validate parameters
            is_valid, error = self.parser.validate_parameters(
                match.path,
                match.method,
                match.parameters
            )
            
            if not is_valid:
                return {
                    "success": False,
                    "error": f"Parameter validation failed: {error}",
                    "endpoint_match": match.dict()
                }
            
            # Execute request
            response = await self._execute_request(
                match.path,
                match.method,
                match.parameters
            )
            
            return {
                "success": response.success,
                "data": response.data,
                "error": response.error_message,
                "execution_time": response.execution_time,
                "endpoint_match": match.dict(),
                "status_code": response.status_code
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "endpoint_match": None
            }

class AsyncAPIBatchTool(BaseTool):
    """Execute multiple API queries in parallel."""
    
    name = "AsyncAPIBatch"
    description = "Execute multiple API queries in parallel"
    
    def __init__(self, api_tool: SwaggerAPITool):
        """Initialize batch tool with API tool instance."""
        super().__init__()
        self.api_tool = api_tool
    
    def _run(self, queries: List[str]) -> Dict[str, Any]:
        """Sync version not implemented."""
        raise NotImplementedError("Use arun")
    
    async def _arun(self, queries: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute multiple API queries in parallel.
        
        Args:
            queries: List of natural language queries
            
        Returns:
            Dict containing results for all queries
        """
        tasks = [self.api_tool._arun(query) for query in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            "results": [
                result if not isinstance(result, Exception) else {"error": str(result)}
                for result in results
            ],
            "success_count": sum(1 for r in results if not isinstance(r, Exception) and r.get("success", False)),
            "error_count": sum(1 for r in results if isinstance(r, Exception) or not r.get("success", False)),
            "execution_time": sum(r.get("execution_time", 0) for r in results if not isinstance(r, Exception))
        }
