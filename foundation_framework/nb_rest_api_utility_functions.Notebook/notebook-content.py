# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests
import json
import logging
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class MetadataDrivenAPIAuthenticator:
    """API authenticator optimized for metadata-driven solutions"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
    
    def extract_data(self, api_config: Dict[str, Any], **runtime_overrides) -> Dict[str, Any]:
        """
        Main method for metadata-driven API extraction
        
        Args:
            api_config: Dictionary containing all API configuration from metadata
            runtime_overrides: Any parameters to override at runtime
            
        Returns:
            Dictionary containing the API response data
        """
        # Merge config with runtime overrides
        config = {**api_config, **runtime_overrides}
        
        try:
            response = self._make_authenticated_request(config)
            
            # Handle different response formats
            if response.content:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return {'raw_response': response.text}
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"API extraction failed: {str(e)}")
            return {'error': str(e), 'config': api_config}
    
    def extract_paginated_data(self, api_config: Dict[str, Any], **runtime_overrides) -> List[Dict[str, Any]]:
        """
        Extract paginated data based on metadata configuration
        
        Expected pagination config in api_config:
        {
            "pagination": {
                "enabled": true,
                "page_param": "page",
                "size_param": "limit", 
                "max_pages": 100,
                "data_path": "data"  # where results are in response
            }
        }
        """
        config = {**api_config, **runtime_overrides}
        pagination_config = config.get('pagination', {})
        
        if not pagination_config.get('enabled', False):
            # Not paginated, just return single response as list
            result = self.extract_data(config)
            return [result] if not isinstance(result, list) else result
        
        all_results = []
        page = 1
        max_pages = pagination_config.get('max_pages', 100)
        page_param = pagination_config.get('page_param', 'page')
        data_path = pagination_config.get('data_path', 'data')
        
        # Initialize query params
        query_params = config.get('query_params', {}).copy()
        
        while page <= max_pages:
            query_params[page_param] = page
            config['query_params'] = query_params
            
            data = self.extract_data(config)
            
            if 'error' in data:
                self.logger.error(f"Pagination failed at page {page}: {data['error']}")
                break
            
            # Extract results based on data_path
            if data_path and data_path in data:
                results = data[data_path]
            elif isinstance(data, list):
                results = data
            else:
                results = [data]
            
            if not results:
                break
                
            all_results.extend(results)
            page += 1
        
        return all_results
    
    def extract_multiple_apis(self, api_configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract data from multiple APIs based on metadata configurations
        
        Args:
            api_configs: Dictionary where keys are API names and values are configurations
            
        Returns:
            Dictionary with API names as keys and extracted data as values
        """
        results = {}
        
        for api_name, config in api_configs.items():
            self.logger.info(f"Extracting data from: {api_name}")
            
            try:
                if config.get('pagination', {}).get('enabled', False):
                    results[api_name] = self.extract_paginated_data(config)
                else:
                    results[api_name] = self.extract_data(config)
                    
            except Exception as e:
                self.logger.error(f"Failed to extract {api_name}: {str(e)}")
                results[api_name] = {'error': str(e)}
        
        return results
    
    def _make_authenticated_request(self, config: Dict[str, Any]) -> requests.Response:
        """Internal method to make authenticated requests based on config"""
        
        # Build URL
        base_url = config.get('base_url', '')
        resource = config.get('resource', '')
        url = f'{base_url.rstrip("/")}/{resource.lstrip("/")}'
        
        # Handle query parameters
        query_params = config.get('query_parameters')
        if query_params:
            url = f'{url}?{urlencode(query_params)}'
        
        # Get authentication headers
        auth_headers = self._get_auth_headers(config)
        
        # Combine all headers
        headers = config.get('headers', config.get('additional_headers', {})).copy()
        headers.update(auth_headers)
        
        # Request configuration
        method = config.get('method', 'GET').upper()
        timeout = config.get('timeout', 30)
        verify_ssl = config.get('verify_ssl', True)
        request_data = config.get('request_data', config.get('data'))
        
        # Set content type for requests with data
        if method in ['POST', 'PUT', 'PATCH'] and request_data:
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
        
        self.logger.info(f'Making {method} request to: {url}')
        
        # Make the request
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            json=request_data if headers.get('Content-Type') == 'application/json' else None,
            data=request_data if headers.get('Content-Type') != 'application/json' else None,
            timeout=timeout,
            verify=verify_ssl
        )
        
        self.logger.info(f'Response status: {response.status_code}')
        response.raise_for_status()
        
        return response
    
    def _get_auth_headers(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Get authentication headers based on configuration"""
        
        auth_type = config.get('authentication', config.get('auth_type', 'None'))
        
        if auth_type == 'Service Principal':
            return self._service_principal_auth(config)
        elif auth_type == 'API Key':
            return self._api_key_auth(config)
        elif auth_type == 'Bearer':
            return self._bearer_auth(config)
        elif auth_type == 'Basic':
            return self._basic_auth(config)
        else:
            return {}
    
    def _service_principal_auth(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Service Principal authentication"""
        authentication_base_url = config.get('authentication_base_url', '')
        tenant_id = config.get('tenant_id', '')
        client_id = config.get('client_id', '')
        client_secret = config.get('client_secret', '')
        resource = config.get('authentication_resource', '')
        grant_type = config.get('grant_type', 'client_credentials')
        
        authentication_url = f'{authentication_base_url.rstrip("/")}/{tenant_id}/oauth2/token'
        
        auth_data = {
            'grant_type': grant_type,
            'resource': resource,
            'client_id': client_id,
            'client_secret': client_secret
        }
        
        response = self.session.post(
            url=authentication_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=auth_data,
            timeout=30
        )
        response.raise_for_status()
        
        token_data = response.json()
        return {'Authorization': f'Bearer {token_data["access_token"]}'}
    
    def _api_key_auth(self, config: Dict[str, Any]) -> Dict[str, str]:
        """API Key authentication"""
        api_key = config.get('api_key', '')
        header_name = config.get('api_key_header', 'x-api-key')
        return {header_name: api_key}
    
    def _bearer_auth(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Bearer token authentication"""
        token = config.get('bearer_token', config.get('token', ''))
        return {'Authorization': f'Bearer {token}'}
    
    def _basic_auth(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Basic authentication"""
        username = config.get('username', '')
        password = config.get('password', '')
        
        from base64 import b64encode
        credentials = b64encode(f'{username}:{password}'.encode()).decode()
        return {'Authorization': f'Basic {credentials}'}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
