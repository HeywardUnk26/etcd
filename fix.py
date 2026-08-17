```python
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import json

@dataclass
class EtcdCredentials:
    """Holds authentication credentials for etcd cluster health checks."""
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    
    @property
    def is_set(self) -> bool:
        return bool(self.username or self.password or self.token)

class EtcdClusterHealth:
    """Manages etcdctl endpoint health checks with proper credential propagation."""
    
    def __init__(
        self, 
        endpoint: str = 'http://localhost:2379',
        credentials: Optional[EtcdCredentials] = None,
        cluster_mode: bool = True
    ):
        self.endpoint = endpoint
        self.credentials = credentials or EtcdCredentials()
        self.cluster_mode = cluster_mode
    
    def _compose_auth_headers(self) -> Dict[str, str]:
        """Build authentication headers for the health check request."""
        headers: Dict[str, str] = {}
        
        if self.credentials.username:
            headers['X-Username'] = self.credentials.username
        if self.credentials.password:
            headers['X-Password'] = self.credentials.password
        if self.credentials.token:
            headers['X-Token'] = self.credentials.token
            
        return headers
    
    def _compose_auth_query(self) -> Dict[str, str]:
        """Build authentication query parameters for GET requests."""
        params: Dict[str, str] = {}
        
        if self.credentials.username:
            params['username'] = self.credentials.username
        if self.credentials.password:
            params['password'] = self.credentials.password
        if self.credentials.token:
            params['token'] = self.credentials.token
            
        return params
    
    def get_health_config(self, verbose: bool = True) -> Dict[str, Any]:
        """Return the complete health check configuration including auth."""
        base_config: Dict[str, Any] = {
            'endpoint': self.endpoint,
            'cluster_mode': self.cluster_mode,
            'credentials': {
                'username': self.credentials.username,
                'password': self.credentials.password,
                'token': self.credentials.token
            } if self.credentials.is_set else None
        }
        
        if verbose:
            headers = self._compose_auth_headers()
            query = self._compose_auth_query()
            base_config.update({
                'headers': headers,
                'query_params': query
            })
        
        return base_config
    
    def get_endpoint_url(self, cluster: bool = True) -> str:
        """Build the endpoint URL including cluster flag."""
        url = f'{self.endpoint}/health'
        if cluster:
            params = f'?cluster=true' if '?' not in url else '&cluster=true'
            return f'{url}{params}'
        return url
    
    def build_command(self) -> str:
        """Build the complete etcdctl command string."""
        base = 'etcdctl endpoint health'
        if self.cluster_mode:
            base += ' --cluster=true'
        
        if self.credentials.is_set:
            if self.credentials.username and self.credentials.password:
                base += f' -u {self.credentials.username} -p {self.credentials.password}'
            elif self.credentials.username:
                base += f' -u {self.credentials.username}'
            elif self.credentials.token:
                base += f' -t {self.credentials.token}'
            
        return base
    
    def get_request(self, method: str = 'GET') -> Dict[str, Any]:
        """Build a complete HTTP request dict for the endpoint health."""
        url = self.get_endpoint_url()
        headers = self._compose_auth_headers()
        
        return {
            'method': method,
            'url': url,
            'endpoint': self.endpoint,
            'headers': headers,
            'body': None,
            'credentials': self.credentials.to_dict()
        }
    
    def add_prefix(self, prefix: str = 'X-') -> 'EtcdClusterHealth':
        """Set a custom prefix for all credential headers."""
        self.credentials.prefix = prefix
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert object to dictionary for serialization."""
        result = {
            'endpoint': self.endpoint,
            'cluster_mode': self.cluster_mode,
            'credentials': self.credentials.to_dict() if self.credentials else None
        }
        if self.credentials.is_set:
            result['headers'] = self._compose_auth_headers()
            result['query_params'] = self._compose_auth_query()
        return result
```