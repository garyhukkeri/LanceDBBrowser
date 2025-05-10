"""
Environment Utilities Module

This module provides utility functions for detecting and handling
environment-specific concerns that might be needed by different UI adapters.
"""
import os
from typing import Dict, Any, Optional


def get_environment_variables() -> Dict[str, str]:
    """
    Get relevant environment variables for LanceDB configuration.
    
    Returns:
        Dictionary with environment variables
    """
    env_vars = {
        'lancedb_path': os.environ.get('LANCEDB_PATH', ''),
        'host_db_path': os.environ.get('HOST_DB_PATH', ''),
        'running_in_docker': os.environ.get('RUNNING_IN_DOCKER', 'false').lower() == 'true'
    }
    
    return env_vars


def is_running_in_docker() -> bool:
    """
    Check if the application is running inside a Docker container.
    This is moved to a utility function so it can be used by any UI adapter.
    
    Returns:
        True if running in Docker, False otherwise
    """
    try:
        # Primary method: check for .dockerenv file
        if os.path.exists('/.dockerenv'):
            return True
            
        # Secondary method: check cgroups
        with open('/proc/self/cgroup', 'r') as f:
            return any('docker' in line for line in f)
    except:
        # Fallback to environment variable check
        return os.environ.get('RUNNING_IN_DOCKER', 'false').lower() == 'true'


def get_default_db_path() -> str:
    """
    Get the default database path based on environment.
    
    Returns:
        Default database path
    """
    if is_running_in_docker():
        return os.environ.get('HOST_DB_PATH', '/data/lancedb')
    else:
        return os.environ.get('LANCEDB_PATH', '.lancedb')