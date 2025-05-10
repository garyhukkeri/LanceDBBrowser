"""
Configuration Manager Module

This module provides a centralized way to manage configuration settings
for the LanceDB Browser application, independent of any UI framework.
"""
import os
import json
from typing import Dict, Any, Optional


class ConfigManager:
    """
    Manages configuration settings for the application.
    This class provides methods to load, save, and access configuration settings
    from different sources (environment variables, config files, etc.)
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Optional path to a JSON config file
        """
        self.config = {}
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config',
            'config.json'
        )
        
        # Load configuration
        self._load_config()
        
    def _load_config(self) -> None:
        """Load configuration from file and environment variables."""
        # Load from file if it exists
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    self.config = json.load(f)
            except Exception as e:
                print(f"Error loading config from {self.config_path}: {str(e)}")
        
        # Override with environment variables
        self._load_from_env()
        
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        # Map of env variables to config keys
        env_mapping = {
            'LANCEDB_PATH': 'lancedb_path',
            'HOST_DB_PATH': 'host_db_path',
            'VECTOR_DIMENSION': 'vector_dimension',
            'MAX_RESULTS': 'max_results'
        }
        
        # Load from environment variables
        for env_var, config_key in env_mapping.items():
            if env_var in os.environ:
                # Convert to int if needed
                if config_key in ['vector_dimension', 'max_results']:
                    try:
                        self.config[config_key] = int(os.environ[env_var])
                    except ValueError:
                        self.config[config_key] = os.environ[env_var]
                else:
                    self.config[config_key] = os.environ[env_var]
    
    def save_config(self) -> None:
        """Save the current configuration to the config file."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config to {self.config_path}: {str(e)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key doesn't exist
            
        Returns:
            The configuration value or default
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get all configuration values.
        
        Returns:
            Dictionary with all configuration values
        """
        return self.config.copy()