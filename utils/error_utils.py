"""
Error Handling Utilities

This module provides centralized error handling functionality for the application.
"""
import logging
from typing import Optional, Any, Dict, Type
from functools import wraps

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class AppError(Exception):
    """Base exception class for application errors"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

class DatabaseError(AppError):
    """Base class for database-related errors"""
    pass

class ConnectionError(DatabaseError):
    """Database connection errors"""
    pass

class TableError(DatabaseError):
    """Table operation errors"""
    pass

class EmbeddingError(AppError):
    """Embedding-related errors"""
    pass

class ValidationError(AppError):
    """Data validation errors"""
    pass

def handle_error(error: Exception) -> Dict[str, Any]:
    """
    Convert an exception into a standardized error response format.
    
    Args:
        error: The exception to handle
        
    Returns:
        Dictionary with error details
    """
    error_type = error.__class__.__name__
    
    if isinstance(error, AppError):
        return {
            'success': False,
            'error': {
                'type': error_type,
                'message': error.message,
                'details': error.details
            }
        }
    
    # Handle unexpected errors
    logger.error(f"Unexpected error: {str(error)}", exc_info=True)
    return {
        'success': False,
        'error': {
            'type': error_type,
            'message': str(error),
            'details': {'unexpected': True}
        }
    }

def with_error_handling(error_class: Type[Exception] = AppError):
    """
    Decorator for handling errors in functions.
    
    Args:
        error_class: The type of error to catch and handle
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return {
                    'success': True,
                    'data': func(*args, **kwargs)
                }
            except error_class as e:
                return handle_error(e)
            except Exception as e:
                return handle_error(e)
        return wrapper
    return decorator

def validate_table_name(table_name: str) -> None:
    """
    Validate a table name.
    
    Args:
        table_name: Name to validate
        
    Raises:
        ValidationError: If name is invalid
    """
    if not table_name:
        raise ValidationError("Table name cannot be empty")
    if not table_name.isidentifier():
        raise ValidationError(
            "Invalid table name",
            {'details': "Table name must be a valid identifier (letters, numbers, underscore)"}
        )

def validate_vector_dimension(vector: list, expected_dim: int) -> None:
    """
    Validate a vector's dimension.
    
    Args:
        vector: Vector to validate
        expected_dim: Expected dimension
        
    Raises:
        ValidationError: If dimension is incorrect
    """
    if len(vector) != expected_dim:
        raise ValidationError(
            "Invalid vector dimension",
            {
                'expected': expected_dim,
                'actual': len(vector)
            }
        ) 