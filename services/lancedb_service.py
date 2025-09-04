"""
LanceDB Service Module

This module provides a consolidated service for all LanceDB operations.
Includes connection management, retry mechanisms, and comprehensive error handling.
"""
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import logging
import time
from functools import wraps
import lancedb

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def retry_operation(max_attempts: int = 3, delay: float = 1.0):
    """Decorator for retrying database operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"Operation failed, attempt {attempt + 1}/{max_attempts}: {str(e)}")
                        time.sleep(delay * (attempt + 1))  # Exponential backoff
                    else:
                        logger.error(f"Operation failed after {max_attempts} attempts: {str(e)}")
                        raise last_error
        return wrapper
    return decorator

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class ConnectionError(DatabaseError):
    """Exception for connection-related errors"""
    pass

class TableOperationError(DatabaseError):
    """Exception for table operation errors"""
    pass

class LanceDBService:
    """
    Consolidated service for LanceDB operations.
    Handles all database interactions with proper error handling and retries.
    """
    
    def __init__(self):
        self._connection = None
        self._db_path = None
        self._max_retries = 3
        self._retry_delay = 1.0
        logger.info("Initializing LanceDBService")

    @property
    def is_connected(self) -> bool:
        """Check if there is an active connection"""
        return self._connection is not None

    @retry_operation(max_attempts=3)
    def connect(self, db_path: str) -> bool:
        """
        Connect to a LanceDB database with retry mechanism.
        
        Args:
            db_path: Path to the LanceDB database
            
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails after retries
        """
        try:
            logger.info(f"Connecting to LanceDB at: {db_path}")
            self._connection = lancedb.connect(db_path)
            self._db_path = db_path
            return True
        except Exception as e:
            logger.error(f"Failed to connect to LanceDB: {str(e)}")
            self._connection = None
            self._db_path = None
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def ensure_connection(self) -> bool:
        """
        Ensure database connection is active, reconnect if needed.
        
        Returns:
            bool: True if connection is active
        """
        if self.is_connected:
            try:
                # Test connection by listing tables
                _ = self._connection.table_names()
                return True
            except Exception:
                self._connection = None

        if self._db_path:
            try:
                return self.connect(self._db_path)
            except ConnectionError:
                return False
        return False

    @retry_operation()
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
            
        Raises:
            ConnectionError: If not connected
            DatabaseError: For other database errors
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            tables = self._connection.table_names()
            logger.debug(f"Found tables: {tables}")
            return tables
        except Exception as e:
            raise DatabaseError(f"Failed to list tables: {str(e)}")

    @retry_operation()
    def delete_table(self, table_name: str) -> bool:
        """
        Delete a table from the database.
        
        Args:
            table_name: Name of the table to delete
            
        Returns:
            bool: True if table was deleted
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If deletion fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            if table_name in self.list_tables():
                self._connection.drop_table(table_name)
            return True
        except Exception as e:
            raise TableOperationError(f"Failed to delete table: {str(e)}")

    @retry_operation()
    def replace_table(self, table_name: str, data: pd.DataFrame, 
                     vector_column: Optional[str] = None) -> bool:
        """
        Replace an existing table with new data.
        
        Args:
            table_name: Name of the table
            data: DataFrame with the new data
            vector_column: Optional name of vector/embedding column
            
        Returns:
            bool: True if table was replaced successfully
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If operation fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            # Delete existing table if it exists
            self.delete_table(table_name)
            
            # Create new table
            logger.info(f"Creating replacement table '{table_name}' with {len(data)} rows")
            self._connection.create_table(table_name, data=data)
            return True
        except Exception as e:
            raise TableOperationError(f"Failed to replace table: {str(e)}")

    @retry_operation()
    def create_table(self, table_name: str, data: pd.DataFrame, 
                    vector_column: Optional[str] = None) -> bool:
        """
        Create a new table in the database.
        
        Args:
            table_name: Name for the new table
            data: DataFrame with the data
            vector_column: Optional name of vector/embedding column
            
        Returns:
            bool: True if table created successfully
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If table creation fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            logger.info(f"Creating table '{table_name}' with {len(data)} rows")
            self._connection.create_table(table_name, data=data)
            return True
        except Exception as e:
            raise TableOperationError(f"Failed to create table: {str(e)}")

    @retry_operation()
    def query_table(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        """
        Query data from a table.
        
        Args:
            table_name: Table to query
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with query results
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If query fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            table = self._connection[table_name]
            return table.search().limit(limit).to_pandas()
        except Exception as e:
            raise TableOperationError(f"Failed to query table: {str(e)}")

    @retry_operation()
    def query_table_paginated(self, table_name: str, offset: int = 0, 
                             limit: int = 50) -> pd.DataFrame:
        """
        Query paginated data from a table using LIMIT/OFFSET.
        
        Args:
            table_name: Table to query
            offset: Number of rows to skip
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with query results
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If query fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            table = self._connection[table_name]
            return table.search().limit(limit).offset(offset).to_pandas()
        except Exception as e:
            raise TableOperationError(f"Failed to query table: {str(e)}")

    @retry_operation()
    def count_table_rows(self, table_name: str) -> int:
        """
        Get total row count efficiently.
        
        Args:
            table_name: Table to count
            
        Returns:
            Total number of rows
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If count fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            table = self._connection[table_name]
            return table.count_rows()
        except Exception as e:
            raise TableOperationError(f"Failed to count table rows: {str(e)}")

    @retry_operation()
    def semantic_search(self, table_name: str, query_vector: List[float],
                       vector_column: str, limit: int = 10) -> pd.DataFrame:
        """
        Perform semantic search in a table.
        
        Args:
            table_name: Table to search in
            query_vector: Vector to search with
            vector_column: Name of the vector/embedding column
            limit: Maximum number of results
            
        Returns:
            DataFrame with search results
            
        Raises:
            ConnectionError: If not connected
            TableOperationError: If search fails
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
            
        try:
            table = self._connection[table_name]
            results = table.search(query_vector, vector_column_name=vector_column) \
                         .limit(limit) \
                         .to_pandas()
            return results
        except Exception as e:
            raise TableOperationError(f"Failed to perform semantic search: {str(e)}")

    def get_connection(self) -> Any:
        """
        Get the underlying database connection.
        
        Returns:
            Database connection object
            
        Raises:
            ConnectionError: If not connected
        """
        if not self.ensure_connection():
            raise ConnectionError("Not connected to database")
        return self._connection
